import { Config, Effect, Layer, Option, Stream, String } from 'effect'
import { LanguageModel, type Response } from 'effect/ai'
import { FetchHttpClient } from 'effect/http'

import { AnthropicClient, AnthropicLanguageModel } from '@effect/ai-anthropic'

// NOTE: with ANTHROPIC_API_KEY set, replies come from Claude through
// @effect/ai-anthropic. Without it, an offline model streams a canned reply so
// the example runs with no account. Both are the same LanguageModel service,
// so the chat handlers do not know which one they use.

const DEFAULT_MODEL = 'claude-haiku-5-5'

const OFFLINE_WORD_DELAY = '60 millis'

const offlineReplyFor = (prompt: string): string =>
  `You said: "${String.trim(prompt)}". This reply comes from the offline model, ` +
  'streamed one word at a time through a server function. Set ANTHROPIC_API_KEY ' +
  'and restart the server to chat with Claude instead.'

const lastUserText = (options: LanguageModel.ProviderOptions): string => {
  const userMessages = options.prompt.content.filter(
    message => message.role === 'user',
  )
  const lastMessage = userMessages.at(-1)
  if (lastMessage === undefined) {
    return ''
  }
  return lastMessage.content
    .flatMap(part => (part.type === 'text' ? [part.text] : []))
    .join(' ')
}

const OFFLINE_TEXT_ID = 'offline-text'

const offlineStream = (
  options: LanguageModel.ProviderOptions,
): Stream.Stream<Response.StreamPartEncoded> => {
  const words = String.split(offlineReplyFor(lastUserText(options)), ' ')
  const deltas = Stream.fromIterable(words).pipe(
    Stream.map(
      (word, index): Response.StreamPartEncoded => ({
        type: 'text-delta',
        id: OFFLINE_TEXT_ID,
        delta: index === 0 ? word : ` ${word}`,
      }),
    ),
    Stream.tap(() => Effect.sleep(OFFLINE_WORD_DELAY)),
  )
  const textStart: Response.StreamPartEncoded = {
    type: 'text-start',
    id: OFFLINE_TEXT_ID,
  }
  const textEnd: Response.StreamPartEncoded = {
    type: 'text-end',
    id: OFFLINE_TEXT_ID,
  }
  return Stream.succeed(textStart).pipe(
    Stream.concat(deltas),
    Stream.concat(Stream.succeed(textEnd)),
  )
}

const OfflineLanguageModel = Layer.effect(LanguageModel.LanguageModel)(
  LanguageModel.make({
    generateText: () => Effect.succeed([]),
    streamText: offlineStream,
  }),
)

const anthropicLanguageModel = (model: string) =>
  AnthropicLanguageModel.layer({ model }).pipe(
    Layer.provide(
      AnthropicClient.layerConfig({
        apiKey: Config.Redacted('ANTHROPIC_API_KEY'),
      }),
    ),
    Layer.provide(FetchHttpClient.layer),
  )

/** Claude when ANTHROPIC_API_KEY is set, the offline model otherwise. */
export const LanguageModelLive = Layer.unwrap(
  Effect.gen(function* () {
    const maybeApiKey = yield* Config.option(Config.String('ANTHROPIC_API_KEY'))
    const model = yield* Config.withDefault(
      Config.String('ANTHROPIC_MODEL'),
      DEFAULT_MODEL,
    )

    if (Option.isSome(maybeApiKey)) {
      yield* Effect.log(`Chat replies use ${model}`)
      return anthropicLanguageModel(model)
    } else {
      yield* Effect.log('ANTHROPIC_API_KEY is not set; chat replies use the offline model')
      return OfflineLanguageModel
    }
  }),
)
