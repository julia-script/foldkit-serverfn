import {
  Array,
  Context,
  Effect,
  Exit,
  FiberSet,
  Layer,
  PubSub,
  Ref,
  Semaphore,
  Stream,
  String,
} from 'effect'
import { LanguageModel, type Prompt } from 'effect/ai'

import {
  type ChatMessage,
  type ConversationEvent,
  MAX_MESSAGE_LENGTH,
  type ReplyStatus,
} from './domain'
import { LanguageModelLive } from './languageModel.server'
import { ChatFunctions, MessageRejected } from './serverFunction'

// CHAT

type ChatService = Readonly<{
  messages: Effect.Effect<ReadonlyArray<ChatMessage>>
  send: (
    author: string,
    text: string,
  ) => Effect.Effect<ChatMessage, MessageRejected>
  stopReplies: Effect.Effect<void>
  watch: Stream.Stream<ConversationEvent>
}>

export class Chat extends Context.Service<Chat, ChatService>()('Chat') {}

const ASSISTANT_NAME = 'Assistant'
const MAX_HISTORY = 40

const SYSTEM_PROMPT =
  'You are a friendly assistant in a small group chat demo built with Foldkit, ' +
  'a TypeScript framework on Effect with the Elm Architecture. Several people ' +
  'may be talking; each user message starts with their name. Keep replies short.'

const validate = (
  author: string,
  text: string,
): Effect.Effect<Readonly<{ author: string; text: string }>, MessageRejected> => {
  const trimmedAuthor = String.trim(author)
  const trimmedText = String.trim(text)

  if (String.isEmpty(trimmedAuthor)) {
    return Effect.fail(new MessageRejected({ reason: 'MissingAuthor' }))
  } else if (String.isEmpty(trimmedText)) {
    return Effect.fail(new MessageRejected({ reason: 'MissingText' }))
  } else if (trimmedText.length > MAX_MESSAGE_LENGTH) {
    return Effect.fail(new MessageRejected({ reason: 'TextTooLong' }))
  } else {
    return Effect.succeed({ author: trimmedAuthor, text: trimmedText })
  }
}

const toPromptMessages = (
  message: ChatMessage,
): ReadonlyArray<Prompt.MessageEncoded> => {
  if (message.role === 'User') {
    return [{ role: 'user', content: `${message.author}: ${message.text}` }]
  } else if (String.isEmpty(message.text)) {
    return []
  } else {
    return [{ role: 'assistant', content: message.text }]
  }
}

const toPrompt = (
  messages: ReadonlyArray<ChatMessage>,
): ReadonlyArray<Prompt.MessageEncoded> => [
  { role: 'system', content: SYSTEM_PROMPT },
  ...Array.flatMap(messages, toPromptMessages),
]

const replyStatusFromExit = (exit: Exit.Exit<unknown, unknown>): ReplyStatus => {
  if (Exit.isSuccess(exit)) {
    return 'Complete'
  } else if (Exit.hasInterrupts(exit)) {
    return 'Stopped'
  } else {
    return 'Failed'
  }
}

export const ChatLive = Layer.effect(Chat)(
  Effect.gen(function* () {
    const languageModel = yield* LanguageModel.LanguageModel
    const messagesRef = yield* Ref.make<ReadonlyArray<ChatMessage>>([])
    const events = yield* PubSub.unbounded<ConversationEvent>()
    const replies = yield* FiberSet.make()
    const lock = yield* Semaphore.make(1)

    const commit = (
      transform: (messages: ReadonlyArray<ChatMessage>) => ReadonlyArray<ChatMessage>,
      event: ConversationEvent,
    ): Effect.Effect<void> =>
      lock.withPermit(
        Ref.update(messagesRef, transform).pipe(
          Effect.andThen(PubSub.publish(events, event)),
        ),
      )

    const updateMessage = (
      messageId: string,
      transform: (message: ChatMessage) => ChatMessage,
    ) =>
      Array.map((message: ChatMessage) =>
        message.id === messageId ? transform(message) : message,
      )

    const generateReply = (
      history: ReadonlyArray<ChatMessage>,
      replyId: string,
    ): Effect.Effect<void> =>
      LanguageModel.streamText({ prompt: toPrompt(history) }).pipe(
        Stream.filter(part => part.type === 'text-delta'),
        Stream.runForEach(part =>
          commit(
            updateMessage(replyId, message => ({
              ...message,
              text: message.text + part.delta,
            })),
            { _tag: 'AppendedText', messageId: replyId, delta: part.delta },
          ),
        ),
        Effect.provideService(LanguageModel.LanguageModel, languageModel),
        Effect.onExit(exit => {
          const status = replyStatusFromExit(exit)
          return commit(
            updateMessage(replyId, message => ({ ...message, status })),
            { _tag: 'EndedReply', messageId: replyId, status },
          )
        }),
        Effect.tapCause(cause => Effect.logWarning('Reply failed', cause)),
        Effect.ignore,
      )

    const send = (author: string, text: string) =>
      Effect.gen(function* () {
        const valid = yield* validate(author, text)
        const userMessage: ChatMessage = {
          id: crypto.randomUUID(),
          role: 'User',
          author: valid.author,
          text: valid.text,
          status: 'Complete',
        }
        const reply: ChatMessage = {
          id: crypto.randomUUID(),
          role: 'Assistant',
          author: ASSISTANT_NAME,
          text: '',
          status: 'Streaming',
        }

        yield* commit(Array.append(userMessage), {
          _tag: 'AddedMessage',
          message: userMessage,
        })
        const history = yield* Ref.get(messagesRef)
        yield* commit(
          messages => Array.takeRight(Array.append(messages, reply), MAX_HISTORY),
          { _tag: 'AddedMessage', message: reply },
        )
        yield* FiberSet.run(replies, generateReply(history, reply.id))

        return userMessage
      })

    const watch: Stream.Stream<ConversationEvent> = Stream.unwrap(
      lock.withPermit(
        Effect.gen(function* () {
          const subscription = yield* PubSub.subscribe(events)
          const messages = yield* Ref.get(messagesRef)
          const synced: ConversationEvent = {
            _tag: 'SyncedConversation',
            messages,
          }
          return Stream.succeed(synced).pipe(
            Stream.concat(Stream.fromSubscription(subscription)),
          )
        }),
      ),
    )

    return Chat.of({
      messages: Ref.get(messagesRef),
      send,
      stopReplies: FiberSet.clear(replies),
      watch,
    })
  }),
).pipe(Layer.provide(LanguageModelLive))

// HANDLERS

export const ChatFunctionsLive = ChatFunctions.toLayer(
  Effect.gen(function* () {
    const chat = yield* Chat
    return {
      GetConversation: () => chat.messages,
      SendMessage: ({ author, text }) => chat.send(author, text),
      StopReplies: () => chat.stopReplies,
      WatchConversation: () => chat.watch,
    }
  }),
).pipe(Layer.provide(ChatLive))
