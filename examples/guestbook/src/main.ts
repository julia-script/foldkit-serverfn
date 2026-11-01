import { Array, Effect, Option, Schema, Stream } from 'effect'
import { Command, Subscription, type Update } from 'foldkit'
import type { Document, Html, HtmlBuilder } from 'foldkit/html'
import { defineMessageUnion } from 'foldkit/message'
import { defineTaggedUnion } from 'foldkit/schema'
import { modifyFields } from 'foldkit/struct'
import type { ServerFunctionClient } from 'foldkit-serverfn'

import { Entry, MAX_TEXT_LENGTH } from './domain'
import { SignGuestbook, StreamFortune, WatchEntries } from './serverFunction'

// MODEL

const Signing = defineTaggedUnion({
  Idle: {},
  Sending: {},
  Failed: { reason: Schema.String },
})

const Fortune = defineTaggedUnion({
  Idle: {},
  Streaming: { requestId: Schema.Number, name: Schema.String, text: Schema.String },
  Done: { text: Schema.String },
  Failed: { reason: Schema.String },
})

const LiveFeed = defineTaggedUnion({
  Connected: {},
  Disconnected: { reason: Schema.String },
})

export const Model = Schema.Struct({
  entries: Schema.Array(Entry),
  name: Schema.String,
  text: Schema.String,
  signing: Signing,
  fortune: Fortune,
  fortuneRequestCount: Schema.Number,
  liveFeed: LiveFeed,
  liveFeedGeneration: Schema.Number,
})
export type Model = typeof Model.Type

// FLAGS

export const Flags = Schema.Struct({
  entries: Schema.Array(Entry),
})
export type Flags = typeof Flags.Type

// MESSAGE

export const Message = defineMessageUnion({
  UpdatedName: { name: Schema.String },
  UpdatedText: { text: Schema.String },
  SubmittedSignForm: {},
  SucceededSignGuestbook: { entry: Entry },
  FailedSignGuestbook: { reason: Schema.String },
  ReceivedEntry: { entry: Entry },
  FailedWatchEntries: { reason: Schema.String },
  ClickedReconnect: {},
  ClickedTellFortune: {},
  ClickedStopFortune: {},
  ReceivedFortuneChunk: { chunk: Schema.String },
  CompletedStreamFortune: {},
  FailedStreamFortune: { reason: Schema.String },
})
export type Message = typeof Message.Type

type UpdateReturn = Update.Return<Model, Message, ServerFunctionClient>

// INIT

export const init = (flags: Flags): UpdateReturn => ({
  model: {
    entries: flags.entries,
    name: '',
    text: '',
    signing: Signing.Idle(),
    fortune: Fortune.Idle(),
    fortuneRequestCount: 0,
    liveFeed: LiveFeed.Connected(),
    liveFeedGeneration: 0,
  },
})

// UPDATE

const withEntry = (entry: Entry) => (entries: ReadonlyArray<Entry>) =>
  Array.some(entries, ({ id }) => id === entry.id)
    ? entries
    : Array.prepend(entries, entry)

export const update = (model: Model, message: Message) =>
  Message.match<UpdateReturn>(message, {
    UpdatedName: ({ name }) => ({
      model: modifyFields(model, { name: () => name }),
    }),
    UpdatedText: ({ text }) => ({
      model: modifyFields(model, { text: () => text }),
    }),
    SubmittedSignForm: () => ({
      model: modifyFields(model, { signing: () => Signing.Sending() }),
      commands: [SignEntry({ name: model.name, text: model.text })],
    }),
    SucceededSignGuestbook: ({ entry }) => ({
      model: modifyFields(model, {
        entries: withEntry(entry),
        text: () => '',
        signing: () => Signing.Idle(),
      }),
    }),
    FailedSignGuestbook: ({ reason }) => ({
      model: modifyFields(model, { signing: () => Signing.Failed({ reason }) }),
    }),
    ReceivedEntry: ({ entry }) => ({
      model: modifyFields(model, {
        entries: withEntry(entry),
        liveFeed: () => LiveFeed.Connected(),
      }),
    }),
    FailedWatchEntries: ({ reason }) => ({
      model: modifyFields(model, {
        liveFeed: () => LiveFeed.Disconnected({ reason }),
      }),
    }),
    ClickedReconnect: () => ({
      model: modifyFields(model, {
        liveFeed: () => LiveFeed.Connected(),
        liveFeedGeneration: generation => generation + 1,
      }),
    }),
    ClickedTellFortune: () => {
      const nextFortuneRequestCount = model.fortuneRequestCount + 1
      return {
        model: modifyFields(model, {
          fortuneRequestCount: () => nextFortuneRequestCount,
          fortune: () =>
            Fortune.Streaming({
              requestId: nextFortuneRequestCount,
              name: model.name,
              text: '',
            }),
        }),
      }
    },
    ClickedStopFortune: () => ({
      model: modifyFields(model, {
        fortune: fortune =>
          Fortune.match(fortune, {
            Idle: () => fortune,
            Streaming: ({ text }) => Fortune.Done({ text }),
            Done: () => fortune,
            Failed: () => fortune,
          }),
      }),
    }),
    ReceivedFortuneChunk: ({ chunk }) => ({
      model: modifyFields(model, {
        fortune: fortune =>
          Fortune.match(fortune, {
            Idle: () => fortune,
            Streaming: streaming =>
              Fortune.Streaming({ ...streaming, text: streaming.text + chunk }),
            Done: () => fortune,
            Failed: () => fortune,
          }),
      }),
    }),
    CompletedStreamFortune: () => ({
      model: modifyFields(model, {
        fortune: fortune =>
          Fortune.match(fortune, {
            Idle: () => fortune,
            Streaming: ({ text }) => Fortune.Done({ text }),
            Done: () => fortune,
            Failed: () => fortune,
          }),
      }),
    }),
    FailedStreamFortune: ({ reason }) => ({
      model: modifyFields(model, { fortune: () => Fortune.Failed({ reason }) }),
    }),
  })

// COMMAND

const describeRejection = (
  reason: 'MissingName' | 'MissingText' | 'TextTooLong',
): string => {
  if (reason === 'MissingName') {
    return 'Add your name before signing.'
  } else if (reason === 'MissingText') {
    return 'Write a message before signing.'
  } else {
    return `Keep your message under ${MAX_TEXT_LENGTH} characters.`
  }
}

export const SignEntry = Command.define('SignEntry', {
  args: { name: Schema.String, text: Schema.String },
  messages: [Message.SucceededSignGuestbook, Message.FailedSignGuestbook],
  execute: ({ name, text }) =>
    SignGuestbook.call({ name, text }).pipe(
      Effect.map(entry => Message.SucceededSignGuestbook({ entry })),
      Effect.catchTags({
        SignatureRejected: ({ reason }) =>
          Effect.succeed(
            Message.FailedSignGuestbook({ reason: describeRejection(reason) }),
          ),
        ServerFunctionError: () =>
          Effect.succeed(
            Message.FailedSignGuestbook({
              reason: 'The server could not be reached. Try again.',
            }),
          ),
      }),
    ),
})

// SUBSCRIPTION

const FortuneRequest = Schema.Struct({
  requestId: Schema.Number,
  name: Schema.String,
})

export const subscriptions = Subscription.make<
  Model,
  Message,
  ServerFunctionClient
>()(entry => ({
  liveEntries: entry(
    { liveFeedGeneration: Schema.Number },
    {
      modelToDependencies: model => ({
        liveFeedGeneration: model.liveFeedGeneration,
      }),
      dependenciesToStream: () =>
        WatchEntries.call().pipe(
          Stream.map(entry => Message.ReceivedEntry({ entry })),
          Stream.catch(error =>
            Stream.make(Message.FailedWatchEntries({ reason: error.message })),
          ),
        ),
    },
  ),

  fortune: entry(
    { maybeFortuneRequest: Schema.Option(FortuneRequest) },
    {
      modelToDependencies: model => ({
        maybeFortuneRequest: Fortune.match(model.fortune, {
          Idle: () => Option.none(),
          Streaming: ({ requestId, name }) => Option.some({ requestId, name }),
          Done: () => Option.none(),
          Failed: () => Option.none(),
        }),
      }),
      dependenciesToStream: ({ maybeFortuneRequest }) =>
        Option.match(maybeFortuneRequest, {
          onNone: () => Stream.empty,
          onSome: ({ name }) =>
            StreamFortune.call({ name }).pipe(
              Stream.map(chunk => Message.ReceivedFortuneChunk({ chunk })),
              Stream.concat(Stream.make(Message.CompletedStreamFortune())),
              Stream.catch(error =>
                Stream.make(
                  Message.FailedStreamFortune({ reason: error.message }),
                ),
              ),
            ),
        }),
    },
  ),
}))

// VIEW

const formView = (model: Model, h: HtmlBuilder<Message>): Html => {
  const isSending = Signing.match(model.signing, {
    Idle: () => false,
    Sending: () => true,
    Failed: () => false,
  })

  return h.form(
    [h.Class('card sign-form'), h.OnSubmit(Message.SubmittedSignForm())],
    [
      h.label([h.Class('field')], [
        h.span([], ['Name']),
        h.input([
          h.Id('name'),
          h.Value(model.name),
          h.OnInput(name => Message.UpdatedName({ name })),
        ]),
      ]),
      h.label([h.Class('field')], [
        h.span([], ['Message']),
        h.textarea([
          h.Id('text'),
          h.Value(model.text),
          h.OnInput(text => Message.UpdatedText({ text })),
        ]),
      ]),
      h.div(
        [h.Class('actions')],
        [
          h.button(
            [h.Type('submit'), h.Disabled(isSending)],
            [isSending ? 'Signing...' : 'Sign the guestbook'],
          ),
          fortuneButtonView(model, h),
        ],
      ),
      Signing.match(model.signing, {
        Idle: () => h.empty,
        Sending: () => h.empty,
        Failed: ({ reason }) =>
          h.p([h.Class('error'), h.Id('sign-error')], [reason]),
      }),
    ],
  )
}

const fortuneButtonView = (model: Model, h: HtmlBuilder<Message>): Html =>
  Fortune.match(model.fortune, {
    Idle: () => tellFortuneButton(h),
    Streaming: () =>
      h.button(
        [h.Type('button'), h.Class('secondary'), h.OnClick(Message.ClickedStopFortune())],
        ['Stop'],
      ),
    Done: () => tellFortuneButton(h),
    Failed: () => tellFortuneButton(h),
  })

const tellFortuneButton = (h: HtmlBuilder<Message>): Html =>
  h.button(
    [h.Type('button'), h.Class('secondary'), h.OnClick(Message.ClickedTellFortune())],
    ['Tell my fortune'],
  )

const fortuneView = (model: Model, h: HtmlBuilder<Message>): Html =>
  Fortune.match(model.fortune, {
    Idle: () => h.empty,
    Streaming: ({ text }) =>
      h.p([h.Class('card fortune streaming'), h.Id('fortune'), h.AriaLive('polite')], [text]),
    Done: ({ text }) => h.p([h.Class('card fortune'), h.Id('fortune')], [text]),
    Failed: ({ reason }) => h.p([h.Class('card error'), h.Id('fortune')], [reason]),
  })

const liveFeedView = (model: Model, h: HtmlBuilder<Message>): Html =>
  LiveFeed.match(model.liveFeed, {
    Connected: () => h.p([h.Class('status')], ['Live: new entries appear as they are signed.']),
    Disconnected: ({ reason }) =>
      h.p(
        [h.Class('status error')],
        [
          `Live updates stopped: ${reason} `,
          h.button(
            [h.Type('button'), h.Class('link'), h.OnClick(Message.ClickedReconnect())],
            ['Reconnect'],
          ),
        ],
      ),
  })

const entryView = (entry: Entry, h: HtmlBuilder<Message>): Html =>
  h.keyed('li')(entry.id, [h.Class('card entry')], [
    h.p([h.Class('entry-text')], [entry.text]),
    h.p([h.Class('entry-meta')], [`${entry.name}, ${entry.signedAt.slice(0, 10)}`]),
  ])

export const view = (model: Model, h: HtmlBuilder<Message>): Document => ({
  title: `Guestbook (${model.entries.length})`,
  body: h.main(
    [h.Class('page')],
    [
      h.h1([], ['Guestbook']),
      h.p(
        [h.Class('lede')],
        [
          'Entries are loaded on the server for the first render, signing calls a ',
          'server function, and new entries stream in from other tabs.',
        ],
      ),
      formView(model, h),
      fortuneView(model, h),
      liveFeedView(model, h),
      h.ul(
        [h.Class('entries'), h.Id('entries')],
        Array.map(model.entries, entry => entryView(entry, h)),
      ),
    ],
  ),
})
