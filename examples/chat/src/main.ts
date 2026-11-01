import { Array, Effect, Match, Schema, Stream } from 'effect'
import { Command, Subscription, type Update } from 'foldkit'
import type { Document, Html, HtmlBuilder } from 'foldkit/html'
import { defineMessageUnion } from 'foldkit/message'
import { defineTaggedUnion } from 'foldkit/schema'
import { modifyFields } from 'foldkit/struct'
import type { ServerFunctionClient } from 'foldkit-serverfn'

import {
  ChatMessage,
  ConversationEvent,
  MAX_MESSAGE_LENGTH,
} from './domain'
import { SendMessage, StopReplies, WatchConversation } from './serverFunction'

// MODEL

const Sending = defineTaggedUnion({
  Idle: {},
  Sending: {},
  Failed: { reason: Schema.String },
})

const Connection = defineTaggedUnion({
  Connecting: {},
  Live: {},
  Disconnected: { reason: Schema.String },
})

export const Model = Schema.Struct({
  messages: Schema.Array(ChatMessage),
  author: Schema.String,
  draft: Schema.String,
  sending: Sending,
  connection: Connection,
  connectionGeneration: Schema.Number,
})
export type Model = typeof Model.Type

// FLAGS

export const Flags = Schema.Struct({
  messages: Schema.Array(ChatMessage),
})
export type Flags = typeof Flags.Type

// MESSAGE

export const Message = defineMessageUnion({
  UpdatedAuthor: { author: Schema.String },
  UpdatedDraft: { draft: Schema.String },
  SubmittedMessageForm: {},
  SucceededSendMessage: { message: ChatMessage },
  FailedSendMessage: { reason: Schema.String },
  ClickedStopReplies: {},
  CompletedStopReplies: {},
  ReceivedConversationEvent: { event: ConversationEvent },
  FailedWatchConversation: { reason: Schema.String },
  ClickedReconnect: {},
})
export type Message = typeof Message.Type

type UpdateReturn = Update.Return<Model, Message, ServerFunctionClient>

// INIT

export const init = (flags: Flags): UpdateReturn => ({
  model: {
    messages: flags.messages,
    author: '',
    draft: '',
    sending: Sending.Idle(),
    connection: Connection.Connecting(),
    connectionGeneration: 0,
  },
})

// UPDATE

const withMessage =
  (message: ChatMessage) =>
  (messages: ReadonlyArray<ChatMessage>): ReadonlyArray<ChatMessage> =>
    Array.some(messages, ({ id }) => id === message.id)
      ? messages
      : Array.append(messages, message)

const withUpdatedMessage =
  (messageId: string, transform: (message: ChatMessage) => ChatMessage) =>
  (messages: ReadonlyArray<ChatMessage>): ReadonlyArray<ChatMessage> =>
    Array.map(messages, message =>
      message.id === messageId ? transform(message) : message,
    )

const applyEvent = (
  messages: ReadonlyArray<ChatMessage>,
  event: ConversationEvent,
): ReadonlyArray<ChatMessage> =>
  Match.value(event).pipe(
    Match.withReturnType<ReadonlyArray<ChatMessage>>(),
    Match.tagsExhaustive({
      SyncedConversation: synced => synced.messages,
      AddedMessage: ({ message }) => withMessage(message)(messages),
      AppendedText: ({ messageId, delta }) =>
        withUpdatedMessage(messageId, message => ({
          ...message,
          text: message.text + delta,
        }))(messages),
      EndedReply: ({ messageId, status }) =>
        withUpdatedMessage(messageId, message => ({ ...message, status }))(
          messages,
        ),
    }),
  )

export const update = (model: Model, message: Message) =>
  Message.match<UpdateReturn>(message, {
    UpdatedAuthor: ({ author }) => ({
      model: modifyFields(model, { author: () => author }),
    }),
    UpdatedDraft: ({ draft }) => ({
      model: modifyFields(model, { draft: () => draft }),
    }),
    SubmittedMessageForm: () => ({
      model: modifyFields(model, { sending: () => Sending.Sending() }),
      commands: [SendChatMessage({ author: model.author, text: model.draft })],
    }),
    SucceededSendMessage: ({ message: sent }) => ({
      model: modifyFields(model, {
        messages: withMessage(sent),
        draft: () => '',
        sending: () => Sending.Idle(),
      }),
    }),
    FailedSendMessage: ({ reason }) => ({
      model: modifyFields(model, { sending: () => Sending.Failed({ reason }) }),
    }),
    ClickedStopReplies: () => ({
      model,
      commands: [StopAllReplies()],
    }),
    CompletedStopReplies: () => ({ model }),
    ReceivedConversationEvent: ({ event }) => ({
      model: modifyFields(model, {
        messages: messages => applyEvent(messages, event),
        connection: () => Connection.Live(),
      }),
    }),
    FailedWatchConversation: ({ reason }) => ({
      model: modifyFields(model, {
        connection: () => Connection.Disconnected({ reason }),
      }),
    }),
    ClickedReconnect: () => ({
      model: modifyFields(model, {
        connection: () => Connection.Connecting(),
        connectionGeneration: generation => generation + 1,
      }),
    }),
  })

// COMMAND

const describeRejection = (
  reason: 'MissingAuthor' | 'MissingText' | 'TextTooLong',
): string =>
  Match.value(reason).pipe(
    Match.withReturnType<string>(),
    Match.when('MissingAuthor', () => 'Add your name first.'),
    Match.when('MissingText', () => 'Write a message first.'),
    Match.when(
      'TextTooLong',
      () => `Keep messages under ${MAX_MESSAGE_LENGTH} characters.`,
    ),
    Match.exhaustive,
  )

export const SendChatMessage = Command.define('SendChatMessage', {
  args: { author: Schema.String, text: Schema.String },
  messages: [Message.SucceededSendMessage, Message.FailedSendMessage],
  execute: ({ author, text }) =>
    SendMessage.call({ author, text }).pipe(
      Effect.map(message => Message.SucceededSendMessage({ message })),
      Effect.catchTags({
        MessageRejected: ({ reason }) =>
          Effect.succeed(
            Message.FailedSendMessage({ reason: describeRejection(reason) }),
          ),
        ServerFunctionError: () =>
          Effect.succeed(
            Message.FailedSendMessage({
              reason: 'The server could not be reached. Try again.',
            }),
          ),
      }),
    ),
})

export const StopAllReplies = Command.define('StopAllReplies', {
  messages: [Message.CompletedStopReplies],
  execute: StopReplies.call().pipe(
    Effect.as(Message.CompletedStopReplies()),
    Effect.catch(() => Effect.succeed(Message.CompletedStopReplies())),
  ),
})

// SUBSCRIPTION

export const subscriptions = Subscription.make<
  Model,
  Message,
  ServerFunctionClient
>()(entry => ({
  conversation: entry(
    { connectionGeneration: Schema.Number },
    {
      modelToDependencies: model => ({
        connectionGeneration: model.connectionGeneration,
      }),
      dependenciesToStream: () =>
        WatchConversation.call().pipe(
          Stream.map(event => Message.ReceivedConversationEvent({ event })),
          Stream.catch(error =>
            Stream.succeed(
              Message.FailedWatchConversation({ reason: error.message }),
            ),
          ),
        ),
    },
  ),
}))

// VIEW

const isReplying = (model: Model): boolean =>
  Array.some(model.messages, ({ status }) => status === 'Streaming')

const statusLabel = (message: ChatMessage): string =>
  Match.value(message.status).pipe(
    Match.withReturnType<string>(),
    Match.when('Complete', () => ''),
    Match.when('Streaming', () => ''),
    Match.when('Stopped', () => ' (stopped)'),
    Match.when('Failed', () => ' (failed)'),
    Match.exhaustive,
  )

const messageView = (message: ChatMessage, h: HtmlBuilder<Message>): Html =>
  h.keyed('li')(
    message.id,
    [
      h.Class(
        `bubble ${message.role === 'User' ? 'user' : 'assistant'} ${
          message.status === 'Streaming' ? 'streaming' : ''
        }`,
      ),
    ],
    [
      h.p([h.Class('author')], [`${message.author}${statusLabel(message)}`]),
      h.p([h.Class('text')], [message.text]),
    ],
  )

const connectionView = (model: Model, h: HtmlBuilder<Message>): Html =>
  Connection.match(model.connection, {
    Connecting: () => h.p([h.Class('status'), h.Id('connection')], ['Connecting...']),
    Live: () => h.p([h.Class('status'), h.Id('connection')], ['Live']),
    Disconnected: ({ reason }) =>
      h.p(
        [h.Class('status error'), h.Id('connection')],
        [
          `Disconnected: ${reason} `,
          h.button(
            [h.Type('button'), h.Class('link'), h.OnClick(Message.ClickedReconnect())],
            ['Reconnect'],
          ),
        ],
      ),
  })

const composerView = (model: Model, h: HtmlBuilder<Message>): Html => {
  const isSending = Sending.match(model.sending, {
    Idle: () => false,
    Sending: () => true,
    Failed: () => false,
  })
  const isBusy = isSending || isReplying(model)

  return h.form(
    [h.Class('composer'), h.OnSubmit(Message.SubmittedMessageForm())],
    [
      h.input([
        h.Id('author'),
        h.Class('author-input'),
        h.Placeholder('Your name'),
        h.Value(model.author),
        h.OnInput(author => Message.UpdatedAuthor({ author })),
      ]),
      h.input([
        h.Id('draft'),
        h.Class('draft-input'),
        h.Placeholder('Say something'),
        h.Value(model.draft),
        h.OnInput(draft => Message.UpdatedDraft({ draft })),
      ]),
      isReplying(model)
        ? h.button(
            [
              h.Type('button'),
              h.Class('secondary'),
              h.OnClick(Message.ClickedStopReplies()),
            ],
            ['Stop'],
          )
        : h.button([h.Type('submit'), h.Disabled(isBusy)], ['Send']),
      Sending.match(model.sending, {
        Idle: () => h.empty,
        Sending: () => h.empty,
        Failed: ({ reason }) =>
          h.p([h.Class('error'), h.Id('send-error')], [reason]),
      }),
    ],
  )
}

export const view = (model: Model, h: HtmlBuilder<Message>): Document => ({
  title: isReplying(model) ? 'Chat (replying...)' : 'Chat',
  body: h.main(
    [h.Class('page')],
    [
      h.header(
        [h.Class('header')],
        [h.h1([], ['Chat']), connectionView(model, h)],
      ),
      Array.match(model.messages, {
        onEmpty: () =>
          h.p(
            [h.Class('empty')],
            ['No messages yet. Everyone with this page open sees the same chat.'],
          ),
        onNonEmpty: messages =>
          h.ul(
            [h.Class('messages'), h.Id('messages')],
            Array.map(messages, message => messageView(message, h)),
          ),
      }),
      composerView(model, h),
    ],
  ),
})
