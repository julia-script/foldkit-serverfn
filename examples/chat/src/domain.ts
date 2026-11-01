import { Schema } from 'effect'

export const ChatRole = Schema.Literals(['User', 'Assistant'])
export type ChatRole = typeof ChatRole.Type

export const ReplyStatus = Schema.Literals([
  'Complete',
  'Streaming',
  'Stopped',
  'Failed',
])
export type ReplyStatus = typeof ReplyStatus.Type

export const ChatMessage = Schema.Struct({
  id: Schema.String,
  role: ChatRole,
  author: Schema.String,
  text: Schema.String,
  status: ReplyStatus,
})
export type ChatMessage = typeof ChatMessage.Type

export const MAX_MESSAGE_LENGTH = 500

// CONVERSATION EVENT

export const SyncedConversation = Schema.TaggedStruct('SyncedConversation', {
  messages: Schema.Array(ChatMessage),
})

export const AddedMessage = Schema.TaggedStruct('AddedMessage', {
  message: ChatMessage,
})

export const AppendedText = Schema.TaggedStruct('AppendedText', {
  messageId: Schema.String,
  delta: Schema.String,
})

export const EndedReply = Schema.TaggedStruct('EndedReply', {
  messageId: Schema.String,
  status: ReplyStatus,
})

export const ConversationEvent = Schema.Union([
  SyncedConversation,
  AddedMessage,
  AppendedText,
  EndedReply,
])
export type ConversationEvent = typeof ConversationEvent.Type
