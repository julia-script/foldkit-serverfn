import { Schema } from 'effect'
import * as ServerFunction from 'foldkit-serverfn'

import { ChatMessage, ConversationEvent } from './domain'

// ERROR

export class MessageRejected extends Schema.TaggedError<MessageRejected>()(
  'MessageRejected',
  {
    reason: Schema.Literals(['MissingAuthor', 'MissingText', 'TextTooLong']),
  },
) {}

// SERVER FUNCTION

export const GetConversation = ServerFunction.define('GetConversation', {
  success: Schema.Array(ChatMessage),
})

export const SendMessage = ServerFunction.define('SendMessage', {
  payload: { author: Schema.String, text: Schema.String },
  success: ChatMessage,
  error: MessageRejected,
})

export const StopReplies = ServerFunction.define('StopReplies')

export const WatchConversation = ServerFunction.define('WatchConversation', {
  success: ConversationEvent,
  stream: true,
})

export const ChatFunctions = ServerFunction.group(
  GetConversation,
  SendMessage,
  StopReplies,
  WatchConversation,
)
