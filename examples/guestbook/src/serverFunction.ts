import { Schema } from 'effect'
import * as ServerFunction from 'foldkit-serverfn'

import { Entry } from './domain'

// ERROR

export class SignatureRejected extends Schema.TaggedError<SignatureRejected>()(
  'SignatureRejected',
  {
    reason: Schema.Literals(['MissingName', 'MissingText', 'TextTooLong']),
  },
) {}

// SERVER FUNCTION

export const GetEntries = ServerFunction.define('GetEntries', {
  success: Schema.Array(Entry),
})

export const SignGuestbook = ServerFunction.define('SignGuestbook', {
  payload: { name: Schema.String, text: Schema.String },
  success: Entry,
  error: SignatureRejected,
})

export const WatchEntries = ServerFunction.define('WatchEntries', {
  success: Entry,
  stream: true,
})

export const StreamFortune = ServerFunction.define('StreamFortune', {
  payload: { name: Schema.String },
  success: Schema.String,
  stream: true,
})

export const GuestbookFunctions = ServerFunction.group(
  GetEntries,
  SignGuestbook,
  WatchEntries,
  StreamFortune,
)
