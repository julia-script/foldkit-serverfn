import { Schema } from 'effect'

export const Entry = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  text: Schema.String,
  signedAt: Schema.String,
})
export type Entry = typeof Entry.Type

export const MAX_TEXT_LENGTH = 140
