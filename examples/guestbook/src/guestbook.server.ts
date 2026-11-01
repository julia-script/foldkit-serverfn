import {
  Array,
  Context,
  DateTime,
  Effect,
  Exit,
  Layer,
  PubSub,
  Ref,
  Stream,
  String,
} from 'effect'

import { type Entry, MAX_TEXT_LENGTH } from './domain'
import { GuestbookFunctions, SignatureRejected } from './serverFunction'

// GUESTBOOK

type GuestbookService = Readonly<{
  entries: Effect.Effect<ReadonlyArray<Entry>>
  sign: (name: string, text: string) => Effect.Effect<Entry, SignatureRejected>
  watch: Stream.Stream<Entry>
}>

export class Guestbook extends Context.Service<Guestbook, GuestbookService>()(
  'Guestbook',
) {}

const SEED_ENTRIES: ReadonlyArray<Entry> = [
  {
    id: 'seed-1',
    name: 'Ada',
    text: 'Rendered on the server, hydrated in the browser. Lovely.',
    signedAt: '2026-11-01T00:00:00.000Z',
  },
]

const validate = (
  name: string,
  text: string,
): Effect.Effect<Readonly<{ name: string; text: string }>, SignatureRejected> => {
  const trimmedName = String.trim(name)
  const trimmedText = String.trim(text)

  if (String.isEmpty(trimmedName)) {
    return Effect.fail(new SignatureRejected({ reason: 'MissingName' }))
  } else if (String.isEmpty(trimmedText)) {
    return Effect.fail(new SignatureRejected({ reason: 'MissingText' }))
  } else if (trimmedText.length > MAX_TEXT_LENGTH) {
    return Effect.fail(new SignatureRejected({ reason: 'TextTooLong' }))
  } else {
    return Effect.succeed({ name: trimmedName, text: trimmedText })
  }
}

export const GuestbookLive = Layer.effect(Guestbook)(
  Effect.gen(function* () {
    const entriesRef = yield* Ref.make(SEED_ENTRIES)
    const signed = yield* PubSub.unbounded<Entry>()

    const sign = (name: string, text: string) =>
      Effect.gen(function* () {
        const valid = yield* validate(name, text)
        const now = yield* DateTime.now
        const entry: Entry = {
          id: crypto.randomUUID(),
          name: valid.name,
          text: valid.text,
          signedAt: DateTime.formatIso(now),
        }
        yield* Ref.update(entriesRef, Array.prepend(entry))
        yield* PubSub.publish(signed, entry)
        return entry
      })

    return Guestbook.of({
      entries: Ref.get(entriesRef),
      sign,
      watch: Stream.fromPubSub(signed),
    })
  }),
)

// FORTUNE

const FORTUNES: Array.NonEmptyReadonlyArray<string> = [
  'A typed error today keeps the crash view away.',
  'Your next Message will be a fact, not a wish.',
  'The Model you seek is already the single source of truth.',
  'Interrupt what no longer serves you, and the server will notice.',
]

const FORTUNE_WORD_DELAY = '150 millis'

const fortuneFor = (name: string): string => {
  const seed = Array.reduce(globalThis.Array.from(name), 0, (sum, character) =>
    sum + (character.codePointAt(0) ?? 0),
  )
  return Array.getUnsafe(FORTUNES, seed % FORTUNES.length)
}

const streamFortune = (name: string): Stream.Stream<string> =>
  Stream.fromIterable(String.split(fortuneFor(name), ' ')).pipe(
    Stream.map((word, index) => (index === 0 ? word : ` ${word}`)),
    Stream.tap(() => Effect.sleep(FORTUNE_WORD_DELAY)),
    Stream.onExit(exit =>
      Exit.hasInterrupts(exit)
        ? Effect.log(`Fortune for ${name} cancelled by the client`)
        : Effect.void,
    ),
  )

// HANDLERS

export const GuestbookFunctionsLive = GuestbookFunctions.toLayer(
  Effect.gen(function* () {
    const guestbook = yield* Guestbook
    return {
      GetEntries: () => guestbook.entries,
      SignGuestbook: ({ name, text }) => guestbook.sign(name, text),
      WatchEntries: () => guestbook.watch,
      StreamFortune: ({ name }) => streamFortune(name),
    }
  }),
).pipe(Layer.provide(GuestbookLive))
