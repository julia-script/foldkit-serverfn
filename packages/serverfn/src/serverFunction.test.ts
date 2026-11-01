import { Context, Deferred, Effect, Exit, Fiber, Layer, Ref, Schema, Stream } from 'effect'
import { afterAll, describe, expect, it, vi } from 'vitest'

import { layerClient } from './client.js'
import { makeServer } from './server.js'
import { ServerFunctionError, define, group } from './serverFunction.js'

class TooLong extends Schema.TaggedError<TooLong>()('TooLong', {
  maxLength: Schema.Number,
}) {}

const Echo = define('Echo', {
  payload: { text: Schema.String },
  success: Schema.String,
  error: TooLong,
})

const Count = define('Count', {
  payload: { to: Schema.Number },
  success: Schema.Number,
  stream: true,
})

const Forever = define('Forever', {
  success: Schema.Number,
  stream: true,
})

const ReadCalls = define('ReadCalls', { success: Schema.Number })

const Unserved = define('Unserved', { success: Schema.String })

const TestFunctions = group(Echo, Count, Forever, ReadCalls)

const MAX_LENGTH = 5

class Calls extends Context.Service<Calls, Ref.Ref<number>>()('Calls') {}

const foreverInterrupted = Deferred.makeUnsafe<void>()

const TestFunctionsLive = TestFunctions.toLayer(
  Effect.gen(function* () {
    const calls = yield* Calls
    return {
      Echo: ({ text }) =>
        Ref.update(calls, count => count + 1).pipe(
          Effect.andThen(
            text.length > MAX_LENGTH
              ? Effect.fail(new TooLong({ maxLength: MAX_LENGTH }))
              : Effect.succeed(text.toUpperCase()),
          ),
        ),
      Count: ({ to }) => Stream.range(1, to),
      Forever: () =>
        Stream.iterate(0, count => count + 1).pipe(
          Stream.tap(() => Effect.sleep('10 millis')),
          Stream.onExit(() => Deferred.succeed(foreverInterrupted, undefined)),
        ),
      ReadCalls: () => Ref.get(calls),
    }
  }),
).pipe(Layer.provide(Layer.effect(Calls)(Ref.make(0))))

const server = makeServer(TestFunctions, { layer: TestFunctionsLive })

const ORIGIN = 'http://app.test'

vi.stubGlobal('location', { href: `${ORIGIN}/` })
vi.stubGlobal('fetch', (input: RequestInfo | URL, init?: RequestInit) =>
  server.handle(
    new Request(input, {
      ...init,
      headers: { ...Object.fromEntries(new Headers(init?.headers)), 'sec-fetch-site': 'same-origin' },
    }),
  ),
)

afterAll(() => server.dispose())

const ClientLive = layerClient(TestFunctions)

describe('server functions over HTTP', () => {
  it('returns the decoded success value', async () => {
    const result = await Effect.runPromise(
      Echo.call({ text: 'hi' }).pipe(Effect.provide(ClientLive)),
    )
    expect(result).toBe('HI')
  })

  it('fails with the declared error as a typed value', async () => {
    const result = await Effect.runPromise(
      Echo.call({ text: 'too long' }).pipe(
        Effect.catchTag('TooLong', error =>
          Effect.succeed(`max ${error.maxLength}`),
        ),
        Effect.provide(ClientLive),
      ),
    )
    expect(result).toBe('max 5')
  })

  it('streams every chunk in order', async () => {
    const values = await Effect.runPromise(
      Count.call({ to: 4 }).pipe(Stream.runCollect, Effect.provide(ClientLive)),
    )
    expect(Array.from(values)).toEqual([1, 2, 3, 4])
  })

  it('interrupts the server stream when the client stops reading', async () => {
    const values = await Effect.runPromise(
      Forever.call().pipe(
        Stream.take(3),
        Stream.runCollect,
        Effect.provide(ClientLive),
      ),
    )
    expect(Array.from(values)).toEqual([0, 1, 2])
    await Effect.runPromise(
      Deferred.await(foreverInterrupted).pipe(Effect.timeout('2 seconds')),
    )
  })

  it('fails with ServerFunctionError when the group lacks the function', async () => {
    const error = await Effect.runPromise(
      Unserved.call().pipe(Effect.flip, Effect.provide(ClientLive)),
    )
    expect(error).toBeInstanceOf(ServerFunctionError)
    expect(error.reason).toBe('NotRegistered')
  })
})

describe('request handling', () => {
  it('refuses a cross-site request', async () => {
    const response = await server.handle(
      new Request(`${ORIGIN}/_serverfn`, {
        method: 'POST',
        headers: { 'sec-fetch-site': 'cross-site' },
      }),
    )
    expect(response.status).toBe(403)
  })

  it('routes only POST requests on its path', () => {
    expect(
      server.isServerFunctionRequest(
        new Request(`${ORIGIN}/_serverfn`, { method: 'POST' }),
      ),
    ).toBe(true)
    expect(
      server.isServerFunctionRequest(
        new Request(`${ORIGIN}/_serverfn/`, { method: 'POST' }),
      ),
    ).toBe(true)
    expect(
      server.isServerFunctionRequest(new Request(`${ORIGIN}/_serverfn`)),
    ).toBe(false)
    expect(
      server.isServerFunctionRequest(
        new Request(`${ORIGIN}/other`, { method: 'POST' }),
      ),
    ).toBe(false)
  })

  it('passes other requests to renderPage', async () => {
    const result = await server.route(new Request(`${ORIGIN}/`), () =>
      Promise.resolve({ _tag: 'Responded', response: new Response('page') }),
    )
    expect(result._tag).toBe('Responded')
  })
})

describe('in-process calls', () => {
  it('shares handler services with HTTP calls', async () => {
    const before = await Effect.runPromise(
      ReadCalls.call().pipe(Effect.provide(server.layerInProcess)),
    )
    await Effect.runPromise(
      Echo.call({ text: 'one' }).pipe(Effect.provide(ClientLive)),
    )
    const after = await Effect.runPromise(
      ReadCalls.call().pipe(Effect.provide(server.layerInProcess)),
    )
    expect(after).toBe(before + 1)
  })

  it('streams without HTTP', async () => {
    const values = await Effect.runPromise(
      Count.call({ to: 2 }).pipe(
        Stream.runCollect,
        Effect.provide(server.layerInProcess),
      ),
    )
    expect(Array.from(values)).toEqual([1, 2])
  })

  it('can be interrupted', async () => {
    const exit = await Effect.runPromise(
      Effect.gen(function* () {
        const fiber = yield* Forever.call().pipe(
          Stream.runDrain,
          Effect.provide(server.layerInProcess),
          Effect.forkChild,
        )
        yield* Effect.sleep('30 millis')
        return yield* Fiber.interrupt(fiber).pipe(Effect.andThen(Fiber.await(fiber)))
      }),
    )
    expect(Exit.hasInterrupts(exit)).toBe(true)
  })
})
