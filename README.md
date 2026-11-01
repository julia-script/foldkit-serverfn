# foldkit-serverfn

Schema-typed server functions and streaming for [Foldkit](https://foldkit.dev), built on `effect/rpc`. It uses only Foldkit's public API, so it works with a stock Foldkit app.

You declare a server function once with Effect Schemas for its payload, success value, and error. The browser calls it from a Command and gets back an `Effect`. A function declared with `stream: true` returns an Effect `Stream`, which a Subscription consumes. During a server render, the same call runs the handler in process, and the result reaches the client through Flags.

TanStack Start's `createServerFn` was the reference. The main differences come from Foldkit's architecture. Calls happen inside Commands and Subscriptions, never in views. Errors are typed values instead of thrown control flow. Schemas define the wire format.

## Packages

- [`packages/serverfn`](packages/serverfn): the library, published as `foldkit-serverfn`.
- [`examples/guestbook`](examples/guestbook): a server-rendered guestbook. It loads entries in process for the first render, signs entries through a server function with a typed validation error, streams new entries to every open tab, and streams a fortune word by word that you can stop halfway.
- [`examples/chat`](examples/chat): a group chat with an AI assistant, built on `effect/ai`. Messages are submitted with a server function, and every open tab follows one conversation stream that carries new messages and the assistant's reply as it is generated. Anyone can stop a reply halfway. With `ANTHROPIC_API_KEY` set, replies come from Claude through `@effect/ai-anthropic` (`ANTHROPIC_MODEL` picks the model). Without a key, an offline model streams a canned reply, so the example runs with no account.

## Usage

### 1. Declare server functions

The declaration module holds only Schemas, so the browser can import it.

```ts
// src/serverFunction.ts
import { Schema } from 'effect'
import * as ServerFunction from 'foldkit-serverfn'

export class SignatureRejected extends Schema.TaggedError<SignatureRejected>()(
  'SignatureRejected',
  { reason: Schema.Literals(['MissingName', 'MissingText', 'TextTooLong']) },
) {}

export const SignGuestbook = ServerFunction.define('SignGuestbook', {
  payload: { name: Schema.String, text: Schema.String },
  success: Entry,
  error: SignatureRejected,
})

export const WatchEntries = ServerFunction.define('WatchEntries', {
  success: Entry,
  stream: true,
})

export const GuestbookFunctions = ServerFunction.group(SignGuestbook, WatchEntries)
```

### 2. Implement them on the server

`group(...).toLayer` is `RpcGroup.toLayer`. Each handler is an Effect, or a Stream for streaming functions, and can use any service its Layer provides.

```ts
// src/guestbook.server.ts
export const GuestbookFunctionsLive = GuestbookFunctions.toLayer(
  Effect.gen(function* () {
    const guestbook = yield* Guestbook
    return {
      SignGuestbook: ({ name, text }) => guestbook.sign(name, text),
      WatchEntries: () => guestbook.watch,
    }
  }),
).pipe(Layer.provide(GuestbookLive))
```

### 3. Serve them from the Foldkit server entry

`makeServer` builds the handler Layer once. `route` answers `POST /_serverfn` and passes every other request to your page render. `layerInProcess` calls the same handlers directly during a server render.

```ts
// src/entry.server.ts
import { makeServer } from 'foldkit-serverfn/server'

const server = makeServer(GuestbookFunctions, { layer: GuestbookFunctionsLive })

export const renderDocument = Server.renderDocument
export const handleServerFunction = server.handle

export const renderPage = (request: Request) =>
  server.route(request, renderApplication)

const loadFlags = GetEntries.call().pipe(
  Effect.map(entries => Flags.make({ entries })),
  Effect.catch(() => Effect.succeed(Flags.make({ entries: [] }))),
  Effect.provide(server.layerInProcess),
)
```

### 4. Provide the client in `resources`

```ts
// src/entry.ts
import { layerClient } from 'foldkit-serverfn/client'

const application = Runtime.makeApplication({
  // ...
  subscriptions,
  resources: layerClient(GuestbookFunctions),
})
```

### 5. Call from Commands and Subscriptions

```ts
export const SignEntry = Command.define('SignEntry', {
  args: { name: Schema.String, text: Schema.String },
  messages: [Message.SucceededSignGuestbook, Message.FailedSignGuestbook],
  execute: ({ name, text }) =>
    SignGuestbook.call({ name, text }).pipe(
      Effect.map(entry => Message.SucceededSignGuestbook({ entry })),
      Effect.catchTags({
        SignatureRejected: ({ reason }) =>
          Effect.succeed(Message.FailedSignGuestbook({ reason })),
        ServerFunctionError: () =>
          Effect.succeed(Message.FailedSignGuestbook({ reason: 'Unreachable' })),
      }),
    ),
})
```

A streaming function is consumed by a Subscription. When the Model changes the Subscription's dependencies, Foldkit interrupts the stream. The interruption aborts the request, and the handler's Stream on the server is interrupted too.

```ts
liveEntries: entry(
  { liveFeedGeneration: Schema.Number },
  {
    modelToDependencies: model => ({ liveFeedGeneration: model.liveFeedGeneration }),
    dependenciesToStream: () =>
      WatchEntries.call().pipe(
        Stream.map(entry => Message.ReceivedEntry({ entry })),
        Stream.catch(error =>
          Stream.make(Message.FailedWatchEntries({ reason: error.message })),
        ),
      ),
  },
),
```

### 6. Add the dev plugin

Foldkit's dev SSR middleware buffers a whole response before sending it, which would hold a stream back until it ends. The plugin answers server function requests first and streams them. List it before `foldkit()`. Production builds do not need it, because `renderPage` routes these requests.

```ts
// vite.config.ts
import { serverFunctions } from 'foldkit-serverfn/vite'

export default defineConfig({
  plugins: [
    serverFunctions({ serverEntry: '/src/entry.server.ts' }),
    foldkit({ ssr: { serverEntry: '/src/entry.server.ts', clientEntry: '/src/entry.ts', build: true } }),
  ],
})
```

## Errors

Every `call` can fail with `ServerFunctionError` in addition to the declared error. Its `reason` is one of these:

- `Transport`: the request failed, or the response could not be decoded.
- `Defect`: the handler died instead of failing with a declared error.
- `NotRegistered`: the client's group does not contain the function.

Converting defects to a typed error means a server bug becomes a Message instead of crashing the client.

## Security

Server functions are public HTTP endpoints. Every payload is decoded with its Schema before the handler runs. Requests use `content-type: application/x-ndjson`, so a cross-origin browser call needs a CORS preflight. In addition, `makeServer` refuses requests whose `Sec-Fetch-Site` or `Origin` names another site, unless you pass `isSameOriginRequired: false`. Authentication is up to you, for example with `RpcMiddleware` through `ServerFunction.middleware`.

## Limitations

- Calls always use `POST`, so responses are not cacheable by URL.
- Handlers can read request headers, but cannot set response headers or cookies.
- There is no check that the browser and server come from the same deployment.
- It depends on `foldkit/experimental/server`, which may change between Foldkit releases.
- Keep server-only modules out of browser imports yourself. The example uses a `.server.ts` suffix as a convention.

## Development

```sh
pnpm install
pnpm build        # builds the library, which the example's Vite config imports
pnpm test
pnpm typecheck
pnpm dev          # runs the guestbook example at http://localhost:5173
pnpm dev:chat     # runs the chat example; set ANTHROPIC_API_KEY to use Claude
```

To try the production build, run `pnpm --filter guestbook-example build`, then `pnpm --filter guestbook-example start`.
