import { Cause, Context, Effect, Schema, Stream } from 'effect'
import { Rpc, RpcClientError, RpcGroup, RpcSchema } from 'effect/rpc'
import type { RpcMiddleware } from 'effect/rpc'

// ERROR

/**
 * Why a server function call failed outside the function's own declared
 * errors.
 *
 * - `Transport`: the request did not reach the server, or the response could
 *   not be read or decoded.
 * - `Defect`: the handler died instead of failing with a declared error.
 * - `NotRegistered`: the client was built from a group that does not contain
 *   this server function.
 */
export const ServerFunctionErrorReason = Schema.Literals([
  'Transport',
  'Defect',
  'NotRegistered',
])
export type ServerFunctionErrorReason = typeof ServerFunctionErrorReason.Type

/**
 * The failure every `call` can produce in addition to the server function's
 * declared `error` Schema. A Command matches it like any other tagged error
 * and turns it into a Message, so an unreachable server or a server bug never
 * crashes the client.
 */
export class ServerFunctionError extends Schema.TaggedError<ServerFunctionError>()(
  'ServerFunctionError',
  {
    reason: ServerFunctionErrorReason,
    message: Schema.String,
  },
) {}

// CLIENT SERVICE

/**
 * Invokes a server function by tag. Built once per application by
 * `layerClient` in the browser, or by `makeServer(...).layerInProcess` on the
 * server, and read by every `call`.
 */
export type ServerFunctionInvoker = Readonly<{
  has: (tag: string) => boolean
  invoke: (
    tag: string,
    payload: unknown,
  ) => Effect.Effect<unknown, unknown> | Stream.Stream<unknown, unknown>
}>

/**
 * The service every `call` requires. Provide it through the application's
 * `resources` Layer in the browser and through `layerInProcess` during a
 * server render.
 */
export class ServerFunctionClient extends Context.Service<
  ServerFunctionClient,
  ServerFunctionInvoker
>()('foldkit-serverfn/ServerFunctionClient') {}

// SERVER FUNCTION

/**
 * The result of `call`: an `Effect` for a one-shot server function and a
 * `Stream` for one declared with `stream: true`. The error channel holds the
 * declared error, the errors of any middleware, and `ServerFunctionError`.
 */
export type CallResult<R extends Rpc.Any> =
  Rpc.Success<R> extends Stream.Stream<infer A, infer _E, infer _R>
    ? Stream.Stream<A, Rpc.ErrorExit<R> | ServerFunctionError, ServerFunctionClient>
    : Effect.Effect<
        Rpc.Success<R>,
        Rpc.Error<R> | ServerFunctionError,
        ServerFunctionClient
      >

/**
 * One server function: the `Rpc` it is built on, plus a typed `call` that
 * Commands and Subscriptions use.
 */
export interface ServerFunction<R extends Rpc.Any> {
  readonly tag: Rpc.Tag<R>
  readonly rpc: R
  readonly isStream: boolean
  /**
   * Calls the server function. Returns an `Effect` for a one-shot function and
   * a `Stream` for a streaming one. Interrupting either aborts the request,
   * which interrupts the handler on the server.
   */
  readonly call: (payload: Rpc.PayloadConstructor<R>) => CallResult<R>
  /**
   * Adds an `RpcMiddleware` service. Server middleware can provide services to
   * the handler and add its declared error to the call's error channel.
   */
  readonly middleware: <M extends RpcMiddleware.AnyService>(
    middleware: M,
  ) => ServerFunction<Rpc.AddMiddleware<R, M>>
}

/** Any server function, whatever its Schemas. */
export type Any = ServerFunction<any>

const toServerFunctionError = (
  error: RpcClientError.RpcClientError,
): ServerFunctionError =>
  new ServerFunctionError({ reason: 'Transport', message: error.message })

const defectError = (cause: Cause.Cause<unknown>): ServerFunctionError =>
  new ServerFunctionError({ reason: 'Defect', message: Cause.pretty(cause) })

const notRegisteredError = (tag: string): ServerFunctionError =>
  new ServerFunctionError({
    reason: 'NotRegistered',
    message: `The server function client has no "${tag}". Add it to the group passed to layerClient or makeServer.`,
  })

const isRpcClientError = (
  error: unknown,
): error is RpcClientError.RpcClientError =>
  error instanceof RpcClientError.RpcClientError

const callEffect = (
  tag: string,
  payload: unknown,
): Effect.Effect<unknown, unknown, ServerFunctionClient> =>
  Effect.gen(function* () {
    const client = yield* ServerFunctionClient
    if (!client.has(tag)) {
      return yield* Effect.fail(notRegisteredError(tag))
    }
    return yield* client.invoke(tag, payload) as Effect.Effect<unknown, unknown>
  }).pipe(
    Effect.mapError(error =>
      isRpcClientError(error) ? toServerFunctionError(error) : error,
    ),
    Effect.catchDefect(defect =>
      Effect.fail(defectError(Cause.die(defect))),
    ),
  )

const callStream = (
  tag: string,
  payload: unknown,
): Stream.Stream<unknown, unknown, ServerFunctionClient> =>
  Stream.unwrap(
    Effect.gen(function* () {
      const client = yield* ServerFunctionClient
      if (!client.has(tag)) {
        return Stream.fail(notRegisteredError(tag))
      }
      return client.invoke(tag, payload) as Stream.Stream<unknown, unknown>
    }),
  ).pipe(
    Stream.mapError(error =>
      isRpcClientError(error) ? toServerFunctionError(error) : error,
    ),
    Stream.catchDefect(defect => Stream.fail(defectError(Cause.die(defect)))),
  )

const fromRpc = <R extends Rpc.Any>(rpc: R): ServerFunction<R> => {
  const rpcWithProps = rpc as unknown as Rpc.AnyWithProps
  const tag = rpcWithProps._tag
  const isStream = RpcSchema.isStreamSchema(rpcWithProps.successSchema)

  const call = (payload: unknown): unknown =>
    isStream ? callStream(tag, payload) : callEffect(tag, payload)

  return {
    tag: tag as Rpc.Tag<R>,
    rpc,
    isStream,
    call: call as ServerFunction<R>['call'],
    middleware: middleware =>
      fromRpc((rpc as any).middleware(middleware)),
  }
}

/**
 * Declares a server function. The declaration holds only Schemas, so the
 * module that exports it is safe to import from browser code. The handler is
 * supplied separately on the server through `group(...).toLayer`.
 *
 * @example
 * ```ts
 * export const GetEntries = ServerFunction.define('GetEntries', {
 *   success: Schema.Array(Entry),
 * })
 *
 * export const WatchEntries = ServerFunction.define('WatchEntries', {
 *   success: Entry,
 *   stream: true,
 * })
 * ```
 */
export const define = <
  const Tag extends string,
  Payload extends Schema.Top | Schema.Struct.Fields = typeof Schema.Void,
  Success extends Schema.Top = typeof Schema.Void,
  Error extends Schema.Top = typeof Schema.Never,
  const IsStream extends boolean = false,
>(
  tag: Tag,
  options?: Readonly<{
    payload?: Payload
    success?: Success
    error?: Error
    stream?: IsStream
  }>,
) =>
  fromRpc(
    Rpc.make<Tag, Payload, Success, Error, IsStream>(tag, {
      ...(options?.payload !== undefined ? { payload: options.payload } : {}),
      ...(options?.success !== undefined ? { success: options.success } : {}),
      ...(options?.error !== undefined ? { error: options.error } : {}),
      ...(options?.stream !== undefined ? { stream: options.stream } : {}),
    }),
  )

/** The `Rpc` union of a list of server functions. */
export type RpcsOf<Functions extends ReadonlyArray<Any>> =
  Functions[number] extends ServerFunction<infer R> ? R : never

/**
 * Collects server functions into an `RpcGroup`. The group is what the server
 * implements with `toLayer` and what `layerClient` and `makeServer` serve.
 */
export const group = <const Functions extends ReadonlyArray<Any>>(
  ...functions: Functions
): RpcGroup.RpcGroup<RpcsOf<Functions>> =>
  RpcGroup.make(...functions.map(({ rpc }) => rpc)) as RpcGroup.RpcGroup<
    RpcsOf<Functions>
  >
