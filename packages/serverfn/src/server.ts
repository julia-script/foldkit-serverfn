import { Effect, Layer, ManagedRuntime } from 'effect'
import { HttpRouter } from 'effect/http'
import { RpcSerialization, RpcServer, RpcTest } from 'effect/rpc'
import type { Rpc, RpcGroup } from 'effect/rpc'
import { Server } from 'foldkit/experimental'

import { DEFAULT_PATH, matchesPath } from './path.js'
import { ServerFunctionClient } from './serverFunction.js'

/** The services a group's handlers and middleware need on the server. */
export type HandlerServices<Rpcs extends Rpc.Any> =
  | Rpc.ToHandler<Rpcs>
  | Rpc.Middleware<Rpcs>
  | Rpc.ServicesServer<Rpcs>

/** Options for {@link makeServer}. */
export type MakeServerOptions<Rpcs extends Rpc.Any, LayerError> = Readonly<{
  /**
   * Implements every server function in the group, usually
   * `group.toLayer({ ... })` with its dependencies provided. It is built once
   * and shared by HTTP requests and in-process calls.
   */
  layer: Layer.Layer<HandlerServices<Rpcs>, LayerError>
  /** The path server function requests arrive on. Defaults to `/_serverfn`. */
  path?: string
  /**
   * Refuses requests whose `Sec-Fetch-Site` or `Origin` header names another
   * site. Defaults to `true`.
   */
  isSameOriginRequired?: boolean
}>

/** The server side of a group, created once per server process. */
export type ServerFunctionServer = Readonly<{
  /** The path this server answers. */
  path: string
  /** Whether a request is a server function call this server should answer. */
  isServerFunctionRequest: (request: Request) => boolean
  /** Answers one server function request. */
  handle: (request: Request) => Promise<Response>
  /**
   * Answers server function requests and passes every other request to
   * `renderPage`. Use it as the body of a Foldkit server entry's `renderPage`.
   */
  route: (
    request: Request,
    renderPage: (request: Request) => Promise<Server.EntryResult>,
  ) => Promise<Server.EntryResult>
  /**
   * Provides `ServerFunctionClient` by calling handlers directly, with no HTTP
   * or serialization. Use it in a server render to load data for Flags.
   */
  layerInProcess: Layer.Layer<ServerFunctionClient>
  /** Releases the services the handlers hold. */
  dispose: () => Promise<void>
}>

const CROSS_SITE_STATUS = 403
const SAME_SITE_FETCH_VALUES = new Set(['same-origin', 'none'])

const isSameOrigin = (request: Request): boolean => {
  const fetchSite = request.headers.get('sec-fetch-site')
  if (fetchSite !== null) {
    return SAME_SITE_FETCH_VALUES.has(fetchSite)
  }

  const origin = request.headers.get('origin')
  if (origin !== null) {
    return origin === new URL(request.url).origin
  }

  return true
}

/**
 * Serves a group of server functions. Returns a `fetch`-style `handle`, a
 * `route` helper for a Foldkit server entry, and `layerInProcess` for server
 * renders. Create it once at module scope in the server entry.
 *
 * @example
 * ```ts
 * const server = makeServer(GuestbookFunctions, { layer: GuestbookFunctionsLive })
 *
 * export const handleServerFunction = server.handle
 *
 * export const renderPage = (request: Request) =>
 *   server.route(request, renderApplication)
 * ```
 */
export const makeServer = <Rpcs extends Rpc.Any, LayerError>(
  group: RpcGroup.RpcGroup<Rpcs>,
  options: MakeServerOptions<Rpcs, LayerError>,
): ServerFunctionServer => {
  const path = options.path ?? DEFAULT_PATH
  const isSameOriginRequired = options.isSameOriginRequired ?? true
  const memoMap = Layer.makeMemoMapUnsafe()

  const httpLayer = RpcServer.layerHttp({
    group,
    path: path as HttpRouter.PathInput,
    protocol: 'http',
  }).pipe(
    Layer.provide(options.layer),
    Layer.provide(RpcSerialization.layerNdjson),
  )

  const webHandler = HttpRouter.toWebHandler(httpLayer, {
    memoMap,
    disableLogger: true,
  }) as Readonly<{
    handler: (request: Request) => Promise<Response>
    dispose: () => Promise<void>
  }>

  const runtime = ManagedRuntime.make(options.layer, { memoMap })

  const tags = new Set<string>(group.requests.keys())

  const layerInProcess = Layer.effect(ServerFunctionClient)(
    Effect.gen(function* () {
      const context = yield* runtime.contextEffect
      const client = yield* RpcTest.makeClient(group, { flatten: true }).pipe(
        Effect.provideContext(context),
      )
      return ServerFunctionClient.of({
        has: tag => tags.has(tag),
        invoke: (tag, payload) => (client as any)(tag, payload),
      })
    }).pipe(Effect.orDie),
  )

  const isServerFunctionRequest = (request: Request): boolean =>
    request.method.toUpperCase() === 'POST' &&
    matchesPath(new URL(request.url).pathname, path)

  const handle = (request: Request): Promise<Response> => {
    if (isSameOriginRequired && !isSameOrigin(request)) {
      return Promise.resolve(new Response(null, { status: CROSS_SITE_STATUS }))
    }
    return webHandler.handler(request)
  }

  const route = async (
    request: Request,
    renderPage: (request: Request) => Promise<Server.EntryResult>,
  ): Promise<Server.EntryResult> => {
    if (isServerFunctionRequest(request)) {
      return Server.Responded(await handle(request))
    }
    return renderPage(request)
  }

  const dispose = async (): Promise<void> => {
    await webHandler.dispose()
    await runtime.dispose()
  }

  return {
    path,
    isServerFunctionRequest,
    handle,
    route,
    layerInProcess,
    dispose,
  }
}
