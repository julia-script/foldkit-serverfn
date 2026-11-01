import { Effect, Layer } from 'effect'
import { RpcClient, RpcSerialization } from 'effect/rpc'
import type { Rpc, RpcGroup } from 'effect/rpc'
import { Http } from 'foldkit'

import { DEFAULT_PATH } from './path.js'
import { ServerFunctionClient } from './serverFunction.js'

/** Options for {@link layerClient}. */
export type LayerClientOptions = Readonly<{
  /**
   * Where server function requests go. A path is resolved against the page's
   * origin. Defaults to `/_serverfn`.
   */
  url?: string
}>

const resolveUrl = (url: string): string =>
  new URL(url, globalThis.location?.href ?? 'http://localhost').href

/**
 * The browser side of a group: an `RpcClient` over HTTP with NDJSON
 * serialization, exposed as the `ServerFunctionClient` service. Pass it to the
 * application's `resources` so it is built once and shared by every Command
 * and Subscription.
 *
 * @example
 * ```ts
 * const application = Runtime.makeApplication({
 *   // ...
 *   resources: layerClient(GuestbookFunctions),
 * })
 * ```
 */
export const layerClient = <Rpcs extends Rpc.Any>(
  group: RpcGroup.RpcGroup<Rpcs>,
  options?: LayerClientOptions,
): Layer.Layer<ServerFunctionClient> =>
  Layer.effect(ServerFunctionClient)(
    Effect.gen(function* () {
      const client = yield* RpcClient.make(group, { flatten: true })
      const tags = new Set<string>(group.requests.keys())
      return ServerFunctionClient.of({
        has: tag => tags.has(tag),
        invoke: (tag, payload) => (client as any)(tag, payload),
      })
    }),
  ).pipe(
    Layer.provide(
      Layer.unwrap(
        Effect.sync(() =>
          RpcClient.layerProtocolHttp({
            url: resolveUrl(options?.url ?? DEFAULT_PATH),
          }),
        ),
      ),
    ),
    Layer.provide([Http.layer, RpcSerialization.layerNdjson]),
  )
