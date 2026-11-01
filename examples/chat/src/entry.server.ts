import { Effect } from 'effect'
import { Server } from 'foldkit/experimental'
import { makeServer } from 'foldkit-serverfn/server'

import { ChatFunctionsLive } from './chat.server'
import { Flags, init, view } from './main'
import { ChatFunctions, GetConversation } from './serverFunction'

const server = makeServer(ChatFunctions, { layer: ChatFunctionsLive })

export const renderDocument = Server.renderDocument

export const handleServerFunction = server.handle

const loadFlags: Effect.Effect<Flags> = GetConversation.call().pipe(
  Effect.map(messages => Flags.make({ messages })),
  Effect.catch(() => Effect.succeed(Flags.make({ messages: [] }))),
  Effect.provide(server.layerInProcess),
)

const renderApplication = (): Promise<Server.EntryResult> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const flags = yield* loadFlags
      const renderedApplication = yield* Server.renderToString(
        { Flags, init, view },
        { flags },
      )
      return Server.Rendered(renderedApplication, {
        headers: { 'cache-control': 'no-store' },
      })
    }),
  )

export const renderPage = (request: Request): Promise<Server.EntryResult> =>
  server.route(request, renderApplication)
