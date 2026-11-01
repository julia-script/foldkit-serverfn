import { Effect } from 'effect'
import { Server } from 'foldkit/experimental'
import { makeServer } from 'foldkit-serverfn/server'

import { GuestbookFunctionsLive } from './guestbook.server'
import { Flags, init, view } from './main'
import { GetEntries, GuestbookFunctions } from './serverFunction'

const server = makeServer(GuestbookFunctions, { layer: GuestbookFunctionsLive })

export const renderDocument = Server.renderDocument

export const handleServerFunction = server.handle

const loadFlags: Effect.Effect<Flags> = GetEntries.call().pipe(
  Effect.map(entries => Flags.make({ entries })),
  Effect.catch(() => Effect.succeed(Flags.make({ entries: [] }))),
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
