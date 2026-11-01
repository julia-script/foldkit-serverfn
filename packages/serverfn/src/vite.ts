import type { IncomingMessage, ServerResponse } from 'node:http'
import { Readable } from 'node:stream'
import type { ReadableStream as NodeReadableStream } from 'node:stream/web'

import type { Plugin, ViteDevServer } from 'vite'

import { DEFAULT_PATH, matchesPath } from './path.js'

/** Options for {@link serverFunctions}. */
export type ServerFunctionsPluginOptions = Readonly<{
  /** The server entry module, as passed to Foldkit's `ssr.serverEntry`. */
  serverEntry: string
  /**
   * The server entry export that answers one request. Defaults to
   * `handleServerFunction`.
   */
  exportName?: string
  /** The path server function requests use. Defaults to `/_serverfn`. */
  path?: string
}>

type RequestHandler = (request: Request) => Promise<Response>

const toWebRequest = (nodeRequest: IncomingMessage): Request => {
  const host = nodeRequest.headers.host ?? 'localhost'
  const url = new URL(nodeRequest.url ?? '/', `http://${host}`)
  const headers = new Headers()
  for (const [name, value] of Object.entries(nodeRequest.headers)) {
    if (value === undefined) {
      continue
    }
    if (Array.isArray(value)) {
      value.forEach(item => headers.append(name, item))
    } else {
      headers.set(name, value)
    }
  }

  const init: RequestInit & { duplex: 'half' } = {
    method: nodeRequest.method ?? 'POST',
    headers,
    body: Readable.toWeb(nodeRequest) as ReadableStream<Uint8Array>,
    duplex: 'half',
  }
  return new Request(url, init)
}

const sendWebResponse = async (
  response: Response,
  nodeResponse: ServerResponse,
): Promise<void> => {
  nodeResponse.statusCode = response.status
  response.headers.forEach((value, name) => nodeResponse.setHeader(name, value))

  if (response.body === null) {
    nodeResponse.end()
    return
  }

  nodeResponse.flushHeaders()
  const body = Readable.fromWeb(
    response.body as unknown as NodeReadableStream<Uint8Array>,
  )
  nodeResponse.on('close', () => body.destroy())
  body.pipe(nodeResponse)
}

const loadHandler = async (
  server: ViteDevServer,
  options: ServerFunctionsPluginOptions,
): Promise<RequestHandler> => {
  const exportName = options.exportName ?? 'handleServerFunction'
  const module = await server.ssrLoadModule(options.serverEntry)
  const handler: unknown = module[exportName]
  if (typeof handler !== 'function') {
    throw new Error(
      `[foldkit-serverfn] ${options.serverEntry} must export a function named ${exportName}.`,
    )
  }
  return handler as RequestHandler
}

/**
 * Answers server function requests from the Vite dev server, streaming each
 * response as the handler writes it. List it before `foldkit()` in the Vite
 * config. Production needs no plugin: the server entry's `renderPage` routes
 * these requests through `makeServer(...).route`.
 */
export const serverFunctions = (
  options: ServerFunctionsPluginOptions,
): Plugin => {
  const path = options.path ?? DEFAULT_PATH

  return {
    name: 'foldkit-serverfn',
    apply: 'serve',
    configureServer: server => {
      server.middlewares.use((nodeRequest, nodeResponse, next) => {
        const pathname = new URL(nodeRequest.url ?? '/', 'http://localhost')
          .pathname
        if (nodeRequest.method !== 'POST' || !matchesPath(pathname, path)) {
          next()
          return
        }

        loadHandler(server, options)
          .then(handle => handle(toWebRequest(nodeRequest)))
          .then(response => sendWebResponse(response, nodeResponse))
          .catch((error: unknown) => {
            if (error instanceof Error) {
              server.ssrFixStacktrace(error)
            }
            next(error)
          })
      })
    },
  }
}
