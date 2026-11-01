import { fileURLToPath } from 'node:url'

import { foldkit } from '@foldkit/vite-plugin'
import { defineConfig } from 'vite'

import { serverFunctions } from 'foldkit-serverfn/vite'

const SERVER_ENTRY = '/src/entry.server.ts'

const librarySource = (path: string): string =>
  fileURLToPath(new URL(`../../packages/serverfn/src/${path}`, import.meta.url))

export default defineConfig({
  plugins: [
    serverFunctions({ serverEntry: SERVER_ENTRY }),
    foldkit({
      ssr: {
        serverEntry: SERVER_ENTRY,
        clientEntry: '/src/entry.ts',
        build: true,
      },
    }),
  ],
  resolve: {
    alias: [
      { find: /^foldkit-serverfn$/, replacement: librarySource('index.ts') },
      { find: /^foldkit-serverfn\/(.*)$/, replacement: librarySource('$1.ts') },
    ],
  },
})
