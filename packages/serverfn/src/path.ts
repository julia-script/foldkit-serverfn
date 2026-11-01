/** The path server function requests use unless configured otherwise. */
export const DEFAULT_PATH = '/_serverfn'

const TRAILING_SLASHES = /\/+$/

/**
 * Whether a request pathname names the server function path. The RPC HTTP
 * client appends a trailing slash to the configured URL, so one is ignored.
 */
export const matchesPath = (pathname: string, path: string): boolean =>
  pathname.replace(TRAILING_SLASHES, '') === path.replace(TRAILING_SLASHES, '')
