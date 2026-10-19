import { pathToFileURL } from 'node:url'

if (typeof globalThis.Bun !== 'undefined' || !process.versions.node)
  throw new Error('The Node portability check must execute in Node, not a Bun node shim')
await import(pathToFileURL(process.argv[2]).href)
