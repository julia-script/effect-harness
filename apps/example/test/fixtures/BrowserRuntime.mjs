// Run a fully bundled ES module with web globals and no Node/Bun/Deno capabilities.
import { readFile } from 'node:fs/promises'
import * as vm from 'node:vm'

const source = await readFile(process.argv[2], 'utf8')
// Eagerly initialize Node's lazy web globals before removing its host capabilities.
for (const key of ['Request', 'Response', 'Headers', 'fetch', 'crypto'])
  Reflect.get(globalThis, key)
// Keep native structuredClone and language builtins in the same realm, as in a browser.
// Copying the host's clone function into a separate VM realm would return foreign objects.
for (const key of ['process', 'Buffer', 'Bun', 'Deno', 'global', 'setImmediate', 'clearImmediate'])
  Reflect.deleteProperty(globalThis, key)
const module = new vm.SourceTextModule(source)
await module.link((specifier) => {
  throw new Error(`Browser bundle has an unresolved import: ${specifier}`)
})
await module.evaluate()
