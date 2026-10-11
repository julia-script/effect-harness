import assert from 'node:assert/strict'

for (const [module, name] of [
  ['FileSystem', 'fileSystemConformance'],
  ['Process', 'processConformance'],
  ['Images', 'imagesConformance'],
  ['ToolkitIsolation', 'toolkitIsolationConformance'],
]) {
  const imported = await import(`effect-harness/testing/${module}`)
  assert.equal(typeof imported[name], 'function')
}
console.log('All four portable testing routes imported without native or codec dependencies')
