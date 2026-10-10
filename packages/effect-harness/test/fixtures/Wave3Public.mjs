import assert from 'node:assert/strict'
import { registerHooks } from 'node:module'

// Run unchanged in a fresh tarball consumer. Optional platform codecs must stay lazy.
registerHooks({
  resolve(specifier, context, next) {
    if (
      specifier === 'sharp' ||
      specifier === 'bun' ||
      specifier === 'jpeg-js' ||
      specifier.startsWith('@cf-wasm/')
    )
      throw new Error(`Portable public import eagerly loaded ${specifier}`)
    return next(specifier, context)
  },
})

const root = await import('effect-harness')
const coding = await import('effect-harness/coding')
assert.equal(root.Coding, coding)
for (const name of [
  'DocumentRetirement',
  'OutputRetention',
  'StorageFormat',
  'TaskScheduler',
  'ToolSpill',
])
  assert.equal(root[name], await import(`effect-harness/${name}`))
for (const name of [
  'Bash',
  'Edit',
  'ImageRead',
  'ImageReadPolicy',
  'PowerShell',
  'Read',
  'ShellProcess',
])
  assert.equal(coding[name], await import(`effect-harness/coding/${name}`))
assert.equal(coding.Read.tool.name, 'read')
assert.equal(coding.Edit.tool.name, 'edit')
assert.equal(coding.Bash.tool.name, 'bash')
assert.equal(coding.PowerShell.tool.name, 'powershell')
for (const name of [
  'internal/EditText',
  'internal/EditDiff',
  'internal/Shell',
  'internal/TaskScheduler',
])
  await assert.rejects(import(`effect-harness/${name}`), { code: 'ERR_PACKAGE_PATH_NOT_EXPORTED' })
console.log('Packed public namespaces preserve identity, deny private helpers and load no codecs')
