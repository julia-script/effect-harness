import assert from 'node:assert/strict'

const privatePaths = [
  'storage/internal/state',
  'storage/internal/backend',
  'storage/internal/nested/deeper/state',
  'workflow/internal/nested/executor',
  'testing/internal/nested/fixture',
  'internal/nested/helper',
]
const denied = []
for (const path of privatePaths) {
  try {
    await import(`effect-harness/durable/${path}`)
    assert.fail(`Private import ${path} must be denied`)
  } catch (error) {
    assert.equal(
      error.code,
      'ERR_PACKAGE_PATH_NOT_EXPORTED',
      `Wrong denial for ${path}: ${String(error)}`,
    )
    denied.push({ path, code: error.code })
  }
}
const store = await import('effect-harness/durable/Store')
const document = await import('effect-harness/durable/Document')
const harness = await import('effect-harness')
const durable = await import('effect-harness/durable')
const auth = await import('effect-harness/auth')
const openai = await import('effect-harness/provider-openai')
const anthropic = await import('effect-harness/provider-anthropic')
const claude = await import('effect-harness/provider-claude-code')
const model = await import('effect-harness/Model')
const session = await import('effect-harness/durable/Session')
assert.equal(harness.Model.Catalog, model.Catalog)
assert.equal(durable.Store.Store, store.Store)
assert.equal(durable.Session.Session, session.Session)
assert.equal(typeof auth.CredentialStore.layerMemory, 'object')
assert.equal(typeof openai.Catalog.layerApiKey, 'function')
assert.equal(typeof anthropic.Catalog.layerApiKey, 'function')
assert.equal(typeof claude.Cli.layer, 'function')
assert.equal(typeof store.Store, 'function')
assert.equal(typeof store.makeCandidate, 'function')
assert.equal(typeof document.CloneError, 'function')
console.log(
  JSON.stringify({
    denied,
    publicStoreImported: true,
    publicCloneErrorImported: true,
  }),
)
