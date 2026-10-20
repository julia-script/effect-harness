# effect-harness

## 0.7.0

### Minor Changes

- 447110d: Reorganize `effect-harness` into a single package with concept-oriented public subpaths and separate client/runtime APIs.

  - Replace the previous uppercase and `provider-*` imports with lowercase subpaths such as `effect-harness/client`, `effect-harness/harness`, `effect-harness/model`, and `effect-harness/tool`. The root export groups the portable core namespaces.
  - Expose `HarnessServer` and `HarnessClient` for composing the runtime and accessing it through memory or WebSocket RPC transports.
  - Reserve `$`-prefixed identifiers for built-in entry, document, and task kinds and tool-output discriminants. Custom input-entry kinds must not start with `$`.
  - Make provider and platform integrations optional peer dependencies, and include source files alongside compiled ESM and declarations so published source maps resolve.

  This is a breaking pre-1.0 release. Applications must update imports and composition to the new APIs.
