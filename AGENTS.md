# @effect-harness

Use `bun`. `bun run check` (format, lint, typecheck) must pass before any commit; `bun run check:write` auto-fixes. Lint warnings and nested ternaries are errors.

## Layout

Each folder under `src/` is a module with a package subpath (`src/harness` is `effect-harness/harness`):

- `index.ts` exports the module's public files, one PascalCase namespace each, and only files from its own folder.
- `internal/` holds the module's private code. Other modules never import it; helpers more than one module needs go in `src/internal/`.
- `test/` holds the module's tests. Fixtures more than one module's tests use go in `src/test/`.
- Code tied to an optional platform or peer lives under its own subpath folder (`coding/bun`, `credentials/bun`), with its private code in that folder's `internal/`.
- `src/protocol` is the contract the client and the loop share; `src/client` and `src/harness` both import it and never each other.

## Vendored Repositories

This project vendors external repositories under @repos/

- Use vendored repositories as read-only reference material when working with related libraries
- Prefer examples and patterns from the vendored source code over generated guesses or web search results
- Do not edit files under @repos/ unless explicitly asked
- Do not import from @repos/ - application code should continue importing from normal package dependencies

When writing Effect code, inspect @repos/effect/ for examples of idiomatic usage, tests, module structure, and API design.
Update the vendored copy with `bun ~/.agents/skills/new-project/scripts/scaffold.ts subtree --dir .`
