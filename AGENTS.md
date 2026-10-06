# @effect-harness

Use `bun`. `bun run check` (format, lint, typecheck) must pass before any commit; `bun run check:write` auto-fixes. Lint warnings and nested ternaries are errors.

## Vendored Repositories

This project vendors external repositories under @repos/

- Use vendored repositories as read-only reference material when working with related libraries
- Prefer examples and patterns from the vendored source code over generated guesses or web search results
- Do not edit files under @repos/ unless explicitly asked
- Do not import from @repos/ - application code should continue importing from normal package dependencies

When writing Effect code, inspect @repos/effect/ for examples of idiomatic usage, tests, module structure, and API design.
Update the vendored copy with `bun ~/.agents/skills/new-project/scripts/scaffold.ts subtree --dir .`
