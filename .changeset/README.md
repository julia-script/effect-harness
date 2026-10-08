# Releases

Run `bun run changeset` for a public API change, bug fix or other package change, and commit the generated Markdown file with the implementation. Select `effect-harness` and its appropriate semver bump. The workspace root and example application remain private.

Successful CI on `main` triggers `.github/workflows/release.yml`. Pending changesets create or update the **Version Packages** PR. Its version command updates package versions, changelogs and `bun.lock`, then formats the result. Merge that PR when the release is ready. CI checks the merged commit before the release workflow builds and publishes it through npm OIDC; GitHub releases and git tags follow successful publication.

The npm trusted publisher for `effect-harness` uses owner `julia-script`, repository `effect-harness`, workflow filename `release.yml` and environment `npm-production`. Allow direct `npm publish`. The GitHub environment permits `main` and has no required reviewers. No npm token secret is needed.

`bun run version:packages` applies version changes locally. `bun run release` builds and publishes unpublished package versions; use it only when intentionally publishing with suitable npm authentication. Private workspaces are never published.
