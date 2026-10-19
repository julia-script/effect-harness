# 📦 Releasing

This page is for maintainers.

Releases use Changesets and npm trusted publishing, with Node 24 and Bun 1.4.2. The package stays at the published base version until the automated version PR is merged; the pending minor changeset prepares the breaking pre-1.0 release from `0.5.0` to `0.6.0`.

1. Include a changeset in each PR with `bun run changeset`; use `bun run changeset --empty` for changes that intentionally do not release the package. `bun run check:changesets` checks additions against `origin/main`. CI requires this except for the same-repository `changeset-release/main` version branch, whose changesets have already been consumed.
2. Merge the change into protected `main`. After its push-triggered `CI` succeeds, `.github/workflows/release.yml` runs `changesets/action` to create or update the `changeset-release/main` version PR. Its `bun run version:packages` command updates the version and changelog, refreshes the Bun lockfile without install scripts, and formats the result.
3. Review the version PR before merging. PRs created or updated with `GITHUB_TOKEN` do not automatically trigger other workflows; a maintainer can close and reopen the PR to trigger PR CI on its latest revision. CI builds, checks, tests, and runs `bun run test:package` against a real installed tarball.
4. Merge the version PR. After the resulting main commit passes `CI`, the release workflow runs `bun run release` (`changeset publish`). The package's `prepack` script builds the distribution before npm packs it. Publication uses OIDC, requests provenance, and creates the release tag and GitHub release; no long-lived npm token is needed.

Release jobs accept only successful, same-repository `push` runs of `CI` on `main`. The tested SHA must also equal the release event's `github.sha`, because Changesets uses that SHA when resetting its version branch. Immediately before running Changesets, the workflow checks that remote `main` still matches and skips superseded runs. The checkout and Changesets base remain pinned to the tested commit, so a later branch update cannot silently substitute untested code. Release jobs are serialized.

In the npm settings for `effect-harness`, keep the GitHub Actions trusted publisher configured for repository **`julia-script/effect-harness`**, workflow filename **`release.yml`**, and environment **`npm-production`**. GitHub must have that environment configured, allow Actions to create pull requests, and protect `main` with the CI checks. These repository/npm settings are external prerequisites, not settings this checkout can provision.

This publishing-readiness change has not published anything to npm. `bun run release` performs a real publication and is reserved for the approved release workflow.
