# Releasing

One version covers the whole toolchain (**lockstep** — "same version == compatible"): the Cargo workspace
(engine, server, codegen, native lib, index modules) and both npm packages (`marcidb-client`,
`marcidb-embedded`) always release together at the same number.

The server ships in two flavours:

| Flavour  | Cargo features    | Toolchain | Includes                         |
| -------- | ----------------- | --------- | -------------------------------- |
| **full** | `vector fulltext` | nightly   | vector + full-text index modules |
| **core** | _(none)_          | stable    | core engine only (smaller)       |

## Cutting a release

From a clean working tree on `main`:

```bash
./scripts/release.sh 0.6.2            # stage locally (commit + tag, no push)
./scripts/release.sh 0.6.2 --push     # …and push, which triggers the release CI
```

`scripts/release.sh` only stamps and tags — all building, testing and publishing happens in CI. It:

1. stamps `0.6.2` into `[workspace.package].version` (every crate inherits it via `version.workspace = true`)
   and into both `package.json`s (and `marcidb-embedded`'s `marcidb-client` peerDep),
2. syncs `Cargo.lock` (`cargo update --workspace` — re-locks only the workspace crates, external deps stay
   pinned),
3. runs a `cargo check --workspace` pre-flight (skip with `--no-verify`),
4. commits `release: v0.6.2`, tags `v0.6.2`, and — with `--push` — pushes the commit + tag.

Pushing the `v*` tag runs [`.github/workflows/release.yml`](../.github/workflows/release.yml).

## What CI produces (on every `v*` tag)

- **Server binaries** → GitHub Release: `marcidb-server-vX.Y.Z-x86_64-{windows,linux}-{full,core}`.
- **Embedded native libs** (`marcidb-ffi`, full) for `{windows, linux, mac}` → bundled into `marcidb-embedded`.
- **Codegen binaries** (`marci-generate`, `marci-migrate`) for `{windows, linux, mac}` → bundled into
  `marcidb-client/bin`.
- **npm packages** — `marcidb-client` then `marcidb-embedded`, with every platform's binaries/libs assembled
  from the build artifacts (the `publish-npm` job). One tag ships all platforms, mac included.
- **Docker images** on GHCR (`ghcr.io/<owner>/marcidb-server`): `:X.Y.Z-full`, `:full`, `:latest`,
  `:X.Y.Z-core`, `:core`.
- **GitHub Release** with the auto-generated changelog.

The Rust build jobs use `--locked`, so a release fails loudly if `Cargo.lock` is out of sync with the
manifests rather than silently re-resolving.

## Prerequisites (one-time)

- **npm trusted publishing** — no token or secret. On npmjs.com, for **each** of `marcidb-client` and
  `marcidb-embedded`: *Settings* → *Trusted Publisher* → *GitHub Actions*, organization/user `den59k`,
  repository `marci-db`, workflow filename `release.yml` (no environment). The `publish-npm` job gets a
  short-lived OIDC token (`id-token: write`) and npm exchanges it for publish rights; provenance is attached
  automatically. Renaming or moving `release.yml` breaks publishing until these settings are updated.
  Once it works, you can set *Publishing access* to "Require two-factor authentication and disallow tokens".
- **GHCR package visibility** — container packages are **private by default**. To allow anonymous
  `docker pull`, set `marcidb-server` to **Public** once (GitHub → your packages → `marcidb-server` →
  *Package settings* → *Change visibility*). Pushing uses the built-in `GITHUB_TOKEN`; no extra secret.

If the npm publish fails, fix the cause and **Re-run failed jobs** — the tag already points at the right
commit, and a package version that is already on the registry is skipped rather than failing the run. Note
that a re-run uses the workflow file *as of the tag*; if the fix was to `release.yml` itself, run the
workflow manually instead (*Actions* → *Release* → *Run workflow* on `main`, with the existing tag).

## Changelog format

Notes are grouped from [conventional commit](https://www.conventionalcommits.org) prefixes
(`feat:`, `fix:`, `refactor:`, …) by [`scripts/gen-changelog.sh`](../scripts/gen-changelog.sh), which CI runs
for the GitHub Release body. Anything without a recognised prefix lands under **Other Changes**. Preview the
next release's notes anytime:

```bash
./scripts/gen-changelog.sh
```

### Keeping noise out

Two ways to keep a commit out of the changelog:

- **Hidden categories.** Internal / tooling types are dropped by default — `chore`, `style`, `ci`, `build`
  (this also hides the auto `release:` commits). Override per-run with `CHANGELOG_HIDE`:

  ```bash
  CHANGELOG_HIDE="chore style"   ./scripts/gen-changelog.sh   # hide fewer
  CHANGELOG_HIDE=""              ./scripts/gen-changelog.sh   # show everything
  ```

- **Sign a single commit to skip it**, regardless of type — put `[skip changelog]` anywhere in the commit
  message, or add a trailer:

  ```
  fix: tidy up an internal helper

  Changelog: skip
  ```
