# Releasing

MarciDB ships two flavours of the server:

| Flavour     | Cargo features      | Toolchain | Includes                       |
| ----------- | ------------------- | --------- | ------------------------------ |
| **full**    | `vector fulltext`   | nightly   | vector + full-text index modules |
| **core**    | _(none)_            | stable    | core engine only (smaller)     |

Each flavour is published as a Windows `.exe`, a Linux binary, and a Docker image.

## Cutting a release

From a clean working tree on the release branch:

```bash
./scripts/release.sh 0.2.3
```

This will:

1. set `marcidb-server`'s package version to `0.2.3`,
2. regenerate `CHANGELOG.md` from commits since the last tag,
3. build & push both Docker flavours to Docker Hub (`den59k/marcidb-server`),
4. commit, create the `v0.2.3` tag and push it.

Pushing the tag triggers [`.github/workflows/release.yml`](../.github/workflows/release.yml),
which builds the binaries, mirrors the Docker images to GHCR, and publishes the
GitHub Release with the same changelog notes.

Useful flags: `--no-docker` (let CI build the images), `--no-push` (stage
locally), `--dry-run` (preview, change nothing).

## What CI produces (on every `v*` tag)

- **Binaries** attached to the GitHub Release:
  - `marcidb-server-vX.Y.Z-x86_64-windows-full.exe` / `-core.exe`
  - `marcidb-server-vX.Y.Z-x86_64-linux-full` / `-core`
- **Docker images** on GHCR (`ghcr.io/<owner>/marcidb-server`):
  - `:X.Y.Z-full`, `:full`, `:latest`
  - `:X.Y.Z-core`, `:core`
- **GitHub Release** with the auto-generated changelog.

Docker Hub images (`den59k/marcidb-server`, referenced in the README) are pushed
by `release.sh` from your machine; GHCR is the automatic CI mirror. To push
Docker Hub from CI instead, add `DOCKERHUB_USERNAME` / `DOCKERHUB_TOKEN` secrets
and extend the `docker` job's login + tag list.

## Changelog format

Notes are grouped from [conventional commit](https://www.conventionalcommits.org)
prefixes (`feat:`, `fix:`, `refactor:`, …) by
[`scripts/gen-changelog.sh`](../scripts/gen-changelog.sh). Anything without a
recognised prefix lands under **Other Changes**. You can preview the next
release's notes at any time:

```bash
./scripts/gen-changelog.sh
```

### Keeping noise out

Two ways to keep a commit out of the changelog:

- **Hidden categories.** Internal / tooling types are dropped by default —
  `chore`, `style`, `ci`, `build` (this also hides the auto `chore: release`
  commits). Override per-run with `CHANGELOG_HIDE`:

  ```bash
  CHANGELOG_HIDE="chore style"   ./scripts/gen-changelog.sh   # hide fewer
  CHANGELOG_HIDE=""              ./scripts/gen-changelog.sh   # show everything
  ```

- **Sign a single commit to skip it**, regardless of type — put `[skip changelog]`
  anywhere in the commit message, or add a trailer:

  ```
  fix: tidy up an internal helper

  Changelog: skip
  ```
