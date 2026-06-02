#!/usr/bin/env bash
# Cut a hestia release: version bump PR -> tag -> release workflow ->
# dogfood repository variables.
#
# Usage: bin/create-release.sh 0.1.0-alpha.3
#
# The tag push triggers .github/workflows/release.yml, which builds the
# static binaries and creates the GitHub release. This script then points
# the HESTIA_DOGFOOD_* repository variables at the new release so CI
# dogfoods it.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
cd "$SCRIPT_DIR/.."

version="${1:-}"
if [[ -z $version || $version == v* ]]; then
  echo "USAGE: $0 <version without leading v, e.g. 0.1.0-alpha.3>" >&2
  exit 1
fi
tag="v${version}"

if [[ "$(git symbolic-ref --short HEAD)" != "main" ]]; then
  echo "must be on the main branch" >&2
  exit 1
fi

uncommitted_changes=$(git diff --compact-summary)
if [[ -n $uncommitted_changes ]]; then
  echo -e "there are uncommitted changes, exiting:\n${uncommitted_changes}" >&2
  exit 1
fi

git pull --ff-only origin main

unpushed_commits=$(git log --format=oneline origin/main..main)
if [[ -n $unpushed_commits ]]; then
  echo -e "there are unpushed commits, exiting:\n${unpushed_commits}" >&2
  exit 1
fi

if git tag -l | grep -q "^${tag}\$"; then
  echo "tag ${tag} already exists, exiting" >&2
  exit 1
fi

# Bump the version. Cargo.toml is the single source of truth (the nix
# packages read it via importTOML); cargo syncs Cargo.lock.
sed -i -e "0,/^version = \".*\"\$/s//version = \"${version}\"/" Cargo.toml
nix develop --command cargo update --package hestia
# Release examples in the documentation reference the latest tag.
sed -i -e "s/version: v[0-9][^\"' ]*/version: ${tag}/" README.md action/README.md

branch="release-${version}"
git branch -D "$branch" 2>/dev/null || true
git checkout -b "$branch"
git add Cargo.toml Cargo.lock README.md action/README.md
git commit -m "release: bump version to ${version}"
git push --force-with-lease origin "$branch"

pr_url=$(gh pr create \
  --base main \
  --head "$branch" \
  --title "release: bump version to ${version}" \
  --body "Version bump for ${tag}. Tag will be pushed once this lands.")
gh pr merge "$pr_url" --auto --rebase --delete-branch
git checkout main

echo "waiting for ${pr_url} to be merged..."
while [[ "$(gh pr view "$pr_url" --json state --jq .state)" != "MERGED" ]]; do
  sleep 10
done

git pull --ff-only origin main
git tag "$tag"
git push origin "$tag"

echo "waiting for the release workflow..."
# The run may take a moment to appear after the tag push.
run_id=""
for _ in $(seq 30); do
  run_id=$(gh run list --workflow Release --branch "$tag" --limit 1 \
    --json databaseId --jq '.[0].databaseId // empty')
  [[ -n $run_id ]] && break
  sleep 10
done
if [[ -z $run_id ]]; then
  echo "release workflow did not start; check the Actions tab" >&2
  exit 1
fi
gh run watch "$run_id" --exit-status

# Point CI dogfooding at the new release. Downloads are verified via build
# attestations, so only the version variable is needed.
gh variable set HESTIA_DOGFOOD_VERSION --body "$tag"

echo
echo "released ${tag}:"
gh release view "$tag" --json url --jq .url
echo "dogfood variables updated:"
echo "  HESTIA_DOGFOOD_VERSION=${tag}"
