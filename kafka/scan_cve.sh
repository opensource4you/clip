#!/usr/bin/env bash
# Build a Kafka release tarball from any git ref and scan its libs/ with grype.
#
# usage:
#   scan_cve.sh <ref>        ref = tag | branch | commit
#   scan_cve.sh 4.2.1
#   scan_cve.sh 4.2          # auto-resolves to origin/4.2
#   scan_cve.sh 8c0bc0ca
#
# outputs: one directory per scan under $OUT
#   <ref>-<sha>/grype.json     full grype report (CVE ids preferred)
#   <ref>-<sha>/ids.txt        sorted unique vulnerability IDs, for comm/diff
#   <ref>-<sha>/table.txt      human-readable table
#   <ref>-<sha>/libs.txt       jar list of the scanned tarball
#   scan-log.txt               one line per run: ref, sha, timestamp, dir
set -euo pipefail

REF="${1:?usage: scan_cve.sh <branch-or-tag-or-commit>}"
SRC="${KAFKA_SRC:-$HOME/project/kafka}"
WORK="${KAFKA_SCAN_WORK:-$HOME/cve-scan/work}"
OUT="${KAFKA_SCAN_OUT:-$HOME/cve-scan/out}"
GRYPE_IMAGE="${GRYPE_IMAGE:-docker.io/anchore/grype:latest}"

NAME="${REF//\//-}"                      # origin/4.2 -> origin-4.2
TREE="$WORK/$NAME"
DIST="$WORK/$NAME-dist"

mkdir -p "$WORK" "$OUT" "$HOME/.cache/grype"

# ---------------------------------------------------------------- 1. resolve ref
cd "$SRC"
git fetch --quiet origin
if git rev-parse --verify --quiet "refs/remotes/origin/$REF" >/dev/null; then
  RESOLVED="origin/$REF"                 # prefer remote branch over local
else
  RESOLVED="$REF"                        # tag / sha / any other ref
fi
SHA=$(git rev-parse --short "$RESOLVED")
echo ">>> $REF resolves to $RESOLVED ($SHA)"

# ---------------------------------------------------------------- 2. worktree
git worktree prune
git worktree remove --force "$TREE" 2>/dev/null || true
git worktree add --quiet --detach "$TREE" "$RESOLVED"
cleanup() {
  cd "$SRC" && git worktree remove --force "$TREE" 2>/dev/null || true
  rm -rf "$DIST"
}
trap cleanup EXIT

# ---------------------------------------------------------------- 3. build tarball
cd "$TREE"
echo ">>> building releaseTarGz (this takes a few minutes)"
./gradlew --quiet clean releaseTarGz
TGZ=$(ls core/build/distributions/kafka_2.13-*.tgz | grep -v site-docs | head -1)
echo ">>> built $TGZ"

# ---------------------------------------------------------------- 4. untar on host
rm -rf "$DIST" && mkdir -p "$DIST"
tar xzf "$TGZ" -C "$DIST" --strip-components=1
echo ">>> $(ls "$DIST/libs" | wc -l) jars in $DIST/libs"

# ---------------------------------------------------------------- 5. grype
RUN="$OUT/$NAME-$SHA"
mkdir -p "$RUN"
ls "$DIST/libs" > "$RUN/libs.txt"

podman run --rm \
  -v "$DIST/libs:/libs:ro,z" \
  -v "$HOME/.cache/grype:/root/.cache/grype:z" \
  "$GRYPE_IMAGE" dir:/libs --by-cve -o json > "$RUN/grype.json"

podman run --rm \
  -v "$DIST/libs:/libs:ro,z" \
  -v "$HOME/.cache/grype:/root/.cache/grype:z" \
  "$GRYPE_IMAGE" dir:/libs --by-cve | tee "$RUN/table.txt"

# ---------------------------------------------------------------- 6. summarize
jq -r '.matches[].vulnerability.id' "$RUN/grype.json" | sort -u > "$RUN/ids.txt"
echo "$RESOLVED $SHA $(date -u +%FT%TZ) $RUN" >> "$OUT/scan-log.txt"

echo
echo ">>> $(wc -l < "$RUN/ids.txt") unique vulnerabilities"
echo ">>> results: $RUN/"
echo ">>> compare: comm -23 <old>/ids.txt <new>/ids.txt   # fixed in new"
echo ">>>          comm -12 <old>/ids.txt <new>/ids.txt   # still present"