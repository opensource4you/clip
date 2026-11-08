#!/usr/bin/env bash
# Remove every container (running or not) that mounts the given Gradle cache dir.
# A cancelled job leaves its container running and holding the cache lock, so the next
# run fails with "Timeout waiting to lock journal cache". Matching on the mount source
# catches leftovers with or without a name, and never touches containers of other
# runners on the same machine, which use their own cache dirs.
# Usage: remove_cache_containers.sh <podman|docker> <gradle cache dir>
set -u

cr=$1
cache=$(realpath -m "$2")

for id in $("$cr" ps -aq 2>/dev/null); do
  sources=$("$cr" inspect -f '{{range .Mounts}}{{.Source}}{{"\n"}}{{end}}' "$id" 2>/dev/null) || continue
  while IFS= read -r src; do
    [ -n "$src" ] || continue
    if [ "$(realpath -m "$src")" = "$cache" ]; then
      echo "removing leftover container $id (mounts $cache)"
      "$cr" rm -f "$id" >/dev/null 2>&1 || true
      break
    fi
  done <<< "$sources"
done
exit 0
