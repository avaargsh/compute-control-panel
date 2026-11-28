#!/bin/sh
# Print one UTC timestamp for every binary in a make build.
set -eu
format='+%Y-%m-%dT%H:%M:%SZ'
epoch=${SOURCE_DATE_EPOCH:-}

case "$epoch" in
    '') exec date -u "$format" ;;
    *[!0123456789]*)
        printf '%s\n' 'SOURCE_DATE_EPOCH must be a non-negative integer (Unix seconds).' >&2
        exit 1 ;;
esac

# BSD date also accepts octal; remove leading zeros to preserve decimal input.
epoch=${epoch#"${epoch%%[!0]*}"}
epoch=${epoch:-0}
case "$(uname -s)" in
    Darwin|*BSD|DragonFly) stamp=$(date -u -r "$epoch" "$format") ;;
    *) stamp=$(date -u -d "@$epoch" "$format") ;;
esac
printf '%s\n' "$stamp"
