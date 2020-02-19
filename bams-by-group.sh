#!/usr/bin/env bash

# Filter mpistat output for BAM files by a given group
# Christopher Harrison <ch12@sanger.ac.uk>

set -eu

declare WORK_DIR="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
export PATH="${WORK_DIR}:${PATH}"

group_id() {
  local group="$1"
  getent group "${group}" | cut -d: -f3
}

base64_regex() {
  local suffix="$1"

  base64-suffix.sh "${suffix}" 2>/dev/null \
  | paste -sd "|" \
  | sed "s/.*/(&)$/"
}

main() {
  local group="$1"
  local gid="$(group_id "${group}")"

  local regex="$(base64_regex ".bam")"

  awk -v GID="${gid}" -v REGEX="${regex}" '
    BEGIN { FS  = "\t" }

    $4 == GID && $8 == "f" && $1 ~ REGEX {
      print $1 "AA=="
    }
  ' \
  | base64 -di
}

main "$@"
