#!/usr/bin/env bash

# Find the common base64 suffices for a given plaintext suffix
# Christopher Harrison <ch12@sanger.ac.uk>

set -eu
declare -i TRIALS="${TRIALS-10}"

random_data() {
  local bytes="$1"

  dd if=/dev/urandom 2>/dev/null \
  | tr -dc "a-zA-Z0-9" \
  | head -c "${bytes}"
}

common_suffix() {
  printf "%s\n%s\n" "$@" \
  | rev \
  | sed -e "N;s/^\(.*\).*\n\1.*$/\1/" \
  | rev
}

main() {
  local txt_suffix="$1"
  local -a b64_suffix=()

  local -i trial
  local -i done=0
  local sample
  local sample_bin
  for trial in $(seq 0 $(( (TRIALS * 3) - 1 ))); do
    sample="$(echo -n "$(random_data "${trial}")${txt_suffix}" | base64)"
    sample_bin="$(echo "${sample}" | tr -dc "=" | wc -c)"
    b64_suffix[${sample_bin}]="$(common_suffix "${b64_suffix[${sample_bin}]-${sample}}" "${sample}")"

    if (( trial % 3 == 2 )); then
      done+=1
      echo "Completed trial ${done} of ${TRIALS}" >&2
    fi
  done

  echo "Common base64 suffices for \"${txt_suffix}\":" >&2
  printf "%s\n" "${b64_suffix[@]}"
}

main "${1-.bam}"
