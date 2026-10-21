#!/usr/bin/env bash
# Downloads the DEBS 2022 Grand Challenge trading data (Zenodo record 6382482,
# CC BY-NC-SA 4.0) into data/full/, verifying each file's MD5.
#
#   scripts/download-data.sh              # all 7 days (~25 GB)
#   scripts/download-data.sh 13-11-21     # only the given days (DD-MM-YY)
#
# Files that already match are skipped; interrupted downloads resume.
# DATA_DIR overrides the target directory.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
data_dir="${DATA_DIR:-$repo_root/data/full}"
base_url="https://zenodo.org/api/records/6382482/files"

# day -> MD5, as published on Zenodo.
declare -A md5=(
  [08-11-21]=a08013f09a6b77355dae8b3ce141624d
  [09-11-21]=3e1b7a79c63c71e03f4cef82fbda94a2
  [10-11-21]=bed2f79e124f68da54929500f115a820
  [11-11-21]=943b8b04853c3fe70477b5aa1e5c6533
  [12-11-21]=4d9d0529948879e581cbe047eb23bfb7
  [13-11-21]=64f57410de31c5753b848bf4eeee7bc4
  [14-11-21]=adfc75f72d9348209d602baa72af44b5
)
all_days=(08-11-21 09-11-21 10-11-21 11-11-21 12-11-21 13-11-21 14-11-21)

days=("$@")
[ ${#days[@]} -eq 0 ] && days=("${all_days[@]}")
for day in "${days[@]}"; do
  [ -n "${md5[$day]:-}" ] || { echo "unknown day '$day' (expected one of: ${all_days[*]})" >&2; exit 1; }
done

mkdir -p "$data_dir"
if [ ! -w "$data_dir" ]; then
  echo "$data_dir is not writable (Docker may have created it as root): sudo chown \"\$USER\" $data_dir" >&2
  exit 1
fi
[ ${#days[@]} -eq 7 ] && echo "downloading the full week into $data_dir: needs ~25 GB of free disk"

for day in "${days[@]}"; do
  name="debs2022-gc-trading-day-$day.csv"
  file="$data_dir/$name"
  if [ -f "$file" ] && echo "${md5[$day]}  $file" | md5sum -c --status; then
    echo "$name: already present"
    continue
  fi
  echo "$name: downloading"
  curl -fL --retry 3 -C - -o "$file.part" "$base_url/$name/content"
  if echo "${md5[$day]}  $file.part" | md5sum -c --status; then
    mv "$file.part" "$file"
    echo "$name: OK"
  else
    rm -f "$file.part"
    echo "$name: MD5 mismatch, removed the partial file; run again" >&2
    exit 1
  fi
done
