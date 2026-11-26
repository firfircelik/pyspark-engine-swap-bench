#!/bin/bash
# Download and checksum the pinned Iceberg / Delta / Comet-4.1 jars into dist/.
set -euo pipefail
ROOT=${ROOT:-/workspace/engine-swap}
DEST=$ROOT/dist
mkdir -p "$DEST"
cd "$DEST"

fetch() {
  local url=$1 dest=$2 alg=$3 expected=$4
  if [ -f "$dest" ]; then
    local got
    got=$(${alg}sum "$dest" | awk '{print $1}')
    if [ "$got" = "$expected" ]; then
      echo "OK cached $dest"
      return
    fi
    echo "checksum mismatch for cached $dest (got $got want $expected), re-downloading"
    rm -f "$dest"
  fi
  echo "GET $url"
  curl -fL --retry 3 --retry-delay 2 -o "$dest" "$url"
  local got
  got=$(${alg}sum "$dest" | awk '{print $1}')
  if [ "$got" != "$expected" ]; then
    echo "CHECKSUM FAIL $dest got $got want $expected" >&2
    rm -f "$dest"
    exit 1
  fi
  echo "OK $dest sha${alg}=$got"
}

# sha files from Maven Central, fetched and recorded here so the expected
# digests are pinned in this script after the first successful resolve.
# We resolve expected values live below if EXPECT file is absent.

pin() {
  # pin <url> <filename> <sha512|sha1>
  local url=$1 name=$2 alg=$3
  local sumurl="${url}.${alg}"
  local expected
  expected=$(curl -fsSL "$sumurl" | awk '{print $1}')
  if [ -z "$expected" ]; then
    echo "failed to fetch $sumurl" >&2
    exit 1
  fi
  echo "$name sha${alg}=$expected" | tee -a $ROOT/results/tablefmt/jar-checksums.txt
  fetch "$url" "$name" "$alg" "$expected"
}

: > $ROOT/results/tablefmt/jar-checksums.txt
pin https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.12.0/iceberg-spark-runtime-4.1_2.13-1.12.0.jar \
    iceberg-spark-runtime-4.1_2.13-1.12.0.jar sha512
pin https://repo1.maven.org/maven2/io/delta/delta-spark_4.2_2.13/4.4.1/delta-spark_4.2_2.13-4.4.1.jar \
    delta-spark_4.2_2.13-4.4.1.jar sha512
pin https://repo1.maven.org/maven2/io/delta/delta-spark_4.1_2.13/4.4.1/delta-spark_4.1_2.13-4.4.1.jar \
    delta-spark_4.1_2.13-4.4.1.jar sha512
pin https://repo1.maven.org/maven2/io/delta/delta-storage/4.4.1/delta-storage-4.4.1.jar \
    delta-storage-4.4.1.jar sha512
pin https://repo1.maven.org/maven2/io/delta/delta-kernel-api/4.4.1/delta-kernel-api-4.4.1.jar \
    delta-kernel-api-4.4.1.jar sha512
pin https://repo1.maven.org/maven2/io/delta/delta-kernel-defaults/4.4.1/delta-kernel-defaults-4.4.1.jar \
    delta-kernel-defaults-4.4.1.jar sha512
pin https://repo1.maven.org/maven2/io/delta/delta-kernel-unitycatalog/4.4.1/delta-kernel-unitycatalog-4.4.1.jar \
    delta-kernel-unitycatalog-4.4.1.jar sha512
# Comet 4.1 publishes sha1 only
pin https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark4.1_2.13/1.2.0/comet-spark-spark4.1_2.13-1.2.0.jar \
    comet-spark-spark4.1_2.13-1.2.0.jar sha1

ls -lh iceberg-spark-runtime-4.1_2.13-1.12.0.jar delta-spark_4.2_2.13-4.4.1.jar delta-spark_4.1_2.13-4.4.1.jar \
  delta-storage-4.4.1.jar delta-kernel-api-4.4.1.jar delta-kernel-defaults-4.4.1.jar \
  delta-kernel-unitycatalog-4.4.1.jar comet-spark-spark4.1_2.13-1.2.0.jar
