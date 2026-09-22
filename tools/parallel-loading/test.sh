#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TEST_WORK="${PARALLEL_TEST_WORK:-$ROOT/.parallel-test}"
mkdir -p "$TEST_WORK/deps" "$TEST_WORK/classes"
fetch() {
  if [ ! -f "$TEST_WORK/deps/$2" ]; then
    curl -fLsS --retry 3 "$1" -o "$TEST_WORK/deps/$2.tmp"
    mv "$TEST_WORK/deps/$2.tmp" "$TEST_WORK/deps/$2"
  fi
}
fetch https://repo.maven.apache.org/maven2/com/squareup/okhttp3/okhttp/3.12.13/okhttp-3.12.13.jar okhttp.jar
fetch https://repo.maven.apache.org/maven2/com/squareup/okio/okio/1.15.0/okio-1.15.0.jar okio.jar
javac -cp "$TEST_WORK/deps/*" -d "$TEST_WORK/classes" \
  "$ROOT/exoplayer-amzn-2.10.6/extensions/okhttp/src/main/java/com/google/android/exoplayer2/ext/okhttp/ParallelRangeReader.java" \
  "$ROOT/tools/parallel-loading/ParallelRangeReaderTest.java"
java -cp "$TEST_WORK/classes:$TEST_WORK/deps/*" com.google.android.exoplayer2.ext.okhttp.ParallelRangeReaderTest
