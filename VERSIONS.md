# Versions, environment and checksums

## Hardware and OS

- One Linux VM, 8 vCPU (Intel Xeon), 16 GB RAM, kernel 6.12. One benchmark at a time. No containers.
- Memory cap: cgroup v2 group `/sys/fs/cgroup/engine-swap-bench`, `memory.max=9G`, `memory.swap.max=0`. Every job process tree is moved into it by `scripts/run_one.py --cgroup` (needs sudo). Recorded per job: tree RSS peak, cgroup `memory.peak` (includes page cache), `oom_kill` events.
- Kernel had no `sch_netem`, so latency was injected with toxiproxy instead of `tc netem`.

## Engines and tools

| Component | Version | Source / note |
|---|---|---|
| Apache Spark | 4.1.3 (hadoop-client 3.4.2), 4.2.0 (hadoop-client 3.5.0) | archive.apache.org binary tarballs, sha512 verified |
| S3A jars for 4.1.3 | hadoop-aws 3.4.2, AWS SDK bundle 2.29.52, analyticsaccelerator-s3 (AAL) 1.2.1 | Maven Central, versions from the Hadoop poms, copied into `jars/` |
| S3A jars for 4.2.0 | hadoop-aws 3.5.0, AWS SDK bundle 2.35.4, AAL 1.3.1 | as above |
| DataFusion Comet | 1.2.0 (`comet-spark-spark4.2_2.13`, `comet-spark-spark4.1_2.13`) | Maven Central |
| Sail | pysail 0.7.3 (2026-10-30), pyspark-client 4.2.0 | PyPI, Spark Connect server |
| DuckDB | 2.0.1 (Python) | PyPI; httpfs, iceberg, delta extensions installed from a local extension dir |
| Iceberg | iceberg-spark-runtime-4.1_2.13 1.12.0 | no 4.2 runtime published (HTTP 404 on 2026-11-26) |
| Delta Lake | delta-spark 4.4.1 (4.1 and 4.2 builds), delta-storage 4.4.1 | Maven Central |
| JDK | Eclipse Temurin 21.0.13.1+2 (tzdata 2026f) | adoptium.net tarball |
| Python | 3.12.16 | venv-spark: pyarrow 26.0.1, pandas 2.3.3, numpy 2.5.5, psutil 7.2.2; venv-sail: pysail 0.7.3, pyspark-client 4.2.0, duckdb 2.0.1, pyarrow 26.0.1, pandas 3.1.1, psutil 7.2.2 |
| Analysis | numpy 2.5.5, scipy 1.18.1, pandas 3.0.6, matplotlib 3.11.2 | seed 20261126 |
| MinIO | `RELEASE.2025-10-15T17-29-55Z`, built from source with go 1.27.4; `mc` from source | dl.min.io community binaries returned HTTP 410 |
| toxiproxy | 2.13.0 | GitHub release |
| Data generator | tpchgen-cli 4.0.0 | `cargo install tpchgen-cli` |
| Paper build | pandoc 3.14, tectonic | static binaries |

## Engine settings inside the cap

- Spark: `local[8]`, driver memory 6g.
- Comet: driver heap 2g + off-heap 3g, `MALLOC_ARENA_MAX=2` (see paper Section 7 for why).
- DuckDB: `memory_limit='7GB'`, 8 threads, `enable_external_file_cache=false`, fresh process per run.
- Sail: defaults (no comparable memory knob); join reorder on/off is a server setting.

## Jar checksums (SHA-256)

| Jar | Size | SHA-256 |
|---|---|---|
| `hadoop-aws-3.4.2.jar` | 916K | `d2d24e6b715b5656b9d238bf13b6682a07e2004dd36aad74a73aeb854319f4d9` |
| `bundle-2.29.52.jar` | 612M | `62638f33045690c6bc5e496b54b0b2e89a9ad38cbedacdf6e017fea6bc85f55c` |
| `analyticsaccelerator-s3-1.2.1.jar` | 2.2M | `11029552e37408e41e689b20150f4b372e0b4f232de321ccafd0fcf443cdfb5a` |
| `hadoop-aws-3.5.0.jar` | 920K | `66bd9eefb938b1a1657487324b63c13df2d8b17164a1dd24e0ef37b905bc5baa` |
| `bundle-2.35.4.jar` | 655M | `db4b55f24ad1b4e70f17eb1717a4de76a8dadc5a27d764be7c44ff67b2c1c677` |
| `analyticsaccelerator-s3-1.3.1.jar` | 2.4M | `4959a9747cbb479c73c3ffbcd479bee5b014f8b2880994585a8693fb9d24675e` |
| `comet-spark-spark4.1_2.13-1.2.0.jar` | 95M | `5055c43dca9e8cce83c33b1ff25646d1436c94698ca48beb3475bf60843b8d37` |
| `comet-spark-spark4.2_2.13-1.2.0.jar` | 95M | `ea5d1caba4db6b16e86c31aa0694ea1330a8faaec58d44856df425adf5f7fe86` |
| `delta-kernel-api-4.4.1.jar` | 2.8M | `19c93b5e6740bb3001d3deca0b8c3fa5470197c520ffa36ffa58df98e91ae459` |
| `delta-kernel-defaults-4.4.1.jar` | 224K | `329995bbc9b2e5d74a9a7c9e968574c1e438bbb183aaf1b2c629a87ce8558fe2` |
| `delta-kernel-unitycatalog-4.4.1.jar` | 44K | `4d92b9248f8a5f9b3043fea2bb7666f0db26f5491e279c30868280be35450368` |
| `delta-spark_4.1_2.13-4.4.1.jar` | 9.1M | `ef0e152d6f43a22a4fed7b65a1b7508cee1e20a70f7d301fb53c6a9225e1b979` |
| `delta-spark_4.2_2.13-4.4.1.jar` | 9.1M | `2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be` |
| `delta-storage-4.4.1.jar` | 112K | `f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f` |
| `iceberg-spark-runtime-4.1_2.13-1.12.0.jar` | 47M | `3123c3798af642dd537230923548e013ef41cc28b6626e5225808cf5670a0f82` |

Jars are not in this repository. `scripts/fetch_tablefmt_jars.sh` downloads the Iceberg, Delta and Comet 4.1 jars and checks them.
