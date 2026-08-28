# Content Claim InputStream benchmark fixture

## Scope

- Benchmark-only change in `nifi-system-tests/nifi-record-serialization-benchmarks`.
- Uses the production-shaped `TaskTerminationInputStream -> ContentClaimInputStream -> ContentRepository` chain.
- `PRODUCTION` passes the streaming reader's 1,000,000-byte mark hint unchanged.
- `FORCE_REOPEN` uses the same `ContentClaimInputStream` implementation with a one-byte mark limit.
- Inputs contain 1, 100, or 600 records of approximately 2 KiB each.
- The repository is an in-memory read-only fixture, so results exclude disk latency.

## Correctness assertions

JMH setup validates all output records and repository open counts before measurement:

| Records | Approximate FlowFile size | `PRODUCTION` repository reads | `FORCE_REOPEN` repository reads |
| ---: | ---: | ---: | ---: |
| 1 | 2 KiB | 1 | 2 |
| 100 | 200 KiB | 1 | 2 |
| 600 | 1.2 MiB | 2 | 2 |

The 1.2 MiB production case crosses the current mark limit and exercises `ContentClaimInputStream.reset()` repository reopen behavior naturally.

## Verification

Dependency reactor install:

```shell
./mvnw -B -ntp -pl nifi-framework-bundle/nifi-framework/nifi-framework-components -am \
    -DskipTests -Dskip.nar -Dbuild.iteration=content-claim-fixture-deps-1 install
```

Result: 49 modules passed.

Benchmark clean package and tests:

```shell
./mvnw -B -ntp -Pbenchmarks -pl nifi-system-tests/nifi-record-serialization-benchmarks \
    -Dskip.nar -Dbuild.iteration=content-claim-fixture-final-1 clean verify
```

Result: 3 tests passed; shaded JAR built.

Fixture smoke matrix:

```shell
java -jar nifi-system-tests/nifi-record-serialization-benchmarks/target/benchmarks.jar \
    'StreamingJsonContentClaimBenchmark.convertFlowFile' \
    -p flowFileRecords=1,100,600 -p rewindMode=PRODUCTION,FORCE_REOPEN \
    -f 1 -wi 0 -i 1 -r 100ms -prof gc -rf json \
    -rff .claude_workspace/benchmarks/content-claim-d9c4a4ccbc86-fixture-smoke.json
```

- Benchmark JAR SHA-256: `d9c4a4ccbc869ceb9fc88a0b968a108ab4dbf451cdd8f37ef4cc72566c75fcf5`
- Raw result: `.claude_workspace/benchmarks/content-claim-d9c4a4ccbc86-fixture-smoke.json`
- This is a smoke run, not an acceptance-quality performance result.
- The fixture's initial signal is important: retaining the mark buffer allocated about 1.34 MB versus 0.80 MB for forced reopen at 100 records, and about 7.46 MB versus 5.20 MB at 600 records. A full warmed/forked run is required after rebuilding the final production sources.
- The benchmark artifact used the locally installed streaming service artifacts available at build time. Rebuild the streaming bundle and benchmark JAR together before final performance claims.
