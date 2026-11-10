# BigQuery JSON Write Benchmark

Compares three client-side JSON encoding and ingestion paths for the BigQuery
Storage Write API in `google-cloud-bigquery` ([#6597]):

1. **`arrow-json` -> Arrow IPC (`StreamWriter`)**: parses JSON rows into Arrow
   `RecordBatch`es with `arrow_json::ReaderBuilder` and serializes Arrow IPC
   frames (`reuse` reuses one decoder and `StreamWriter`; `per-batch` constructs
   them per `append()` call).
2. **`direct-proto` (`DeserializeSeed` -> `ProtoRows`)**: streams JSON tokens
   through a schema-driven `serde::de::DeserializeSeed` visitor directly into
   protobuf wire bytes (`ProtoRows`) with zero intermediate row structs.
3. **`hybrid-proto-reflect` (`DeserializeSeed` -> `prost_reflect::DynamicMessage` -> `ProtoRows`)**:
   uses the same streaming `DeserializeSeed` visitor to populate a reused
   `prost_reflect::DynamicMessage`, then encodes it with `prost::Message::encode`.

[#6597]: https://github.com/googleapis/google-cloud-rust/issues/6597

## Reproduction

### Offline CPU & Wire-Size Benchmark (no network or credentials)

```bash
cargo run --release -p bigquery-json-write-benchmark -- local
```

Optional flags: `--samples <N>` (default `9`), `--simple-rows <N>` (default
`60000`), `--realistic-rows <N>` (default `40000`).

### Live End-to-End Benchmark (BigQuery Storage Write API)

Creates a temporary dataset (`rust_bq_json_bench_<random>`), verifies stored
rows via BigQuery SQL `SELECT`, runs interleaved fresh-connection throughput
rounds across all three approaches, and deletes the temporary dataset on exit
(including on error or `Ctrl-C`).

```bash
cargo run --release -p bigquery-json-write-benchmark -- live \
    --project ${GOOGLE_CLOUD_PROJECT} \
    --rounds 5
```

## Sample Run Environment

- **CPU:** AMD EPYC 7B13 (32 cores / 64 threads, x86_64)
- **OS:** Linux x86_64
- **Toolchain:** `rustc 1.99.0`, `--release` profile
- **Crate versions:** `arrow` / `arrow-json` `60.1.0`, `prost` `0.14.4`, `prost-reflect` `0.16.5`, `serde_json` `1.0.154`
- **Live setup:** default stream, 1-row stream warmup excluded from timed loop, fresh `Write` client per approach per round

## Schemas

### Workload A: 2-Column Sample Schema (`examples/src/write/json_default.rs`, ~38 B JSON/row)

| Column | BigQuery Type | `ProtoRows` Wire Type | Arrow `DataType` |
| --- | --- | --- | --- |
| `string` | `STRING` | `TYPE_STRING` | `Utf8` |
| `int` | `INT64` | `TYPE_INT64` | `Int64` |

### Workload B: 11-Column Realistic Schema (~396 B JSON/row)

| Column | BigQuery Type | `ProtoRows` Wire Type | Arrow `DataType` |
| --- | --- | --- | --- |
| `id` | `INT64` | `TYPE_INT64` | `Int64` |
| `name` | `STRING` | `TYPE_STRING` | `Utf8` |
| `flag` | `BOOL` | `TYPE_BOOL` | `Boolean` |
| `amount` | `NUMERIC` | `TYPE_STRING` | `Decimal128(38, 9)` |
| `created` | `TIMESTAMP` | `TYPE_INT64` (epoch micros) | `Timestamp(Microsecond, Some("+00:00"))` |
| `d` | `DATE` | `TYPE_INT32` (days since 1970-01-01) | `Date32` |
| `dt` | `DATETIME` | `TYPE_INT64` (packed civil datetime) | `Timestamp(Microsecond, None)` (`google:sqlType:datetime`) |
| `meta` | `JSON` | `TYPE_STRING` | `Utf8` (`google:sqlType:json`) |
| `tags` | `ARRAY<STRING>` | `LABEL_REPEATED TYPE_STRING` | `List(Utf8)` |
| `payload` | `STRING` (100 B) | `TYPE_STRING` | `Utf8` |
| `addr` | `STRUCT<city STRING, zip STRING>` | `TYPE_MESSAGE` | `Struct(city: Utf8, zip: Utf8)` |

On both schemas, `direct-proto` and `hybrid-proto-reflect` are verified to emit
byte-for-byte identical `ProtoRows` payloads across 500 rows before timing.

## Results

### 1. `arrow-json` Semantic Gaps on BigQuery JSON Inputs

| Input Case | JSON | `arrow-json` (`strict_mode` + `coerce_primitive`) |
| --- | --- | --- |
| `BYTES` as base64 | `{"b": "aGk="}` | Error (`invalid hex encoding in binary data`) |
| `JSON` column as nested object | `{"meta": {"k": 1}}` | Error (`expected string got {"k": 1}`) |
| Case-insensitive column key | `{"Name": "alice"}` | Error (`column 'Name' missing from schema`) |
| `BOOL` as string | `{"flag": "true"}` | Error (`expected boolean got "true"`) |
| `INT64` as string | `{"id": "42"}` | OK |
| Error message PII check | `{"id": "SSN-999-00-1234"}` | Quotes raw input (`failed to parse "SSN-999-00-1234" as Int64`) and omits row index |

### 2. Offline CPU & Wire Size: Workload A (2 Columns, 60,000 rows/sample, 9 samples)

| Batch Size | Approach | Median (`ns/row`) | Min (`ns/row`) | Max (`ns/row`) | Wire (`B/row`) |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | `direct-proto` | 169 | 167 | 180 | 17.5 |
| 1 | `hybrid-proto-reflect` | 464 | 463 | 471 | 17.5 |
| 1 | `arrow-json (reuse)` | 1,896 | 1,853 | 1,972 | 448.0 |
| 1 | `arrow-json (per-batch)` | 3,793 | 3,756 | 4,009 | 448.0 |
| 10 | `direct-proto` | 144 | 143 | 146 | 17.5 |
| 10 | `hybrid-proto-reflect` | 437 | 435 | 440 | 17.5 |
| 10 | `arrow-json (reuse)` | 316 | 314 | 320 | 57.6 |
| 10 | `arrow-json (per-batch)` | 540 | 523 | 696 | 57.6 |
| 100 | `direct-proto` | 141 | 139 | 153 | 17.5 |
| 100 | `hybrid-proto-reflect` | 436 | 433 | 443 | 17.5 |
| 100 | `arrow-json (reuse)` | 148 | 147 | 151 | 25.5 |
| 100 | `arrow-json (per-batch)` | 172 | 170 | 181 | 25.5 |
| 1,000 | `direct-proto` | 141 | 139 | 151 | 17.5 |
| 1,000 | `hybrid-proto-reflect` | 448 | 437 | 513 | 17.5 |
| 1,000 | `arrow-json (reuse)` | 130 | 128 | 144 | 22.1 |
| 1,000 | `arrow-json (per-batch)` | 193 | 189 | 200 | 22.1 |

### 3. Offline CPU & Wire Size: Workload B (11 Columns, 40,000 rows/sample, 9 samples)

| Batch Size | Approach | Median (`ns/row`) | Min (`ns/row`) | Max (`ns/row`) | Wire (`B/row`) |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | `direct-proto` | 1,510 | 1,486 | 1,775 | 237.4 |
| 1 | `hybrid-proto-reflect` | 3,976 | 3,946 | 4,126 | 237.4 |
| 1 | `arrow-json (reuse)` | 10,175 | 10,046 | 10,532 | 2,176.0 |
| 1 | `arrow-json (per-batch)` | 16,802 | 16,671 | 17,807 | 2,176.0 |
| 10 | `direct-proto` | 1,463 | 1,454 | 1,565 | 237.4 |
| 10 | `hybrid-proto-reflect` | 3,933 | 3,900 | 4,235 | 237.4 |
| 10 | `arrow-json (reuse)` | 2,142 | 2,094 | 2,395 | 403.2 |
| 10 | `arrow-json (per-batch)` | 2,775 | 2,756 | 3,095 | 403.2 |
| 100 | `direct-proto` | 1,506 | 1,468 | 1,657 | 237.4 |
| 100 | `hybrid-proto-reflect` | 3,957 | 3,936 | 4,173 | 237.4 |
| 100 | `arrow-json (reuse)` | 1,291 | 1,251 | 1,336 | 272.6 |
| 100 | `arrow-json (per-batch)` | 1,385 | 1,357 | 1,542 | 272.6 |
| 1,000 | `direct-proto` | 1,465 | 1,455 | 1,481 | 237.4 |
| 1,000 | `hybrid-proto-reflect` | 3,979 | 3,909 | 4,153 | 237.4 |
| 1,000 | `arrow-json (reuse)` | 1,219 | 1,205 | 1,341 | 258.3 |
| 1,000 | `arrow-json (per-batch)` | 1,645 | 1,607 | 1,701 | 258.3 |

### 4. Live End-to-End Ingestion Throughput (Workload B, 11 Columns)

Each round opens a fresh `Write` client and default stream, warms up the stream
with 1 row, and alternates approach order (`[Arrow, Direct, Hybrid]` on even
rounds, `[Hybrid, Direct, Arrow]` on odd rounds). SQL `SELECT` readback
verified 5/5 stored rows per approach prior to timing.

#### Run 1 (5 interleaved rounds/scenario)

| Scenario | Approach | Median (`rows/s`) | Min (`rows/s`) | Max (`rows/s`) |
| --- | --- | ---: | ---: | ---: |
| 10 rows/append, pipelined (2,000 rows/round) | `arrow-json` -> Arrow writer | 8,313 | 4,100 | 9,954 |
| 10 rows/append, pipelined (2,000 rows/round) | `direct-proto` -> `ProtoRows` | 17,018 | 13,521 | 18,167 |
| 10 rows/append, pipelined (2,000 rows/round) | `hybrid-proto-reflect` -> `ProtoRows` | 17,746 | 17,034 | 22,703 |
| 500 rows/append, sequential (10,000 rows/round) | `arrow-json` -> Arrow writer | 9,219 | 7,958 | 10,433 |
| 500 rows/append, sequential (10,000 rows/round) | `direct-proto` -> `ProtoRows` | 10,622 | 9,871 | 11,024 |
| 500 rows/append, sequential (10,000 rows/round) | `hybrid-proto-reflect` -> `ProtoRows` | 9,933 | 9,409 | 10,581 |
| 500 rows/append, pipelined (25,000 rows/round) | `arrow-json` -> Arrow writer | 64,235 | 20,453 | 83,622 |
| 500 rows/append, pipelined (25,000 rows/round) | `direct-proto` -> `ProtoRows` | 105,714 | 90,833 | 123,666 |
| 500 rows/append, pipelined (25,000 rows/round) | `hybrid-proto-reflect` -> `ProtoRows` | 97,621 | 65,424 | 124,847 |

#### Run 2 (5 interleaved rounds/scenario)

| Scenario | Approach | Median (`rows/s`) | Min (`rows/s`) | Max (`rows/s`) |
| --- | --- | ---: | ---: | ---: |
| 10 rows/append, pipelined (2,000 rows/round) | `arrow-json` -> Arrow writer | 7,030 | 2,758 | 10,038 |
| 10 rows/append, pipelined (2,000 rows/round) | `direct-proto` -> `ProtoRows` | 16,418 | 15,144 | 17,960 |
| 10 rows/append, pipelined (2,000 rows/round) | `hybrid-proto-reflect` -> `ProtoRows` | 16,638 | 15,760 | 17,419 |
| 500 rows/append, sequential (10,000 rows/round) | `arrow-json` -> Arrow writer | 9,314 | 7,826 | 9,943 |
| 500 rows/append, sequential (10,000 rows/round) | `direct-proto` -> `ProtoRows` | 10,162 | 9,463 | 11,399 |
| 500 rows/append, sequential (10,000 rows/round) | `hybrid-proto-reflect` -> `ProtoRows` | 9,793 | 9,077 | 10,680 |
| 500 rows/append, pipelined (25,000 rows/round) | `arrow-json` -> Arrow writer | 42,961 | 38,932 | 77,562 |
| 500 rows/append, pipelined (25,000 rows/round) | `direct-proto` -> `ProtoRows` | 76,230 | 62,624 | 105,168 |
| 500 rows/append, pipelined (25,000 rows/round) | `hybrid-proto-reflect` -> `ProtoRows` | 89,627 | 69,479 | 97,808 |
