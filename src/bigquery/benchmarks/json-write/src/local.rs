// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::args::LocalConfig;
use crate::encoder::{
    DirectState, MessagePlan, encode_row_direct, encode_row_hybrid, proto_rows_wire_size,
};
use crate::schema::{
    generate_realistic_json_rows, generate_simple_json_rows, realistic_arrow_schema,
    realistic_proto_schema_and_plan, simple_arrow_schema, simple_message_plan,
};
use anyhow::{Result, bail};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow_json::ReaderBuilder;
use bytes::BytesMut;
use prost_reflect::DynamicMessage;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

const BATCH_SIZES: [usize; 4] = [1, 10, 100, 1000];

#[derive(Clone, Copy)]
struct SampleStats {
    median_ns: f64,
    min_ns: f64,
    max_ns: f64,
    wire_bytes_per_row: f64,
}

fn measure_samples<F>(total_rows: usize, samples: usize, mut run_once: F) -> Result<SampleStats>
where
    F: FnMut() -> Result<usize>,
{
    let wire_bytes = black_box(run_once()?);
    let mut ns_per_row = Vec::with_capacity(samples);
    for _ in 0..samples {
        let start = Instant::now();
        let bytes = black_box(run_once()?);
        let elapsed_ns = start.elapsed().as_nanos() as f64;
        debug_assert_eq!(bytes, wire_bytes);
        ns_per_row.push(elapsed_ns / total_rows as f64);
    }
    ns_per_row.sort_by(|a, b| a.total_cmp(b));
    Ok(SampleStats {
        median_ns: ns_per_row[ns_per_row.len() / 2],
        min_ns: ns_per_row[0],
        max_ns: ns_per_row[ns_per_row.len() - 1],
        wire_bytes_per_row: wire_bytes as f64 / total_rows as f64,
    })
}

fn check_arrow_json_compatibility() -> Result<()> {
    println!("== 1. arrow-json (v60) semantic gaps on BigQuery JSON inputs ==\n");

    let cases: Vec<(&str, Field, &str)> = vec![
        (
            "BYTES as base64 (\"aGk=\")",
            Field::new("b", DataType::Binary, true),
            r#"{"b": "aGk="}"#,
        ),
        (
            "JSON column as nested object",
            Field::new("meta", DataType::Utf8, true),
            r#"{"meta": {"k": 1}}"#,
        ),
        (
            "Case-insensitive key (\"Name\" for \"name\")",
            Field::new("name", DataType::Utf8, true),
            r#"{"Name": "alice"}"#,
        ),
        (
            "BOOL as string (\"true\")",
            Field::new("flag", DataType::Boolean, true),
            r#"{"flag": "true"}"#,
        ),
        (
            "INT64 as string (\"42\")",
            Field::new("id", DataType::Int64, true),
            r#"{"id": "42"}"#,
        ),
        (
            "Bad value error PII check (secret string in INT64)",
            Field::new("id", DataType::Int64, true),
            r#"{"id": "SSN-999-00-1234"}"#,
        ),
    ];

    for (label, field, json) in cases {
        let schema = Arc::new(Schema::new(vec![field]));
        let mut dec = ReaderBuilder::new(schema)
            .with_strict_mode(true)
            .with_coerce_primitive(true)
            .build_decoder()?;
        let res = dec.decode(json.as_bytes()).and_then(|_| dec.flush());
        match res {
            Ok(_) => println!("  OK    {label:<52} {json}"),
            Err(e) => println!("  ERROR {label:<52} {json}\n        -> {e}"),
        }
    }
    println!();
    Ok(())
}

fn verify_proto_equivalence(plan: &MessagePlan, rows: &[String], check_rows: usize) -> Result<()> {
    let n = rows.len().min(check_rows);
    let mut state = DirectState::with_capacity(256);
    let mut dbuf = BytesMut::new();
    let mut hbuf = BytesMut::new();
    let mut msg = DynamicMessage::new(plan.msg_desc.clone());
    for (idx, row) in rows.iter().take(n).enumerate() {
        encode_row_direct(plan, row, &mut dbuf, &mut state)?;
        let dbytes = dbuf.split().freeze();
        encode_row_hybrid(plan, row, &mut msg, &mut hbuf)?;
        let hbytes = hbuf.split().freeze();
        if dbytes != hbytes {
            bail!("direct vs hybrid wire mismatch at row {idx}");
        }
        let _decoded = DynamicMessage::decode(plan.msg_desc.clone(), dbytes)?;
    }
    Ok(())
}

fn run_workload(
    title: &str,
    rows: &[String],
    plan: &MessagePlan,
    arrow_schema: &Arc<Schema>,
    coerce_primitive: bool,
    row_cap_hint: usize,
    samples: usize,
) -> Result<()> {
    let check_n = rows.len().min(500);
    verify_proto_equivalence(plan, rows, check_n)?;
    let avg_json = rows.iter().map(String::len).sum::<usize>() as f64 / rows.len() as f64;

    println!(
        "== {title} ({} rows/sample, ~{avg_json:.0} B JSON/row, {samples} samples) ==",
        rows.len()
    );
    println!(
        "  (Verified: direct ProtoRows and prost-reflect DynamicMessage produce identical bytes on {check_n}/{check_n} rows)\n"
    );
    println!(
        "  {:<6} | {:<24} | {:>11} | {:>11} | {:>11} | {:>10}",
        "Batch", "Approach", "Median ns/r", "Min ns/r", "Max ns/r", "Wire B/r"
    );
    println!("  {}", "-".repeat(86));

    for batch_size in BATCH_SIZES {
        // Compute exact ProtoRows wire size across all batches once outside the CPU loop
        // so the timed loop measures pure encoding into `Vec<Bytes>` without extra wire-size math.
        let proto_wire_total = {
            let mut state = DirectState::with_capacity(256);
            let mut total = 0usize;
            for chunk in rows.chunks(batch_size) {
                let mut buf = BytesMut::with_capacity(chunk.len() * row_cap_hint);
                let mut batch = Vec::with_capacity(chunk.len());
                for r in chunk {
                    encode_row_direct(plan, r, &mut buf, &mut state)?;
                    batch.push(buf.split().freeze());
                }
                total += proto_rows_wire_size(&batch);
            }
            total
        };

        let mut direct = measure_samples(rows.len(), samples, || {
            let mut state = DirectState::with_capacity(256);
            let mut count = 0usize;
            for chunk in rows.chunks(batch_size) {
                let mut buf = BytesMut::with_capacity(chunk.len() * row_cap_hint);
                let mut batch = Vec::with_capacity(chunk.len());
                for r in chunk {
                    encode_row_direct(plan, r, &mut buf, &mut state)?;
                    batch.push(buf.split().freeze());
                }
                count += black_box(batch).len();
            }
            debug_assert_eq!(count, rows.len());
            Ok(proto_wire_total)
        })?;
        direct.wire_bytes_per_row = proto_wire_total as f64 / rows.len() as f64;

        let mut hybrid = measure_samples(rows.len(), samples, || {
            let mut msg = DynamicMessage::new(plan.msg_desc.clone());
            let mut count = 0usize;
            for chunk in rows.chunks(batch_size) {
                let mut buf = BytesMut::with_capacity(chunk.len() * row_cap_hint);
                let mut batch = Vec::with_capacity(chunk.len());
                for r in chunk {
                    encode_row_hybrid(plan, r, &mut msg, &mut buf)?;
                    batch.push(buf.split().freeze());
                }
                count += black_box(batch).len();
            }
            debug_assert_eq!(count, rows.len());
            Ok(proto_wire_total)
        })?;
        hybrid.wire_bytes_per_row = proto_wire_total as f64 / rows.len() as f64;

        let arrow_reuse = measure_samples(rows.len(), samples, || {
            let mut ipc_writer = StreamWriter::try_new(
                Vec::with_capacity(batch_size.saturating_mul(row_cap_hint).max(64 * 1024)),
                arrow_schema,
            )?;
            let _ = std::mem::take(ipc_writer.get_mut());
            let mut decoder = ReaderBuilder::new(arrow_schema.clone())
                .with_batch_size(batch_size.max(1024))
                .with_strict_mode(true)
                .with_coerce_primitive(coerce_primitive)
                .build_decoder()?;
            let mut total_ipc = 0usize;
            for chunk in rows.chunks(batch_size) {
                for r in chunk {
                    decoder.decode(r.as_bytes())?;
                }
                if let Some(rb) = decoder.flush()? {
                    ipc_writer.write(&rb)?;
                    let bytes = std::mem::take(ipc_writer.get_mut());
                    total_ipc += black_box(bytes).len();
                }
            }
            Ok(total_ipc)
        })?;

        let arrow_per_batch = measure_samples(rows.len(), samples, || {
            let mut total_ipc = 0usize;
            for chunk in rows.chunks(batch_size) {
                let mut ipc_writer = StreamWriter::try_new(Vec::new(), arrow_schema)?;
                let _ = std::mem::take(ipc_writer.get_mut());
                let mut decoder = ReaderBuilder::new(arrow_schema.clone())
                    .with_batch_size(batch_size)
                    .with_strict_mode(true)
                    .with_coerce_primitive(coerce_primitive)
                    .build_decoder()?;
                for r in chunk {
                    decoder.decode(r.as_bytes())?;
                }
                if let Some(rb) = decoder.flush()? {
                    ipc_writer.write(&rb)?;
                    let bytes = std::mem::take(ipc_writer.get_mut());
                    total_ipc += black_box(bytes).len();
                }
            }
            Ok(total_ipc)
        })?;

        for (label, stats) in [
            ("direct-proto", direct),
            ("hybrid-proto-reflect", hybrid),
            ("arrow-json (reuse)", arrow_reuse),
            ("arrow-json (per-batch)", arrow_per_batch),
        ] {
            println!(
                "  {:<6} | {:<24} | {:>11.0} | {:>11.0} | {:>11.0} | {:>10.1}",
                batch_size,
                label,
                stats.median_ns,
                stats.min_ns,
                stats.max_ns,
                stats.wire_bytes_per_row,
            );
        }
        println!("  {}", "-".repeat(86));
    }
    println!();
    Ok(())
}

pub fn run(config: &LocalConfig) -> Result<()> {
    if config.samples == 0 {
        bail!("--samples must be at least 1");
    }
    if config.simple_rows == 0 || config.realistic_rows == 0 {
        bail!("--simple-rows and --realistic-rows must be at least 1");
    }

    check_arrow_json_compatibility()?;

    let simple_plan = simple_message_plan();
    let simple_arrow = simple_arrow_schema();
    let simple_rows = generate_simple_json_rows(config.simple_rows);
    run_workload(
        "2. Workload A: json_default.rs schema (2 cols: STRING, INT64)",
        &simple_rows,
        &simple_plan,
        &simple_arrow,
        false,
        32,
        config.samples,
    )?;

    let (_proto_schema, realistic_plan) = realistic_proto_schema_and_plan();
    let (realistic_arrow, _arrow_wire_schema) = realistic_arrow_schema();
    let realistic_rows = generate_realistic_json_rows(config.realistic_rows, None);
    run_workload(
        "3. Workload B: Realistic 11-col BigQuery schema (INT64, STRING, BOOL, NUMERIC, TIMESTAMP, DATE, DATETIME, JSON, REPEATED, STRUCT)",
        &realistic_rows,
        &realistic_plan,
        &realistic_arrow,
        true,
        256,
        config.samples,
    )?;

    Ok(())
}
