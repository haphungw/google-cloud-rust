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

use crate::args::LiveConfig;
use crate::encoder::{DirectState, MessagePlan, encode_batch_direct, encode_batch_hybrid};
use crate::schema::{
    bq_realistic_table_schema, generate_realistic_json_rows, realistic_arrow_schema,
    realistic_proto_schema_and_plan,
};
use anyhow::{Context, Result, bail};
use arrow::datatypes::Schema;
use arrow::ipc::writer::StreamWriter;
use arrow_json::ReaderBuilder;
use google_cloud_bigquery::client::{BigQuery, Write};
use google_cloud_bigquery::model::{ArrowRecordBatch, ArrowSchema, ProtoSchema};
use google_cloud_bigquery_v2::client::{DatasetService, TableService};
use google_cloud_bigquery_v2::model::get_table_request::TableMetadataView;
use google_cloud_bigquery_v2::model::{
    Dataset, DatasetReference, Table, TableReference, TableSchema,
};
use prost_reflect::DynamicMessage;
use rand::RngExt;
use rand::distr::Alphanumeric;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

#[derive(Clone, Copy)]
enum Approach {
    ArrowJson,
    DirectProto,
    HybridProtoReflect,
}

impl Approach {
    fn label(self) -> &'static str {
        match self {
            Self::ArrowJson => "1. arrow-json -> Arrow writer",
            Self::DirectProto => "2. Direct DeserializeSeed -> ProtoRows",
            Self::HybridProtoReflect => "3. Hybrid prost-reflect -> ProtoRows",
        }
    }
}

struct BenchmarkDataset {
    project: String,
    dataset_id: String,
    is_temporary: bool,
    dataset_service: DatasetService,
}

impl BenchmarkDataset {
    async fn setup(project: &str, dataset_id: &str) -> Result<Self> {
        let dataset_service = DatasetService::builder().build().await?;
        let (dataset_id, is_temporary) = if dataset_id.is_empty() {
            let suffix: String = rand::rng()
                .sample_iter(&Alphanumeric)
                .take(8)
                .map(char::from)
                .collect();
            (
                format!("rust_bq_json_bench_{}", suffix.to_lowercase()),
                true,
            )
        } else {
            (dataset_id.to_string(), false)
        };

        if is_temporary {
            println!("# Creating temporary dataset {project}.{dataset_id} ...");
            dataset_service
                .insert_dataset()
                .set_project_id(project)
                .set_dataset(
                    Dataset::new()
                        .set_dataset_reference(DatasetReference::new().set_dataset_id(&dataset_id))
                        .set_labels([("bq_benchmark", "true")]),
                )
                .send()
                .await?;
        }

        Ok(Self {
            project: project.to_string(),
            dataset_id,
            is_temporary,
            dataset_service,
        })
    }

    async fn cleanup(self) {
        if self.is_temporary {
            println!(
                "# Cleaning up temporary dataset {}.{} ...",
                self.project, self.dataset_id
            );
            match self
                .dataset_service
                .delete_dataset()
                .set_project_id(&self.project)
                .set_dataset_id(&self.dataset_id)
                .set_delete_contents(true)
                .send()
                .await
            {
                Ok(()) => println!(
                    "# Deleted temporary dataset {}.{}.",
                    self.project, self.dataset_id
                ),
                Err(e) => eprintln!(
                    "Warning: failed to delete temporary dataset {}.{}: {e:?}",
                    self.project, self.dataset_id
                ),
            }
        }
    }
}

async fn create_table_ready(
    table_service: &TableService,
    project: &str,
    dataset_id: &str,
    table_id: &str,
    schema: TableSchema,
) -> Result<()> {
    table_service
        .insert_table()
        .set_project_id(project)
        .set_dataset_id(dataset_id)
        .set_table(
            Table::new()
                .set_table_reference(
                    TableReference::new()
                        .set_project_id(project)
                        .set_dataset_id(dataset_id)
                        .set_table_id(table_id),
                )
                .set_schema(schema),
        )
        .send()
        .await?;

    for _ in 0..15 {
        if table_service
            .get_table()
            .set_project_id(project)
            .set_dataset_id(dataset_id)
            .set_table_id(table_id)
            .set_view(TableMetadataView::Full)
            .send()
            .await
            .is_ok()
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    bail!("table {project}.{dataset_id}.{table_id} did not become ready within timeout")
}

#[allow(clippy::too_many_arguments)]
async fn run_one_round(
    table_resource: &str,
    approach: Approach,
    rows: &[String],
    batch_size: usize,
    pipelined: bool,
    arrow_schema: &(Arc<Schema>, ArrowSchema),
    proto_schema: &ProtoSchema,
    plan: &MessagePlan,
) -> Result<f64> {
    // Fresh Write client per round so every approach gets an isolated gRPC connection & stream.
    let client = Write::builder().build().await?;

    match approach {
        Approach::ArrowJson => {
            let writer = client
                .open_default_stream(table_resource)
                .build_arrow(arrow_schema.1.clone())
                .await?;
            let mut ipc_writer =
                StreamWriter::try_new(Vec::with_capacity(256 * 1024), &arrow_schema.0)?;
            let _ = std::mem::take(ipc_writer.get_mut());
            let mut decoder = ReaderBuilder::new(arrow_schema.0.clone())
                .with_batch_size(batch_size.max(1024))
                .with_strict_mode(true)
                .with_coerce_primitive(true)
                .build_decoder()?;

            // 1-row stream warmup so stream-open latency is excluded from steady-state throughput.
            decoder.decode(rows[0].as_bytes())?;
            if let Some(rb) = decoder.flush()? {
                ipc_writer.write(&rb)?;
                let bytes = std::mem::take(ipc_writer.get_mut());
                writer
                    .append(ArrowRecordBatch::new().set_serialized_record_batch(bytes))
                    .send()
                    .await?;
            }

            let start = Instant::now();
            if pipelined {
                let mut futs = JoinSet::new();
                for chunk in rows.chunks(batch_size) {
                    for r in chunk {
                        decoder.decode(r.as_bytes())?;
                    }
                    if let Some(rb) = decoder.flush()? {
                        ipc_writer.write(&rb)?;
                        let bytes = std::mem::take(ipc_writer.get_mut());
                        let batch = ArrowRecordBatch::new().set_serialized_record_batch(bytes);
                        futs.spawn(writer.append(batch).send());
                    }
                }
                while let Some(res) = futs.join_next().await {
                    res??;
                }
            } else {
                for chunk in rows.chunks(batch_size) {
                    for r in chunk {
                        decoder.decode(r.as_bytes())?;
                    }
                    if let Some(rb) = decoder.flush()? {
                        ipc_writer.write(&rb)?;
                        let bytes = std::mem::take(ipc_writer.get_mut());
                        let batch = ArrowRecordBatch::new().set_serialized_record_batch(bytes);
                        writer.append(batch).send().await?;
                    }
                }
            }
            Ok(rows.len() as f64 / start.elapsed().as_secs_f64())
        }
        Approach::DirectProto => {
            let writer = client
                .open_default_stream(table_resource)
                .build_proto(proto_schema.clone())
                .await?;
            let mut state = DirectState::with_capacity(256);

            let warm = encode_batch_direct(plan, &rows[..1], &mut state)?;
            writer.append(warm).send().await?;

            let start = Instant::now();
            if pipelined {
                let mut futs = JoinSet::new();
                for chunk in rows.chunks(batch_size) {
                    let batch = encode_batch_direct(plan, chunk, &mut state)?;
                    futs.spawn(writer.append(batch).send());
                }
                while let Some(res) = futs.join_next().await {
                    res??;
                }
            } else {
                for chunk in rows.chunks(batch_size) {
                    let batch = encode_batch_direct(plan, chunk, &mut state)?;
                    writer.append(batch).send().await?;
                }
            }
            Ok(rows.len() as f64 / start.elapsed().as_secs_f64())
        }
        Approach::HybridProtoReflect => {
            let writer = client
                .open_default_stream(table_resource)
                .build_proto(proto_schema.clone())
                .await?;
            let mut msg = DynamicMessage::new(plan.msg_desc.clone());

            let warm = encode_batch_hybrid(plan, &rows[..1], &mut msg)?;
            writer.append(warm).send().await?;

            let start = Instant::now();
            if pipelined {
                let mut futs = JoinSet::new();
                for chunk in rows.chunks(batch_size) {
                    let batch = encode_batch_hybrid(plan, chunk, &mut msg)?;
                    futs.spawn(writer.append(batch).send());
                }
                while let Some(res) = futs.join_next().await {
                    res??;
                }
            } else {
                for chunk in rows.chunks(batch_size) {
                    let batch = encode_batch_hybrid(plan, chunk, &mut msg)?;
                    writer.append(batch).send().await?;
                }
            }
            Ok(rows.len() as f64 / start.elapsed().as_secs_f64())
        }
    }
}

async fn run_suite(project: &str, dataset_id: &str, rounds: usize) -> Result<()> {
    let table_service = TableService::builder().build().await?;
    let sanity_tbl = "sanity_verify";
    let bench_tbl = "bench_realistic";

    create_table_ready(
        &table_service,
        project,
        dataset_id,
        sanity_tbl,
        bq_realistic_table_schema(),
    )
    .await?;
    create_table_ready(
        &table_service,
        project,
        dataset_id,
        bench_tbl,
        bq_realistic_table_schema(),
    )
    .await?;

    let sanity_resource = format!("projects/{project}/datasets/{dataset_id}/tables/{sanity_tbl}");
    let bench_resource = format!("projects/{project}/datasets/{dataset_id}/tables/{bench_tbl}");

    let arrow_schema = realistic_arrow_schema();
    let (proto_schema, plan) = realistic_proto_schema_and_plan();

    println!(
        "== Step 1: Live Sanity Write & SQL Readback Verification ({project}.{dataset_id}.{sanity_tbl}) ==\n"
    );
    {
        let client = Write::builder().build().await?;

        // 1. Arrow write (5 rows)
        let arrow_rows = generate_realistic_json_rows(5, Some("arrow_ok"));
        let w_arrow = client
            .open_default_stream(&sanity_resource)
            .build_arrow(arrow_schema.1.clone())
            .await?;
        let mut ipc_writer = StreamWriter::try_new(Vec::new(), &arrow_schema.0)?;
        let _ = std::mem::take(ipc_writer.get_mut());
        let mut decoder = ReaderBuilder::new(arrow_schema.0.clone())
            .with_strict_mode(true)
            .with_coerce_primitive(true)
            .build_decoder()?;
        for r in &arrow_rows {
            decoder.decode(r.as_bytes())?;
        }
        let rb = decoder.flush()?.context("expected arrow batch")?;
        ipc_writer.write(&rb)?;
        let ipc_bytes = std::mem::take(ipc_writer.get_mut());
        w_arrow
            .append(ArrowRecordBatch::new().set_serialized_record_batch(ipc_bytes))
            .send()
            .await
            .context("Arrow live append failed")?;

        // 2. Direct Proto write (5 rows)
        let direct_rows = generate_realistic_json_rows(5, Some("direct_proto_ok"));
        let w_proto = client
            .open_default_stream(&sanity_resource)
            .build_proto(proto_schema.clone())
            .await?;
        let mut state = DirectState::with_capacity(256);
        w_proto
            .append(encode_batch_direct(&plan, &direct_rows, &mut state)?)
            .send()
            .await
            .context("Direct Proto live append failed")?;

        // 3. Hybrid Proto-Reflect write (5 rows)
        let hybrid_rows = generate_realistic_json_rows(5, Some("hybrid_proto_ok"));
        let mut msg = DynamicMessage::new(plan.msg_desc.clone());
        w_proto
            .append(encode_batch_hybrid(&plan, &hybrid_rows, &mut msg)?)
            .send()
            .await
            .context("Hybrid Proto live append failed")?;

        // 4. SQL readback verification
        let bq = BigQuery::builder().with_project_id(project).build().await?;
        let sql = format!(
            "SELECT name, COUNT(*) AS cnt FROM `{project}.{dataset_id}.{sanity_tbl}` GROUP BY name ORDER BY name"
        );
        let mut stream = bq.query(sql).until_done().await?.read();
        let mut verified_groups = 0usize;
        while let Some(row) = stream.next().await.transpose()? {
            let tag: String = row.get("name")?;
            let cnt: i64 = row.get("cnt")?;
            println!("  Verified in BigQuery: tag={tag:<18} rows_stored={cnt}");
            if cnt != 5 {
                bail!("expected 5 stored rows for {tag}, got {cnt}");
            }
            verified_groups += 1;
        }
        if verified_groups != 3 {
            bail!("expected 3 verified groups in SQL readback, got {verified_groups}");
        }
    }
    println!();

    let scenarios = [
        (
            "10 rows / append, pipelined (like json_default.rs)",
            2_000usize,
            10usize,
            true,
        ),
        (
            "500 rows / append, sequential (1 batch in flight)",
            10_000usize,
            500usize,
            false,
        ),
        (
            "500 rows / append, pipelined (concurrent batches)",
            25_000usize,
            500usize,
            true,
        ),
    ];

    for (title, total_rows, batch_size, pipelined) in scenarios {
        println!(
            "== Scenario: {title} ({total_rows} rows/round x {rounds} interleaved rounds) ==\n"
        );
        let rows = generate_realistic_json_rows(total_rows, Some("bench"));
        let mut results: HashMap<&'static str, Vec<f64>> = HashMap::new();

        for round in 0..rounds {
            let order = if round.is_multiple_of(2) {
                [
                    Approach::ArrowJson,
                    Approach::DirectProto,
                    Approach::HybridProtoReflect,
                ]
            } else {
                [
                    Approach::HybridProtoReflect,
                    Approach::DirectProto,
                    Approach::ArrowJson,
                ]
            };
            for app in order {
                let rps = run_one_round(
                    &bench_resource,
                    app,
                    &rows,
                    batch_size,
                    pipelined,
                    &arrow_schema,
                    &proto_schema,
                    &plan,
                )
                .await?;
                results.entry(app.label()).or_default().push(rps);
            }
            println!(
                "  Round {}/{rounds}: arrow={:>8.0} r/s | direct_proto={:>8.0} r/s | hybrid_proto={:>8.0} r/s",
                round + 1,
                results[Approach::ArrowJson.label()][round],
                results[Approach::DirectProto.label()][round],
                results[Approach::HybridProtoReflect.label()][round],
            );
        }

        println!("\n  Summary ({title}):");
        for app in [
            Approach::ArrowJson,
            Approach::DirectProto,
            Approach::HybridProtoReflect,
        ] {
            let v = results.get_mut(app.label()).expect("recorded results");
            v.sort_by(|a, b| a.total_cmp(b));
            let med = v[v.len() / 2];
            println!(
                "    {:<38} median {:>9.0} rows/s   (min {:>8.0}, max {:>8.0})",
                app.label(),
                med,
                v[0],
                v[v.len() - 1]
            );
        }
        println!();
    }

    Ok(())
}

pub async fn run(config: &LiveConfig) -> Result<()> {
    if config.project.is_empty() {
        bail!("GOOGLE_CLOUD_PROJECT environment variable or --project argument must be set");
    }
    if config.rounds == 0 {
        bail!("--rounds must be at least 1");
    }

    let env = BenchmarkDataset::setup(&config.project, &config.dataset_id).await?;
    let project = env.project.clone();
    let dataset_id = env.dataset_id.clone();

    let run_res = tokio::select! {
        res = run_suite(&project, &dataset_id, config.rounds) => res,
        _ = tokio::signal::ctrl_c() => {
            Err(anyhow::anyhow!("interrupted by Ctrl-C; cleaning up temporary dataset"))
        }
    };

    env.cleanup().await;
    run_res
}
