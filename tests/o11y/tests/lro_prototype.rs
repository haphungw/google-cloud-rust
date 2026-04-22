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

use integration_tests_o11y::otlp::trace::Builder;
use opentelemetry::trace::Span;
use opentelemetry::trace::TraceContextExt;
use opentelemetry::trace::{Tracer, TracerProvider as _};
use opentelemetry_sdk::trace::SdkTracerProvider;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn test_lro_real_spans() -> anyhow::Result<()> {
    // 1. Initialize the real Cloud Trace exporter
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")
        .expect("GOOGLE_CLOUD_PROJECT environment variable must be set");

    let provider: SdkTracerProvider = Builder::new(project_id.clone(), "lro-prototype-service")
        .build()
        .await?;

    opentelemetry::global::set_tracer_provider(provider.clone());
    let tracer = provider.tracer("lro-tracer");

    // 2. Simulate the LRO based on our plan
    // T2 Span: Wait Operation
    let parent_ctx = opentelemetry::Context::current();
    let mut parent_span =
        tracer.start_with_context("google.longrunning.Operations.Wait", &parent_ctx);
    parent_span.set_attribute(opentelemetry::KeyValue::new(
        "gcp.lro.name",
        "operations/123",
    ));

    let span_context = parent_span.span_context();
    println!("Created Trace with ID: {}", span_context.trace_id());
    println!(
        "Trace Console Link: https://console.cloud.google.com/traces/list?project={}&tid={}",
        project_id,
        span_context.trace_id()
    );

    let parent_ctx = parent_ctx.with_span(parent_span);

    for i in 1..=2 {
        // T3 Span: Poll Attempt
        let mut poll_span =
            tracer.start_with_context(format!("LRO_Poll_Attempt_{}", i), &parent_ctx);
        let poll_ctx = parent_ctx.clone().with_span(poll_span);

        if i == 1 {
            // Simulate a FAILURE first on the first poll attempt
            let mut grpc_span1 =
                tracer.start_with_context("google.longrunning.Operations/GetOperation", &poll_ctx);
            grpc_span1.set_attribute(opentelemetry::KeyValue::new("rpc.system.name", "grpc"));
            grpc_span1.set_attribute(opentelemetry::KeyValue::new(
                "rpc.method",
                "google.longrunning.Operations/GetOperation",
            ));

            tokio::time::sleep(Duration::from_millis(100)).await;

            // Mark as failed
            grpc_span1.set_attribute(opentelemetry::KeyValue::new(
                "rpc.response.status_code",
                "UNAVAILABLE",
            ));
            grpc_span1.set_status(opentelemetry::trace::Status::error("Network dropped"));
            grpc_span1.end();

            // Wait a bit before retry (simulating immediate retry or short delay)
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Physical Network Call (GetOperation) - The successful one or the only one
        let mut grpc_span =
            tracer.start_with_context("google.longrunning.Operations/GetOperation", &poll_ctx);
        grpc_span.set_attribute(opentelemetry::KeyValue::new("rpc.system.name", "grpc"));
        grpc_span.set_attribute(opentelemetry::KeyValue::new(
            "rpc.method",
            "google.longrunning.Operations/GetOperation",
        ));

        tokio::time::sleep(Duration::from_millis(200)).await;

        grpc_span.set_attribute(opentelemetry::KeyValue::new(
            "rpc.response.status_code",
            "OK",
        ));
        grpc_span.end();

        poll_ctx
            .span()
            .set_attribute(opentelemetry::KeyValue::new("status", "STILL_RUNNING"));
        poll_ctx.span().end();

        // T5 Span: Sleep Span (Sits directly under T2)
        let mut sleep_span = tracer.start_with_context("LRO_Backoff", &parent_ctx);
        tokio::time::sleep(Duration::from_millis(500)).await;
        sleep_span.end();
    }

    // Final completion poll
    let mut poll_span = tracer.start_with_context("LRO_Poll_Attempt_Final", &parent_ctx);
    let poll_ctx = parent_ctx.clone().with_span(poll_span);

    let mut grpc_span =
        tracer.start_with_context("google.longrunning.Operations/GetOperation", &poll_ctx);
    grpc_span.set_attribute(opentelemetry::KeyValue::new("rpc.system.name", "grpc"));
    grpc_span.set_attribute(opentelemetry::KeyValue::new(
        "rpc.method",
        "google.longrunning.Operations/GetOperation",
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;
    grpc_span.set_attribute(opentelemetry::KeyValue::new(
        "rpc.response.status_code",
        "OK",
    ));
    grpc_span.end();

    poll_ctx
        .span()
        .set_attribute(opentelemetry::KeyValue::new("status", "DONE"));
    poll_ctx.span().end();

    // End Parent Span
    parent_ctx.span().end();

    // Force flush to ensure spans are sent
    let _ = provider.force_flush();

    println!("Spans should be sent to Cloud Trace!");
    Ok(())
}
