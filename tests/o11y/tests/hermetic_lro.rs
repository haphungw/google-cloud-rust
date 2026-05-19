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

#[cfg(google_cloud_unstable_tracing)]
mod hermetic_lro {
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_lro::Poller;
    use google_cloud_showcase_v1beta1::client::Echo;
    use httptest::{Expectation, Server, matchers::*, responders::status_code};
    use integration_tests_o11y::tracing::trace_layer;
    use opentelemetry_sdk::error::OTelSdkError;
    use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};

    use std::sync::{Arc, Mutex};
    use tracing::Instrument;
    use tracing_subscriber::{Registry, layer::SubscriberExt};

    #[derive(Debug, Clone)]
    struct InMemorySpanExporter {
        spans: Arc<Mutex<Vec<SpanData>>>,
    }

    impl InMemorySpanExporter {
        fn new() -> Self {
            Self {
                spans: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl SpanExporter for InMemorySpanExporter {
        fn export(
            &self,
            batch: Vec<SpanData>,
        ) -> impl futures::Future<Output = Result<(), OTelSdkError>> + Send {
            let spans = self.spans.clone();
            async move {
                spans.lock().unwrap().extend(batch);
                Ok(())
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_hermetic_lro() -> anyhow::Result<()> {
        // 1. Setup Local HTTP Mock Server
        let echo_server = Server::run();

        echo_server.expect(
            Expectation::matching(all_of![
                request::method("POST"),
                request::path("/v1beta1/echo:wait"),
            ])
            .respond_with(
                status_code(200).body(
                    serde_json::json!({
                        "name": "operations/test-op-123",
                        "done": false
                    })
                    .to_string(),
                ),
            ),
        );

        echo_server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path("/v1beta1/operations/test-op-123"),
            ])
            .respond_with(
                status_code(200).body(
                    serde_json::json!({
                        "name": "operations/test-op-123",
                        "done": true,
                        "response": {
                            "@type": "type.googleapis.com/google.showcase.v1beta1.WaitResponse",
                            "content": "lro-test-success"
                        }
                    })
                    .to_string(),
                ),
            ),
        );

        // 2. Setup Local In-Memory Telemetry
        let exporter = InMemorySpanExporter::new();
        let captured_spans = exporter.spans.clone();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter)
            .build();

        let layer = trace_layer(provider.clone());
        let subscriber = Registry::default().with(layer);

        // Execute Showcase client completely isolated in thread-local tracing context
        let _guard = tracing::subscriber::set_default(subscriber);

        let client = Echo::builder()
            .with_endpoint(format!("http://{}", echo_server.addr()))
            .with_credentials(Anonymous::new().build())
            .with_tracing()
            .build()
            .await?;

        let root_span = tracing::info_span!("e2e_root", "otel.name" = "e2e-hermetic-lro-test");

        let result = async {
            let poller = root_span.in_scope(|| {
                client
                    .wait()
                    .set_ttl(std::boxed::Box::new(google_cloud_wkt::Duration::clamp(
                        1, 0,
                    )))
                    .poller()
            });

            let response = poller.until_done().await?;

            assert_eq!(response.content, "lro-test-success");
            anyhow::Ok(())
        }
        .instrument(root_span.clone())
        .await;

        drop(root_span);

        // Force flush in-memory spans
        provider.force_flush()?;

        // Propagate execution result
        result?;

        // 3. Verify span hierarchy locally
        let spans = captured_spans.lock().unwrap();
        println!("spans = {:#?}", spans);

        let root_span = spans
            .iter()
            .find(|s| s.name == "e2e-hermetic-lro-test")
            .expect("missing root span");
        let t2_span = spans
            .iter()
            .find(|s| {
                s.name == "google_cloud_showcase_v1beta1::client::Echo::wait::until_done"
                    && s.attributes
                        .iter()
                        .any(|kv| kv.key.as_str() == "gcp.longrunning.operation_name")
            })
            .expect("missing T2 LRO span");
        let t3_span = spans
            .iter()
            .find(|s| s.name == "google_cloud_showcase_v1beta1::client::Echo::get_operation")
            .expect("missing T3 poll span");
        let initial_rpc_span = spans
            .iter()
            .find(|s| s.name == "google_cloud_showcase_v1beta1::client::Echo::wait")
            .expect("missing initial RPC span");

        // Verify parents (Option 2: Parent-Child Nesting, clean without stub spans)
        // T2 parent is Root span
        assert_eq!(t2_span.parent_span_id, root_span.span_context.span_id());
        // Initial RPC span parent is T2 LRO span
        assert_eq!(initial_rpc_span.parent_span_id, t2_span.span_context.span_id());
        // T3 parent is T2 LRO span
        assert_eq!(t3_span.parent_span_id, t2_span.span_context.span_id());

        // Verify LRO Sleep span exists (since sleep suppression is removed)
        let sleep_span = spans
            .iter()
            .find(|s| s.name == "LRO Sleep")
            .expect("missing LRO Sleep span");
        assert_eq!(sleep_span.parent_span_id, t2_span.span_context.span_id());

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_hermetic_lro_immediate() -> anyhow::Result<()> {
        // 1. Setup Local HTTP Mock Server
        let echo_server = Server::run();

        // Initial wait call: starts the LRO, done = true!
        echo_server.expect(
            Expectation::matching(all_of![
                request::method("POST"),
                request::path("/v1beta1/echo:wait"),
            ])
            .respond_with(
                status_code(200).body(
                    serde_json::json!({
                        "name": "operations/test-op-123",
                        "done": true,
                        "response": {
                            "@type": "type.googleapis.com/google.showcase.v1beta1.WaitResponse",
                            "content": "lro-immediate-success"
                        }
                    })
                    .to_string(),
                ),
            ),
        );

        // 2. Setup Local In-Memory Telemetry
        let exporter = InMemorySpanExporter::new();
        let captured_spans = exporter.spans.clone();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter)
            .build();

        let layer = trace_layer(provider.clone());
        let subscriber = Registry::default().with(layer);

        let _guard = tracing::subscriber::set_default(subscriber);

        let client = Echo::builder()
            .with_endpoint(format!("http://{}", echo_server.addr()))
            .with_credentials(Anonymous::new().build())
            .with_tracing()
            .build()
            .await?;

        let root_span =
            tracing::info_span!("e2e_root", "otel.name" = "e2e-hermetic-lro-immediate-test");

        let result = async {
            let poller = root_span.in_scope(|| {
                client
                    .wait()
                    .set_ttl(std::boxed::Box::new(google_cloud_wkt::Duration::clamp(
                        1, 0,
                    )))
                    .poller()
            });

            let response = poller.until_done().await?;

            assert_eq!(response.content, "lro-immediate-success");
            anyhow::Ok(())
        }
        .instrument(root_span.clone())
        .await;

        drop(root_span);

        provider.force_flush()?;
        result?;

        // 3. Verify span hierarchy locally
        let spans = captured_spans.lock().unwrap();
        println!("immediate spans = {:#?}", spans);

        let root_span = spans
            .iter()
            .find(|s| s.name == "e2e-hermetic-lro-immediate-test")
            .expect("missing root span");
        let t2_span = spans
            .iter()
            .find(|s| s.name == "google_cloud_showcase_v1beta1::client::Echo::wait::until_done")
            .expect("missing T2 LRO span");

        // Verify parent-child nesting
        assert_eq!(t2_span.parent_span_id, root_span.span_context.span_id());

        // Verify NO LRO Sleep and NO get_operation spans exist!
        assert!(
            !spans.iter().any(|s| s.name == "LRO Sleep"),
            "should not have LRO Sleep span"
        );
        assert!(
            !spans.iter().any(|s| s.name == "get_operation"),
            "should not have get_operation span"
        );

        Ok(())
    }
}
