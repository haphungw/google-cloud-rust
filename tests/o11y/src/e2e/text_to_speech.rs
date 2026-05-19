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

use super::{new_credentials, set_up_providers, wait_for_trace};
use google_cloud_lro::Poller;
use google_cloud_texttospeech_v1::client::TextToSpeechLongAudioSynthesize;
use google_cloud_texttospeech_v1::model::{
    AudioConfig, AudioEncoding, SynthesisInput, VoiceSelectionParams,
};
use opentelemetry::trace::TraceContextExt;
use std::collections::BTreeSet;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

const ROOT_SPAN_NAME: &str = "e2e-text-to-speech-test";

pub async fn run() -> anyhow::Result<()> {
    // 1. Setup GCS bucket for synthesis output
    let (control, bucket) = storage_samples::create_test_bucket().await?;
    let bucket_name = bucket.bucket_id.clone();

    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")
        .expect("GOOGLE_CLOUD_PROJECT environment variable must be set");

    // 2. Setup Telemetry
    let id = Uuid::new_v4();
    let credentials = new_credentials(&project_id).await?;
    let (provider, _meter_provider, _) = set_up_providers(
        &project_id,
        ROOT_SPAN_NAME,
        id.to_string(),
        credentials.clone(),
    )
    .await?;

    // 3. Create parent span for test execution
    let parent_span = tracing::info_span!("e2e_root", "otel.name" = ROOT_SPAN_NAME);
    let span_context = parent_span.context().span().span_context().clone();
    println!("Created Trace with ID: {}", span_context.trace_id());
    println!(
        "Trace Console Link: https://console.cloud.google.com/traces/list?project={}&tid={}",
        project_id,
        span_context.trace_id()
    );

    // 4. Run synthesis LRO inside parent span context
    let result = async {
        let client = TextToSpeechLongAudioSynthesize::builder()
            .with_tracing()
            .build()
            .await?;

        let input = SynthesisInput::new().set_text(
            "Observability is the ability to measure the internal states of a system \
             based on its external outputs. This integration test verifies that long \
             running operations propagate tracing spans cleanly.",
        );
        let audio_config = AudioConfig::new()
            .set_audio_encoding(AudioEncoding::Linear16)
            .set_sample_rate_hertz(24000);
        let voice = VoiceSelectionParams::new()
            .set_language_code("en-US")
            .set_name("en-US-Journey-F");

        println!(
            "Synthesizing long audio LRO to gs://{}/output.wav",
            bucket_name
        );

        // Create poller inside parent span scope
        let poller = parent_span.in_scope(|| {
            client
                .synthesize_long_audio()
                .set_parent(format!("projects/{}/locations/us-central1", project_id))
                .set_input(input)
                .set_audio_config(audio_config)
                .set_output_gcs_uri(format!("gs://{}/output.wav", bucket_name))
                .set_voice(voice)
                .poller()
        });

        let response = poller.until_done().await?;

        println!("Audio LRO synthesis complete: {:?}", response);
        anyhow::Ok(())
    }
    .instrument(parent_span.clone())
    .await;

    // 5. Clean up GCS bucket
    if let Err(e) = storage_samples::cleanup_bucket(control, bucket.name.clone(), String::new()).await {
        println!("error cleaning up test bucket {}: {:?}", bucket.name, e);
    }

    // Explicitly drop parent span
    drop(parent_span);

    // Force flush spans
    let _ = provider.force_flush();

    // Propagate synthesis result
    result?;

    let required = BTreeSet::from_iter([
        ROOT_SPAN_NAME,
        "google_cloud_texttospeech_v1::client::TextToSpeechLongAudioSynthesize::synthesize_long_audio::until_done",
        "google_cloud_texttospeech_v1::client::TextToSpeechLongAudioSynthesize::get_operation",
    ]);
    let trace =
        wait_for_trace(&project_id, &span_context.trace_id().to_string(), &required).await?;

    // Verify programmatic parent-child relationship
    let root_span_data = trace
        .spans
        .iter()
        .find(|s| s.name == ROOT_SPAN_NAME)
        .unwrap();
    let t2_span = trace
        .spans
        .iter()
        .find(|s| s.name == "google_cloud_texttospeech_v1::client::TextToSpeechLongAudioSynthesize::synthesize_long_audio::until_done")
        .unwrap();
    let t3_span = trace
        .spans
        .iter()
        .find(|s| s.name == "google_cloud_texttospeech_v1::client::TextToSpeechLongAudioSynthesize::get_operation")
        .unwrap();

    assert_eq!(t2_span.parent_span_id, root_span_data.span_id);
    assert_eq!(t3_span.parent_span_id, t2_span.span_id);

    Ok(())
}
