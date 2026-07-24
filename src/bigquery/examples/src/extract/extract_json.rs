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

// [START bigquery_extract_table_json]
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{
    Job, JobConfiguration, JobConfigurationExtract, TableReference,
};
use google_cloud_lro::Poller;

pub async fn sample(
    project_id: &str,
    dataset_id: &str,
    table_id: &str,
    gcs_uri: &str,
) -> anyhow::Result<()> {
    let client = JobService::builder().build().await?;

    let src_ref = TableReference::new()
        .set_project_id(project_id)
        .set_dataset_id(dataset_id)
        .set_table_id(table_id);

    let extract_config = JobConfigurationExtract::new()
        .set_source_table(src_ref)
        .set_destination_uris(vec![gcs_uri.to_string()])
        .set_destination_format("NEWLINE_DELIMITED_JSON");

    let job_config = JobConfiguration::new().set_extract(extract_config);
    let job = Job::new().set_configuration(job_config);

    let completed_job = client
        .insert_job()
        .set_project_id(project_id)
        .set_job(job)
        .into_job_poller()
        .until_done()
        .await?;

    if let Some(job_ref) = completed_job.job_reference {
        println!(
            "Extract JSON job completed successfully: {}",
            job_ref.job_id
        );
    }
    Ok(())
}
// [END bigquery_extract_table_json]
