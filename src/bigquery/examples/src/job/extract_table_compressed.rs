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

// [START bigquery_extract_table_compressed]
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{
    Job, JobConfiguration, JobConfigurationExtract, TableReference,
    job_configuration_extract::Source,
};

pub async fn sample(
    project_id: &str,
    dataset_id: &str,
    table_id: &str,
    bucket_name: &str,
) -> anyhow::Result<()> {
    let job_service = JobService::builder().build().await?;

    let source_table = TableReference::new()
        .set_project_id(project_id)
        .set_dataset_id(dataset_id)
        .set_table_id(table_id);

    let destination_uri = format!("gs://{}/extract_*.csv.gz", bucket_name);

    let job = Job::new().set_configuration(
        JobConfiguration::new().set_extract(
            JobConfigurationExtract::new()
                .set_source(Source::SourceTable(Box::new(source_table)))
                .set_destination_uris(vec![destination_uri])
                .set_destination_format("CSV".to_string())
                .set_compression("GZIP".to_string()),
        ),
    );

    let inserted = job_service
        .insert_job()
        .set_project_id(project_id)
        .set_job(job)
        .send()
        .await?;

    let job_ref = inserted.job_reference.unwrap();
    println!("Created compressed extract job: {}", job_ref.job_id);

    // Wait for the job to complete
    loop {
        let current_job = job_service
            .get_job()
            .set_project_id(project_id)
            .set_job_id(&job_ref.job_id)
            .send()
            .await?;
        if current_job.status.unwrap().state == "DONE" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }

    println!("Table extracted to compressed GCS file.");
    Ok(())
}
// [END bigquery_extract_table_compressed]
