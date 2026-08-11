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

// [START bigquery_relax_column_load_append]
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{
    Job, JobConfiguration, JobConfigurationLoad, TableFieldSchema, TableReference, TableSchema,
};

pub async fn sample(project_id: &str, dataset_id: &str) -> anyhow::Result<()> {
    let job_service = JobService::builder().build().await?;

    let dest_table = TableReference::new()
        .set_project_id(project_id)
        .set_dataset_id(dataset_id)
        .set_table_id("us_states_schema_evolve2");

    let source_uri = "gs://cloud-samples-data/bigquery/us-states/us-states.csv";

    // Changing the mode of post_abbr from REQUIRED to NULLABLE
    let schema = TableSchema::new().set_fields(vec![
        TableFieldSchema::new()
            .set_name("name")
            .set_type("STRING")
            .set_mode("REQUIRED"),
        TableFieldSchema::new()
            .set_name("post_abbr")
            .set_type("STRING")
            .set_mode("NULLABLE"),
    ]);

    let job = Job::new().set_configuration(
        JobConfiguration::new().set_load(
            JobConfigurationLoad::new()
                .set_source_uris(vec![source_uri.to_string()])
                .set_destination_table(dest_table)
                .set_source_format("CSV".to_string())
                .set_skip_leading_rows(1)
                .set_write_disposition("WRITE_APPEND".to_string())
                // Use schema_update_options to allow relaxing fields
                .set_schema_update_options(vec!["ALLOW_FIELD_RELAXATION".to_string()])
                .set_schema(schema),
        ),
    );

    let inserted = job_service
        .insert_job()
        .set_project_id(project_id)
        .set_job(job)
        .send()
        .await?;

    let job_ref = inserted.job_reference.unwrap();
    println!("Created schema relaxation load job: {}", job_ref.job_id);

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

    println!("Data loaded successfully with schema relaxation.");
    Ok(())
}
// [END bigquery_relax_column_load_append]
