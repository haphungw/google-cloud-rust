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

// [START bigquery_copy_table_multiple_source]
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{
    Job, JobConfiguration, JobConfigurationTableCopy, TableReference,
};

pub async fn sample(project_id: &str, dataset_id: &str) -> anyhow::Result<()> {
    let job_service = JobService::builder().build().await?;

    let source_table_1 = TableReference::new()
        .set_project_id("bigquery-public-data")
        .set_dataset_id("usa_names")
        .set_table_id("usa_1910_2013");

    let source_table_2 = TableReference::new()
        .set_project_id("bigquery-public-data")
        .set_dataset_id("usa_names")
        .set_table_id("usa_1910_current");

    let dest_table = TableReference::new()
        .set_project_id(project_id)
        .set_dataset_id(dataset_id)
        .set_table_id("destination_table_multiple_source");

    let job = Job::new().set_configuration(
        JobConfiguration::new().set_copy(
            JobConfigurationTableCopy::new()
                .set_source_tables(vec![source_table_1, source_table_2])
                .set_destination_table(dest_table),
        ),
    );

    let job = job_service
        .insert_job()
        .set_project_id(project_id)
        .set_job(job)
        .into_job_poller()
        .until_done()
        .await?;
    let job_id = job.job_reference.unwrap().job_id;
    println!("Job completed successfully: {}", job_id);

    Ok(())
}
// [END bigquery_copy_table_multiple_source]
