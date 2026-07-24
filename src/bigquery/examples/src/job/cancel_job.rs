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

// [START bigquery_cancel_job]
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{Job, JobConfiguration, JobConfigurationQuery, JobReference};

pub async fn sample(project_id: &str) -> anyhow::Result<()> {
    let client = JobService::builder().build().await?;

    let job_id = format!("cancel_job_{}", uuid::Uuid::new_v4());
    let query_config = JobConfigurationQuery::new()
        .set_query("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 1000000000))");

    let job_config = JobConfiguration::new().set_query(query_config);
    let job = Job::new()
        .set_job_reference(
            JobReference::new()
                .set_project_id(project_id)
                .set_job_id(&job_id),
        )
        .set_configuration(job_config);

    // Submit long running job without awaiting poller
    let _ = client
        .insert_job()
        .set_project_id(project_id)
        .set_job(job)
        .send()
        .await?;

    println!("Submitted long running job `{job_id}`, requesting cancellation...");

    let cancel_res = client
        .cancel_job()
        .set_project_id(project_id)
        .set_job_id(&job_id)
        .send()
        .await?;

    if let Some(status) = cancel_res.job.and_then(|j| j.status) {
        println!(
            "Job cancellation requested. Current state: {}",
            status.state
        );
    }

    Ok(())
}
// [END bigquery_cancel_job]
