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

// [START bigquery_get_job]
use google_cloud_bigquery_v2::client::JobService;

pub async fn sample(project_id: &str, job_id: &str) -> anyhow::Result<()> {
    let client = JobService::builder().build().await?;

    let job = client
        .get_job()
        .set_project_id(project_id)
        .set_job_id(job_id)
        .send()
        .await?;

    if let Some(status) = &job.status {
        println!("Job state: {}", status.state);
    }
    if let Some(stats) = &job.statistics {
        println!("Job creation time: {:?}", stats.creation_time);
        if let Some(query_stats) = &stats.query {
            println!(
                "Total bytes processed: {:?}",
                query_stats.total_bytes_processed
            );
        }
    }
    Ok(())
}
// [END bigquery_get_job]
