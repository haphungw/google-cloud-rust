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

// [START bigquery_list_jobs]
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_gax::paginator::Paginator;

pub async fn sample(project_id: &str) -> anyhow::Result<()> {
    let client = JobService::builder().build().await?;

    let mut paginator = client
        .list_jobs()
        .set_project_id(project_id)
        .set_max_results(10)
        .by_page();

    let mut count = 0;
    while let Some(page_result) = paginator.next().await {
        let page = page_result?;
        for job_item in page.jobs {
            if let Some(job_ref) = job_item.job_reference {
                println!("Found job ID: {}", job_ref.job_id);
                count += 1;
            }
        }
    }

    println!("Total jobs listed: {count}");
    Ok(())
}
// [END bigquery_list_jobs]
