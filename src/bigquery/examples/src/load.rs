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

mod add_column_append;
mod load_from_file;
mod load_from_gcs;
mod relax_column_append;

use google_cloud_bigquery_v2::client::DatasetService;
use google_cloud_bigquery_v2::model::{Dataset, DatasetReference};
use google_cloud_test_utils::runtime_config::project_id;
use rand::{RngExt, distr::Alphanumeric};

fn random_id_suffix() -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect()
}

pub async fn run_samples_with_resources() -> anyhow::Result<()> {
    let project_id = project_id()?;
    let dataset_service = DatasetService::builder().build().await?;
    let dataset_id = format!("rust_bq_load_{}", random_id_suffix());

    dataset_service
        .insert_dataset()
        .set_project_id(&project_id)
        .set_dataset(
            Dataset::new()
                .set_dataset_reference(DatasetReference::new().set_dataset_id(&dataset_id)),
        )
        .send()
        .await?;

    let t1 = format!("tbl1_{}", random_id_suffix());
    let t2 = format!("tbl2_{}", random_id_suffix());
    let t3 = format!("tbl3_{}", random_id_suffix());
    let t4 = format!("tbl4_{}", random_id_suffix());

    let res = async {
        load_from_gcs::sample(&project_id, &dataset_id, &t1).await?;
        load_from_file::sample(&project_id, &dataset_id, &t2).await?;
        add_column_append::sample(&project_id, &dataset_id, &t3).await?;
        relax_column_append::sample(&project_id, &dataset_id, &t4).await?;
        Ok(())
    }
    .await;

    let _ = dataset_service
        .delete_dataset()
        .set_project_id(&project_id)
        .set_dataset_id(&dataset_id)
        .set_delete_contents(true)
        .send()
        .await;

    res
}
