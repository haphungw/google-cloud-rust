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

use super::base::BaseWriter;
use crate::Result;
use crate::model::{FinalizeWriteStreamResponse, ProtoRows, ProtoSchema};
use crate::write::append_builder::AppendWithOffset;
use crate::write::transport::Transport;
use std::sync::Arc;

/// A writer for a [committed stream].
///
/// [committed stream]: https://docs.cloud.google.com/bigquery/docs/write-api-grpc#committed_type
#[derive(Debug)]
pub struct CommittedWriter {
    pub(crate) inner: BaseWriter,
}

impl CommittedWriter {
    pub(crate) fn new(inner: Arc<Transport>, write_stream: String, schema: ProtoSchema) -> Self {
        Self {
            inner: BaseWriter::new(inner, write_stream, schema),
        }
    }

    /// Return the full resource name of the underlying write stream.
    pub fn write_stream(&self) -> &str {
        &self.inner.write_stream
    }

    /// Append rows to the committed stream.
    pub fn append(&self, rows: ProtoRows) -> AppendWithOffset {
        AppendWithOffset::new(
            self.inner.runner.req_tx.clone(),
            self.inner.append_request(rows),
        )
    }

    /// Finalize the stream, preventing further writes.
    pub async fn finalize(&self) -> Result<FinalizeWriteStreamResponse> {
        self.inner.finalize().await
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::runner::tests::*;
    use super::super::super::transport::tests::*;
    use super::*;
    use crate::error::AppendError;
    use bigquery_grpc_mock::{MockBigQueryWrite, start};
    use gaxi::grpc::tonic::Response as TonicResponse;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn request_fields() -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1".to_string()).await?);
        let writer = CommittedWriter::new(transport, write_stream(), schema());
        assert_eq!(writer.write_stream(), write_stream());

        let b = writer.append(rows(1));
        assert_eq!(b.req.write_stream, write_stream());
        let data = b.req.proto_rows().expect("proto rows should be set");
        let s = data.writer_schema.as_ref().expect("schema should be set");
        assert_eq!(
            s.proto_descriptor.as_ref().unwrap().name,
            "test"
        );
        let r = data.rows.as_ref().expect("rows should be set");
        assert_eq!(
            String::from_utf8(r.serialized_rows[0].to_vec()).unwrap(),
            "1"
        );

        let b = writer.append(rows(2));
        assert_eq!(b.req.write_stream, write_stream());
        let data = b.req.proto_rows().expect("proto rows should be set");
        let s = data.writer_schema.as_ref().expect("schema should be set");
        assert_eq!(
            s.proto_descriptor.as_ref().unwrap().name,
            "test"
        );
        let r = data.rows.as_ref().expect("rows should be set");
        assert_eq!(
            String::from_utf8(r.serialized_rows[0].to_vec()).unwrap(),
            "2"
        );

        Ok(())
    }

    #[tokio::test]
    async fn basic_success() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);

        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));

        mock.expect_finalize_write_stream().return_once(|_| {
            Ok(TonicResponse::new(
                bigquery_grpc_mock::google::cloud::bigquery::storage::v1::FinalizeWriteStreamResponse::default(),
            ))
        });

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);

        let writer = CommittedWriter::new(transport, write_stream(), schema());
        assert_eq!(writer.write_stream(), write_stream());

        response_tx.send(Ok(convert(&test_response(1)))).await?;
        let resp = writer.append(rows(1)).send().await?;
        assert_eq!(resp.offset, Some(1));

        response_tx.send(Ok(convert(&test_response(2)))).await?;
        let resp = writer.append(rows(2)).send().await?;
        assert_eq!(resp.offset, Some(2));

        response_tx.send(Ok(convert(&test_response(3)))).await?;
        let resp = writer.append(rows(3)).send().await?;
        assert_eq!(resp.offset, Some(3));

        drop(response_tx);
        let err = writer.append(rows(4)).send().await.expect_err("channel");
        assert!(matches!(err, AppendError::UnexpectedEndOfStream));

        // We can still finalize the stream even if row appends hit a closed bidirectional stream
        writer.finalize().await?;

        Ok(())
    }

    fn write_stream() -> String {
        "projects/p/datasets/d/tables/t/streams/s".to_string()
    }

    fn schema() -> ProtoSchema {
        ProtoSchema::new().set_proto_descriptor({
            let mut d = wkt::DescriptorProto::default();
            d.name = "test".to_string();
            d
        })
    }

    fn rows(id: i64) -> ProtoRows {
        ProtoRows::new().set_serialized_rows(vec![::bytes::Bytes::from(id.to_string())])
    }
}
