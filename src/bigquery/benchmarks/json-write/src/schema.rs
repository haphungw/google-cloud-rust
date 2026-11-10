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

use crate::encoder::{ColumnKind, ColumnPlan, MessagePlan};
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use google_cloud_bigquery::model::{ArrowSchema, ProtoSchema};
use google_cloud_bigquery_v2::model::{TableFieldSchema, TableSchema};
use google_cloud_wkt as wkt;
use prost_reflect::DescriptorPool;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet};
use std::collections::HashMap;
use std::sync::Arc;

const RNG_SEED: u64 = 0x9E37_79B9_7F4A_7C15;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn letters(&mut self, n: usize) -> String {
        (0..n)
            .map(|_| (b'a' + (self.next() % 26) as u8) as char)
            .collect()
    }
}

fn proto_field(name: &str, number: i32, label: Label, ty: Type) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.into()),
        json_name: Some(name.into()),
        number: Some(number),
        label: Some(label as i32),
        r#type: Some(ty as i32),
        ..Default::default()
    }
}

fn wkt_field(
    name: &str,
    num: i32,
    label: wkt::field_descriptor_proto::Label,
    ty: wkt::field_descriptor_proto::Type,
) -> wkt::FieldDescriptorProto {
    wkt::FieldDescriptorProto::new()
        .set_name(name)
        .set_json_name(name)
        .set_number(num)
        .set_label(label)
        .set_type(ty)
}

/// 2-column Arrow schema from `src/bigquery/examples/src/write/json_default.rs`.
pub fn simple_arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("string", DataType::Utf8, false),
        Field::new("int", DataType::Int64, false),
    ]))
}

/// 2-column `MessagePlan` matching `json_default.rs` (`string`: `STRING`, `int`: `INT64`).
pub fn simple_message_plan() -> MessagePlan {
    let file = FileDescriptorProto {
        name: Some("simple.proto".into()),
        syntax: Some("proto2".into()),
        message_type: vec![DescriptorProto {
            name: Some("root".into()),
            field: vec![
                proto_field("string", 1, Label::Optional, Type::String),
                proto_field("int", 2, Label::Optional, Type::Int64),
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    let pool = DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: vec![file] })
        .expect("valid simple descriptor");
    let root = pool
        .get_message_by_name("root")
        .expect("root message exists");
    let columns = vec![
        ColumnPlan {
            name: "string".into(),
            tag: (1 << 3) | 2,
            repeated: false,
            kind: ColumnKind::String,
            field_desc: root.get_field(1).expect("field 1"),
        },
        ColumnPlan {
            name: "int".into(),
            tag: (2 << 3),
            repeated: false,
            kind: ColumnKind::Int64,
            field_desc: root.get_field(2).expect("field 2"),
        },
    ]
    .into_boxed_slice();
    let by_lower = columns
        .iter()
        .enumerate()
        .map(|(i, c)| (c.name.to_ascii_lowercase().into_boxed_str(), i))
        .collect();
    MessagePlan {
        columns,
        by_lower,
        msg_desc: root,
    }
}

/// Deterministic JSON rows for the 2-column `json_default.rs` schema.
pub fn generate_simple_json_rows(n: usize) -> Vec<String> {
    (0..n)
        .map(|i| format!(r#"{{"string": "batch {}", "int": {}}}"#, i / 10, i))
        .collect()
}

/// 11-column realistic BigQuery `TableSchema` for live table creation.
pub fn bq_realistic_table_schema() -> TableSchema {
    TableSchema::new().set_fields([
        TableFieldSchema::new().set_name("id").set_type("INT64"),
        TableFieldSchema::new().set_name("name").set_type("STRING"),
        TableFieldSchema::new().set_name("flag").set_type("BOOL"),
        TableFieldSchema::new()
            .set_name("amount")
            .set_type("NUMERIC"),
        TableFieldSchema::new()
            .set_name("created")
            .set_type("TIMESTAMP"),
        TableFieldSchema::new().set_name("d").set_type("DATE"),
        TableFieldSchema::new().set_name("dt").set_type("DATETIME"),
        TableFieldSchema::new().set_name("meta").set_type("JSON"),
        TableFieldSchema::new()
            .set_name("tags")
            .set_type("STRING")
            .set_mode("REPEATED"),
        TableFieldSchema::new()
            .set_name("payload")
            .set_type("STRING"),
        TableFieldSchema::new()
            .set_name("addr")
            .set_type("STRUCT")
            .set_fields([
                TableFieldSchema::new().set_name("city").set_type("STRING"),
                TableFieldSchema::new().set_name("zip").set_type("STRING"),
            ]),
    ])
}

/// 11-column realistic Arrow `Schema` + serialized `ArrowSchema` (with BigQuery extension metadata).
pub fn realistic_arrow_schema() -> (Arc<Schema>, ArrowSchema) {
    let dt_meta: HashMap<String, String> = [(
        "ARROW:extension:name".to_string(),
        "google:sqlType:datetime".to_string(),
    )]
    .into_iter()
    .collect();
    let json_meta: HashMap<String, String> = [(
        "ARROW:extension:name".to_string(),
        "google:sqlType:json".to_string(),
    )]
    .into_iter()
    .collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("flag", DataType::Boolean, true),
        Field::new("amount", DataType::Decimal128(38, 9), true),
        Field::new(
            "created",
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            true,
        ),
        Field::new("d", DataType::Date32, true),
        Field::new("dt", DataType::Timestamp(TimeUnit::Microsecond, None), true)
            .with_metadata(dt_meta),
        Field::new("meta", DataType::Utf8, true).with_metadata(json_meta),
        Field::new_list("tags", Field::new("item", DataType::Utf8, false), true),
        Field::new("payload", DataType::Utf8, true),
        Field::new(
            "addr",
            DataType::Struct(Fields::from(vec![
                Field::new("city", DataType::Utf8, true),
                Field::new("zip", DataType::Utf8, true),
            ])),
            true,
        ),
    ]));
    let mut ipc_writer = StreamWriter::try_new(Vec::new(), &schema).expect("arrow ipc schema");
    let buf = std::mem::take(ipc_writer.get_mut());
    (schema, ArrowSchema::new().set_serialized_schema(buf))
}

/// 11-column realistic `ProtoSchema` + `MessagePlan` (binary temporals, string `NUMERIC`/`JSON`).
pub fn realistic_proto_schema_and_plan() -> (ProtoSchema, MessagePlan) {
    use wkt::field_descriptor_proto::{Label as WLabel, Type as WType};
    let addr_nested = wkt::DescriptorProto::new().set_name("Addr").set_field([
        wkt_field("city", 1, WLabel::Optional, WType::String),
        wkt_field("zip", 2, WLabel::Optional, WType::String),
    ]);
    let wkt_desc = wkt::DescriptorProto::new()
        .set_name("root")
        .set_field([
            wkt_field("id", 1, WLabel::Optional, WType::Int64),
            wkt_field("name", 2, WLabel::Optional, WType::String),
            wkt_field("flag", 3, WLabel::Optional, WType::Bool),
            wkt_field("amount", 4, WLabel::Optional, WType::String),
            wkt_field("created", 5, WLabel::Optional, WType::Int64),
            wkt_field("d", 6, WLabel::Optional, WType::Int32),
            wkt_field("dt", 7, WLabel::Optional, WType::Int64),
            wkt_field("meta", 8, WLabel::Optional, WType::String),
            wkt_field("tags", 9, WLabel::Repeated, WType::String),
            wkt_field("payload", 10, WLabel::Optional, WType::String),
            wkt_field("addr", 11, WLabel::Optional, WType::Message).set_type_name("Addr"),
        ])
        .set_nested_type([addr_nested]);
    let proto_schema = ProtoSchema::new().set_proto_descriptor(wkt_desc);

    let mut addr_f = proto_field("addr", 11, Label::Optional, Type::Message);
    addr_f.type_name = Some("Addr".into());
    let file = FileDescriptorProto {
        name: Some("table.proto".into()),
        syntax: Some("proto2".into()),
        message_type: vec![DescriptorProto {
            name: Some("root".into()),
            field: vec![
                proto_field("id", 1, Label::Optional, Type::Int64),
                proto_field("name", 2, Label::Optional, Type::String),
                proto_field("flag", 3, Label::Optional, Type::Bool),
                proto_field("amount", 4, Label::Optional, Type::String),
                proto_field("created", 5, Label::Optional, Type::Int64),
                proto_field("d", 6, Label::Optional, Type::Int32),
                proto_field("dt", 7, Label::Optional, Type::Int64),
                proto_field("meta", 8, Label::Optional, Type::String),
                proto_field("tags", 9, Label::Repeated, Type::String),
                proto_field("payload", 10, Label::Optional, Type::String),
                addr_f,
            ],
            nested_type: vec![DescriptorProto {
                name: Some("Addr".into()),
                field: vec![
                    proto_field("city", 1, Label::Optional, Type::String),
                    proto_field("zip", 2, Label::Optional, Type::String),
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let pool = DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: vec![file] })
        .expect("valid realistic descriptor");
    let root = pool
        .get_message_by_name("root")
        .expect("root message exists");
    let addr_desc = root
        .get_field(11)
        .expect("field 11")
        .kind()
        .as_message()
        .expect("Addr message")
        .clone();
    let addr_cols = vec![
        ColumnPlan {
            name: "city".into(),
            tag: (1 << 3) | 2,
            repeated: false,
            kind: ColumnKind::String,
            field_desc: addr_desc.get_field(1).expect("city"),
        },
        ColumnPlan {
            name: "zip".into(),
            tag: (2 << 3) | 2,
            repeated: false,
            kind: ColumnKind::String,
            field_desc: addr_desc.get_field(2).expect("zip"),
        },
    ]
    .into_boxed_slice();
    let addr_plan = MessagePlan {
        by_lower: addr_cols
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.to_ascii_lowercase().into_boxed_str(), i))
            .collect(),
        columns: addr_cols,
        msg_desc: addr_desc,
    };

    let specs: Vec<(&str, u32, u32, bool, ColumnKind)> = vec![
        ("id", 1, 0, false, ColumnKind::Int64),
        ("name", 2, 2, false, ColumnKind::String),
        ("flag", 3, 0, false, ColumnKind::Bool),
        ("amount", 4, 2, false, ColumnKind::Numeric),
        ("created", 5, 0, false, ColumnKind::Timestamp),
        ("d", 6, 0, false, ColumnKind::Date),
        ("dt", 7, 0, false, ColumnKind::Datetime),
        ("meta", 8, 2, false, ColumnKind::Json),
        ("tags", 9, 2, true, ColumnKind::String),
        ("payload", 10, 2, false, ColumnKind::String),
        ("addr", 11, 2, false, ColumnKind::Struct(addr_plan)),
    ];
    let columns: Box<[ColumnPlan]> = specs
        .into_iter()
        .map(|(name, num, wt, rep, kind)| ColumnPlan {
            name: name.into(),
            tag: (num << 3) | wt,
            repeated: rep,
            kind,
            field_desc: root.get_field(num).expect("column field"),
        })
        .collect();
    let by_lower = columns
        .iter()
        .enumerate()
        .map(|(i, c)| (c.name.to_ascii_lowercase().into_boxed_str(), i))
        .collect();
    (
        proto_schema,
        MessagePlan {
            columns,
            by_lower,
            msg_desc: root,
        },
    )
}

/// Deterministic JSON rows for the 11-column realistic BigQuery schema (~394 B JSON/row).
///
/// Note: `meta` is serialized as an escaped JSON string literal so `arrow-json` can decode
/// the identical input into `DataType::Utf8` without failing on a nested JSON object.
pub fn generate_realistic_json_rows(n: usize, name_override: Option<&str>) -> Vec<String> {
    let mut r = Rng(RNG_SEED);
    (0..n)
        .map(|i| {
            let generated_name = if name_override.is_none() {
                r.letters(16)
            } else {
                String::new()
            };
            let name = name_override.unwrap_or(&generated_name);
            let flag = r.next().is_multiple_of(2);
            let amount = format!("{}.{:02}", r.next() % 1_000_000, r.next() % 100);
            let meta_k = r.next() % 1000;
            let meta_v = r.letters(6);
            let t0 = r.letters(8);
            let t1 = r.letters(8);
            let t2 = r.letters(8);
            let payload = r.letters(100);
            let city = r.letters(10);
            let zip = format!("{:05}", r.next() % 100_000);
            format!(
                r#"{{"id":{i},"name":"{name}","flag":{flag},"amount":{amount},"created":"2026-10-30T12:00:00.123456Z","d":"2026-10-21","dt":"2026-10-21T12:34:56.123456","meta":"{{\"k\":{meta_k},\"v\":\"{meta_v}\"}}","tags":["{t0}","{t1}","{t2}"],"payload":"{payload}","addr":{{"city":"{city}","zip":"{zip}"}}}}"#
            )
        })
        .collect()
}
