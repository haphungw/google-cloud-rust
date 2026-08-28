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

use crate::google::cloud::bigquery::storage::v1;
use crate::model::ProtoSchema;
use gaxi::prost::{ConvertError, FromProto, ToProto};

impl ToProto<v1::ProtoSchema> for ProtoSchema {
    type Output = v1::ProtoSchema;
    fn to_proto(self) -> Result<v1::ProtoSchema, ConvertError> {
        // TODO(#5315) - implement conversions for DescriptorProto
        Err(ConvertError::Unimplemented)
    }
}

impl FromProto<ProtoSchema> for v1::ProtoSchema {
    fn cnv(self) -> Result<ProtoSchema, ConvertError> {
        // TODO(#5315) - implement conversions for DescriptorProto
        Err(ConvertError::Unimplemented)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use crate::google::cloud::bigquery::storage::v1::{TableSchema, TableFieldSchema, table_field_schema::{Type, Mode}};
    use prost_types::{
        field_descriptor_proto, DescriptorProto, FieldDescriptorProto, FileDescriptorProto,
    };
    use prost_reflect::{DescriptorPool, DynamicMessage, Value};

    /// Port of mapping BQ field types to Protobuf field types
    fn get_proto_type(bq_type: Type) -> field_descriptor_proto::Type {
        match bq_type {
            Type::String => field_descriptor_proto::Type::String,
            Type::Bytes => field_descriptor_proto::Type::Bytes,
            Type::Int64 => field_descriptor_proto::Type::Int64,
            Type::Double => field_descriptor_proto::Type::Double,
            Type::Bool => field_descriptor_proto::Type::Bool,
            Type::Timestamp | Type::Datetime | Type::Time => field_descriptor_proto::Type::Int64, 
            Type::Date => field_descriptor_proto::Type::Int32,
            Type::Numeric | Type::Bignumeric => field_descriptor_proto::Type::Bytes,
            Type::Json => field_descriptor_proto::Type::String,
            Type::Struct => field_descriptor_proto::Type::Message,
            Type::Unspecified => field_descriptor_proto::Type::String,
            _ => field_descriptor_proto::Type::String, // Fallback
        }
    }

    /// Port of mapping BQ field modes to Protobuf labels
    fn get_proto_label(bq_mode: Mode) -> field_descriptor_proto::Label {
        match bq_mode {
            Mode::Required => field_descriptor_proto::Label::Required,
            Mode::Repeated => field_descriptor_proto::Label::Repeated,
            Mode::Nullable => field_descriptor_proto::Label::Optional,
            _ => field_descriptor_proto::Label::Optional,
        }
    }

    /// Recursively converts a BigQuery TableSchema into a Protobuf DescriptorProto.
    fn table_schema_to_descriptor(schema: &TableSchema, message_name: &str) -> DescriptorProto {
        let mut fields = Vec::new();
        let mut nested_types = Vec::new();

        for (i, bq_field) in schema.fields.iter().enumerate() {
            let field_num = (i + 1) as i32;
            
            let field_type = Type::try_from(bq_field.r#type).unwrap_or(Type::Unspecified);
            let field_mode = Mode::try_from(bq_field.mode).unwrap_or(Mode::Unspecified);

            let mut field_proto = FieldDescriptorProto {
                name: Some(bq_field.name.clone()),
                number: Some(field_num),
                label: Some(get_proto_label(field_mode) as i32),
                r#type: Some(get_proto_type(field_type) as i32),
                ..Default::default()
            };

            // If it's a structural type (nested message), we have to dynamically generate a nested Descriptor.
            if field_type == Type::Struct {
                let nested_name = format!("{}_Nested{}", message_name, bq_field.name);
                field_proto.type_name = Some(nested_name.clone());

                let nested_schema = TableSchema {
                    fields: bq_field.fields.clone(),
                };
                let nested_desc = table_schema_to_descriptor(&nested_schema, &nested_name);
                nested_types.push(nested_desc);
            }

            fields.push(field_proto);
        }

        DescriptorProto {
            name: Some(message_name.to_string()),
            field: fields,
            nested_type: nested_types,
            ..Default::default()
        }
    }

    #[test]
    fn test_schema_conversion_to_dynamic_message() -> Result<()> {
        // 1. Simulate a schema returned from the BigQuery Storage API
        let bq_schema = TableSchema {
            fields: vec![
                TableFieldSchema {
                    name: "username".to_string(),
                    r#type: Type::String as i32,
                    mode: Mode::Required as i32,
                    ..Default::default()
                },
                TableFieldSchema {
                    name: "metadata".to_string(),
                    r#type: Type::Struct as i32,
                    mode: Mode::Nullable as i32,
                    fields: vec![
                        TableFieldSchema {
                            name: "created_at".to_string(),
                            r#type: Type::Timestamp as i32,
                            mode: Mode::Nullable as i32,
                            ..Default::default()
                        }
                    ],
                    ..Default::default()
                },
            ],
        };

        // 2. Convert TableSchema -> DescriptorProto
        let descriptor = table_schema_to_descriptor(&bq_schema, "DynamicUserRow");

        // 3. Put it in a FileDescriptorProto (required by prost_reflect descriptor pool)
        let file_descriptor = FileDescriptorProto {
            name: Some("dynamic_schema.proto".to_string()),
            package: Some("dynamic".to_string()),
            message_type: vec![descriptor],
            ..Default::default()
        };

        // 4. Create a DescriptorPool and add FileDescriptorProto
        let mut pool = DescriptorPool::new();
        pool.add_file_descriptor_proto(file_descriptor).expect("Failed to add file descriptor to pool");

        // 5. Get the MessageDescriptor by its full name ("package.MessageName")
        let message_desc = pool.get_message_by_name("dynamic.DynamicUserRow").expect("Could not find message in pool");

        // 6. Create a DynamicMessage from the descriptor
        let mut dynamic_msg = DynamicMessage::new(message_desc);

        // 7. Populate fields dynamically based on field names
        dynamic_msg.set_field_by_name("username", Value::String("haphung".to_string()));
        
        let mut nested_msg = DynamicMessage::new(
            pool.get_message_by_name("dynamic.DynamicUserRow.DynamicUserRow_Nestedmetadata").expect("could not find nested msg")
        );
        nested_msg.set_field_by_name("created_at", Value::I64(123456789));
        
        dynamic_msg.set_field_by_name("metadata", Value::Message(nested_msg));

        // 8. Assert that prost_reflect properly ingested the AST and accepted dynamic inputs
        assert_eq!(dynamic_msg.get_field_by_name("username").unwrap().as_str().unwrap(), "haphung");
        
        // 9. Encode it to bytes
        use prost::Message;
        let encoded_bytes = dynamic_msg.encode_to_vec();
        assert!(!encoded_bytes.is_empty());
        
        println!("Successfully built a dynamic message from BigQuery's TableSchema");
        
        Ok(())
    }
}
