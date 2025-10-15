use crate::model;
use crate::model::CreateCollectionRequest;
use crate::model::Field;
use crate::model::FieldType;
use crate::model::Index;
use crate::model::OrderByClause;
use crate::model::Query;
use serde_json::Value;

use crate::Result;
use crate::error::Error;

impl From<super::builder::FieldType> for i32 {
    fn from(val: super::builder::FieldType) -> Self {
        use super::builder::FieldType;
        match val {
            FieldType::String => crate::model::FieldType::String as i32,
            FieldType::Boolean => crate::model::FieldType::Boolean as i32,
            FieldType::Integer => crate::model::FieldType::Integer as i32,
            FieldType::Double => crate::model::FieldType::Double as i32,
            FieldType::Uuid => crate::model::FieldType::Uuid as i32,
        }
    }
}

pub struct ProtobufFieldParts {
    pub(super) proto_field: crate::model::Field,
    pub(super) proto_index: Option<crate::model::Index>,
}

impl From<super::builder::Field> for ProtobufFieldParts {
    fn from(val: super::builder::Field) -> Self {
        let proto_field = crate::model::Field {
            name: val.name.clone(),
            r#type: val.field_type.into(),
        };

        let proto_index = if val.indexed || val.unique {
            Some(crate::model::Index {
                fields: vec![val.name],
                is_unique: val.unique,
            })
        } else {
            None
        };

        ProtobufFieldParts {
            proto_field,
            proto_index,
        }
    }
}

pub fn to_struct(
    json: serde_json::Map<String, serde_json::Value>,
) -> prost_types::Struct {
    prost_types::Struct {
        fields: json
            .into_iter()
            .map(|(k, v)| (k, serde_json_to_prost(v)))
            .collect(),
    }
}

fn serde_json_to_prost(json: serde_json::Value) -> prost_types::Value {
    use prost_types::value::Kind::*;
    use serde_json::Value::*;
    prost_types::Value {
        kind: Some(match json {
            Null => NullValue(0 /* wat? */),
            Bool(v) => BoolValue(v),
            Number(n) => {
                NumberValue(n.as_f64().expect("Non-f64-representable number"))
            }
            String(s) => StringValue(s),
            Array(v) => ListValue(prost_types::ListValue {
                values: v.into_iter().map(serde_json_to_prost).collect(),
            }),
            Object(v) => StructValue(to_struct(v)),
        }),
    }
}

fn prost_to_serde_json(x: prost_types::Value) -> serde_json::Value {
    use prost_types::value::Kind::*;
    use serde_json::Value::*;
    match x.kind {
        Some(x) => match x {
            NullValue(_) => Null,
            BoolValue(v) => Bool(v),
            NumberValue(n) => Number(serde_json::Number::from_f64(n).unwrap()),
            StringValue(s) => String(s),
            ListValue(lst) => {
                Array(lst.values.into_iter().map(prost_to_serde_json).collect())
            }
            StructValue(v) => Object(
                v.fields
                    .into_iter()
                    .map(|(k, v)| (k, prost_to_serde_json(v)))
                    .collect(),
            ),
        },
        None => panic!("todo"),
    }
}

pub fn json_to_immudb_query(json_query: Value) -> Result<Query> {
    let map = match json_query {
        Value::Object(m) => m,
        _ => {
            return Err(Error::InvalidInput(
                "Query must be a JSON object".into(),
            ));
        }
    };

    let collection_name = map
        .get("collection_name")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::InvalidInput("Missing 'collection_name'".into()))?
        .to_string();

    let limit = map.get("limit").and_then(Value::as_u64).unwrap_or(100) as u32; // Устанавливаем разумный дефолт

    let order_by = map
        .get("order_by")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|item| {
                    let m = item.as_object()?;
                    let field = m.get("field")?.as_str()?.to_string();
                    let desc =
                        m.get("desc").and_then(Value::as_bool).unwrap_or(false);
                    Some(OrderByClause { field, desc })
                })
                .collect()
        })
        .unwrap_or_default();

    let mut expressions = Vec::new();
    if let Some(where_clause) = map.get("where").and_then(Value::as_object) {
        // Does immudb use "AND" logic for expressions list?
        // Try to find "AND" in WHERE.
        if let Some(and_array) =
            where_clause.get("AND").and_then(Value::as_array)
        {
            for item in and_array {
                if let Some(comparison_map) = item.as_object() {
                    // Each FieldComparison becomes QueryExpression
                    // in expressions list.
                    let comparison = json_to_field_comparison(comparison_map)?;
                    expressions.push(model::QueryExpression {
                        field_comparisons: vec![comparison],
                    });
                }
            }
        }
        // TODO: Can add "OR" logic or any other complex logic
    }

    Ok(Query {
        collection_name,
        expressions,
        order_by,
        limit,
    })
}

fn json_to_field_comparison(
    json_map: &serde_json::Map<String, Value>,
) -> Result<model::FieldComparison> {
    let field = json_map
        .get("field")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::InvalidInput("Missing 'field'".into()))?
        .to_string();
    let op = json_map
        .get("op")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::InvalidInput("Missing 'op'".into()))?;
    let value = json_map
        .get("value")
        .ok_or_else(|| Error::InvalidInput("Missing 'value'".into()))?
        .clone();

    Ok(model::FieldComparison {
        field,
        operator: map_operator(op)?,
        value: Some(serde_json_to_prost(value)),
    })
}

fn map_operator(op: &str) -> Result<i32> {
    match op.to_uppercase().as_str() {
        "EQ" => Ok(0), // ComparisonOperator::EQ as i32
        "NE" => Ok(1), // ComparisonOperator::NE as i32
        "GT" => Ok(2), // ComparisonOperator::GT as i32
        "GE" => Ok(3), // ComparisonOperator::GE as i32
        "LT" => Ok(4), // ComparisonOperator::LT as i32
        "LE" => Ok(5), // ComparisonOperator::LE as i32
        _ => Err(Error::InvalidInput(format!(
            "Unknown comparison operator: {}",
            op
        ))),
    }
}

fn parse_field_type(type_str: &str) -> Result<FieldType> {
    match type_str.to_uppercase().as_str() {
        "STRING" | "STR" => Ok(FieldType::String),
        "BOOLEAN" | "BOOL" => Ok(FieldType::Boolean),
        "INTEGER" | "INT" => Ok(FieldType::Integer),
        "DOUBLE" | "FLOAT" => Ok(FieldType::Double),
        "UUID" => Ok(FieldType::Uuid),
        _ => Err(Error::InvalidInput(format!(
            "unknown field type: {}",
            type_str
        ))),
    }
}

pub fn json_to_create_collection_request(
    json_schema: Value,
) -> Result<CreateCollectionRequest> {
    let map = json_schema
        .as_object()
        .ok_or_else(|| Error::InvalidInput("root must be an object".into()))?;

    let name = map
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::InvalidInput("Missing or invalid 'name'".into()))?
        .to_string();

    let document_id_field_name = map
        .get("document_id_field_name")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            Error::InvalidInput(
                "Missing or invalid 'document_id_field_name'".into(),
            )
        })?
        .to_string();

    let fields_json =
        map.get("fields").and_then(Value::as_array).ok_or_else(|| {
            Error::InvalidInput("Missing or invalid 'fields' array".into())
        })?;

    let mut fields: Vec<Field> = Vec::new();
    let mut indexes: Vec<Index> = Vec::new();

    for field_def in fields_json {
        let def = field_def.as_object().ok_or_else(|| {
            Error::InvalidInput("Field definition must be an object".into())
        })?;
        let field_name = def
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::InvalidInput("Field 'name' missing".into()))?
            .to_string();
        let type_str =
            def.get("type").and_then(Value::as_str).ok_or_else(|| {
                Error::InvalidInput("Field 'type' missing".into())
            })?;

        let field_type = parse_field_type(type_str)?;

        fields.push(Field {
            name: field_name.clone(),
            r#type: field_type.into(),
        });

        if def.get("indexed").and_then(Value::as_bool).unwrap_or(false)
            || field_name == document_id_field_name
        {
            let is_unique =
                def.get("unique").and_then(Value::as_bool).unwrap_or(false)
                    || field_name == document_id_field_name;

            indexes.push(Index {
                fields: vec![field_name],
                is_unique,
            });
        }
    }

    Ok(CreateCollectionRequest {
        name,
        document_id_field_name,
        fields,
        indexes,
    })
}
