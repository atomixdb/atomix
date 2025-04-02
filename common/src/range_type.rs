use scylla::cql_to_rust::{FromCqlVal, FromCqlValError};
use scylla::frame::response::result::{ColumnType, CqlValue};
use scylla::frame::value::Value;
use scylla::serialize::value::SerializeCql;
use scylla::serialize::writers::WrittenCellProof;
use scylla::serialize::{CellWriter, SerializationError};

#[derive(Debug, Clone, PartialEq)]
pub enum RangeType {
    Primary,
    Secondary,
}

impl SerializeCql for RangeType {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let value = match self {
            RangeType::Primary => "Primary",
            RangeType::Secondary => "Secondary",
        };
        SerializeCql::serialize(&value, typ, writer)
    }
}

impl Value for RangeType {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), scylla::frame::value::ValueTooBig> {
        let value = match self {
            RangeType::Primary => "Primary",
            RangeType::Secondary => "Secondary",
        };
        Value::serialize(&value, buf)
    }
}

impl FromCqlVal<CqlValue> for RangeType {
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        let value = String::from_cql(cql_val)?;
        match value.as_str() {
            "Primary" => Ok(RangeType::Primary),
            "Secondary" => Ok(RangeType::Secondary),
            _ => Err(FromCqlValError::BadVal),
        }
    }
}
