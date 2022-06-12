// use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
// use rust_decimal::Decimal;
// use serde_json::Value;
// use std::collections::HashMap;
// use uuid::Uuid;

// https://docs.rs/odbc-api/latest/odbc_api/enum.DataType.html
// use postgres::types::Type; // Replace with ODBC's
use odbc_api::DataType;

#[derive(Copy, Clone, Debug)]
pub enum ODBCTypeSystem {
    Integer(bool),
}

impl_typesystem! {
    system = ODBCTypeSystem,
    mappings = {
        { Integer => i32 }
    }
}

impl<'a> From<(&'a DataType, bool)> for ODBCTypeSystem {
    fn from(ty_null: (&'a DataType, bool)) -> ODBCTypeSystem {
        use ODBCTypeSystem::*;

        let (ty, nullable) = ty_null;
        match ty {
            DataType::Integer => Integer(nullable),
            _ => unimplemented!("{}", format!("odb type {:?} not supported", ty)),
        }
    }
}
