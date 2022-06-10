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


impl<'a> From<&'a DataType> for ODBCTypeSystem {
    fn from(ty: &'a DataType, nullable: bool) -> ODBCTypeSystem {
        use ODBCTypeSystem::*;

        match ty {
            Integer => Integer(nullable), 
        }
    }
}


