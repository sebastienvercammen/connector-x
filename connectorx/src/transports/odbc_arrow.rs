//! Transport from ODBC Source to Arrow Destination.

use crate::destinations::arrow::{ArrowDestination, ArrowDestinationError, ArrowTypeSystem};

// By default, a macro has no path-based scope. 
// However, if it has the #[macro_export] attribute, then it is declared in the crate root scope and can be referred to normally as such:
// crate::m!();
// Macros labeled with #[macro_export] are always pub and can be referred to by other crates.
use crate::impl_transport; 
use crate::sources::odbc::{ODBCSource, ODBCSourceError, ODBCTypeSystem};

// TypeConversion is a trait implemented by the macro impl_transport
// Also, sometimes we need to implement our own TypeConversions 

// Defines a rule to convert a type `T` to a type `U`.
// pub trait TypeConversion<T, U> {
//     fn convert(val: T) -> U;
// }
use crate::typesystem::TypeConversion;
use thiserror::Error;


// The SourceArrowTransportError is an enum which contains variants which describes the cause of potential errors during transport.
// The error could occur due to the Source, Destination, or due to ConnectorX itself. 
// The enum is used by the macro. 

#[derive(Error, Debug)]
pub enum ODBCArrowTransportError {
    #[error(transparent)]
    // Source(#[from] BigQuerySourceError),
    Source(#[from] ODBCSourceError), // Variant(#[from] Other module/object) // what is this? // I need to make this!

    #[error(transparent)]
    Destination(#[from] ArrowDestinationError),

    #[error(transparent)]
    ConnectorX(#[from] crate::errors::ConnectorXError),
}

/// Convert ODBC data types to Arrow data types.
pub struct ODBCArrowTransport;

// arrows typesystem - https://github.com/sfu-db/connector-x/blob/main/connectorx/src/destinations/arrow/typesystem.rs 

impl_transport!(
    name = ODBCArrowTransport,
    error = ODBCArrowTransportError,
    systems = ODBCTypeSystem => ArrowTypeSystem,
    route = ODBCSource => ArrowDestination, // Don't have ODBCSource yet
    mappings = {
        { Integer[i32] => Int32[i32] | conversion auto } // auto vs none? 
    }
);
