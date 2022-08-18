// use std::string::FromUtf8Error;
use thiserror::Error;

// basically whenever ODBC Source throws any error, it will throw one of these errors.
// this way the program can know who threw the error, and which one it is.
// see thiserror for more

// As I write ODBCSource I will gradually fill this in.
#[derive(Error, Debug)]
pub enum ODBCSourceError {
    #[error(transparent)]
    ConnectorXError(#[from] crate::errors::ConnectorXError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

// #[derive(Error, Debug)]
// pub enum OracleSourceError {
//     #[error(transparent)]
//     ConnectorXError(#[from] crate::errors::ConnectorXError),

//     #[error(transparent)]
//     OracleError(#[from] r2d2_oracle::oracle::Error),

//     #[error(transparent)]
//     OraclePoolError(#[from] r2d2::Error),

//     #[error(transparent)]
//     OracleUrlError(#[from] url::ParseError),

//     #[error(transparent)]
//     OracleUrlDecodeError(#[from] FromUtf8Error),

//     /// Any other errors that are too trivial to be put here explicitly.
//     #[error(transparent)]
//     Other(#[from] anyhow::Error),
// }
