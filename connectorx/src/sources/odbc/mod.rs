//! Source implementation for SQLite embedded database.

mod errors;
mod typesystem;

pub use self::errors::ODBCSourceError;
pub use self::typesystem::ODBCTypeSystem;
use crate::{
    constants::DB_BUFFER_SIZE,
    data_order::DataOrder,
    errors::ConnectorXError,
    sources::{PartitionParser, Produce, Source, SourcePartition},
    sql::{count_query, limit1_query, CXQuery},
    utils::DummyBox,
};
use anyhow::anyhow;
use datafusion::logical_plan::Column;
use fehler::{throw, throws};
use odbc_api::{
    buffers::{ColumnarBuffer, TextColumn, TextRowSet},
    Connection, Cursor, CursorImpl, Environment, ResultSetMetadata, RowSetCursor,
    StatementConnection,
};
use owning_ref::OwningHandle;

pub struct ODBCSource {
    env: Environment,
    dsn: String,
    origin_query: Option<String>,
    queries: Vec<CXQuery<String>>,
    names: Vec<String>,
    schema: Vec<ODBCTypeSystem>,
}

impl ODBCSource {
    #[throws(ODBCSourceError)]
    pub fn new(conn: &str) -> Self {
        let environment = Environment::new().unwrap();
        Self {
            env: environment,
            dsn: conn.to_string(),
            origin_query: None,
            queries: vec![],
            names: vec![],
            schema: vec![],
        }
    }
}

pub struct ODBCSourcePartition {
    dsn: String,
    query: CXQuery<String>,
    schema: Vec<ODBCTypeSystem>,
    nrows: usize,
    ncols: usize,
}

impl ODBCSourcePartition {
    pub fn new(dsn: String, query: &CXQuery<String>, schema: &[ODBCTypeSystem]) -> Self {
        Self {
            dsn,
            query: query.clone(),
            schema: schema.to_vec(),
            nrows: 0,
            ncols: schema.len(),
        }
    }
}

// pub struct ODBCPartitionParser<'a> {
//     cursor: RowSetCursor<CursorImpl<StatementConnection<'a>>, &'a mut TextRowSet>,
//     batch: &'a &'a mut TextRowSet,
// }

// impl<'a> ODBCPartitionParser<'a> {
//     pub fn new(
//         mut cursor: RowSetCursor<CursorImpl<StatementConnection<'a>>, &'a mut TextRowSet>,
//     ) -> Self {
//         let batch = cursor.fetch().unwrap().unwrap();

//         Self { cursor, batch }
// //     }
// }

pub struct ODBCSourcePartitionParser<'a> {
    rows: OwningHandle<
        Box<RowSetCursor<CursorImpl<StatementConnection<'a>>, &'a mut TextRowSet>>,
        DummyBox<Option<&'a &'a mut TextRowSet>>,
    >,
    ncols: usize,
    current_row: usize,
    current_col: usize,
}

impl<'a> ODBCSourcePartitionParser<'a> {
    #[throws(ODBCSourceError)]

    pub fn new(
        cursor: RowSetCursor<CursorImpl<StatementConnection<'a>>, &'a mut TextRowSet>,
        schema: &[ODBCTypeSystem],
    ) -> Self {
        let rows: OwningHandle<
            Box<RowSetCursor<CursorImpl<StatementConnection<'a>>, &'a mut TextRowSet>>,
            DummyBox<Option<&&'a mut TextRowSet>>,
        > = OwningHandle::new_with_fn(
            Box::new(cursor),
            |cursor: *const RowSetCursor<
                CursorImpl<StatementConnection<'a>>,
                &'a mut TextRowSet,
            >| unsafe {
                DummyBox(
                    (&mut *(cursor
                        as *mut RowSetCursor<
                            CursorImpl<StatementConnection<'a>>,
                            &'a mut TextRowSet,
                        >))
                        .fetch()
                        .unwrap(),
                )
            },
        );
        Self {
            rows,
            ncols: schema.len(),
            current_row: 0,
            current_col: schema.len(),
        }
    }

    // pub fn new(query: &str, schema: &[ODBCTypeSystem]) -> Self {
    //     let environment = Environment::new().unwrap();
    //     let connection = environment
    //         .connect("YourDatabase", "SA", "My@Test@Password1")
    //         .unwrap();
    //     let mut cursor = connection.into_cursor(query, ()).unwrap().unwrap();
    //     let mut buffers: TextRowSet =
    //         TextRowSet::for_cursor(DB_BUFFER_SIZE, &mut cursor, Some(4096)).unwrap();
    //     let mut row_set_cursor = cursor.bind_buffer(&mut buffers).unwrap();

    //     // while let Some(batch) = row_set_cursor.fetch().unwrap() {
    //     //     // Within a batch, iterate over every row
    //     //     for row_index in 0..batch.num_rows() {
    //     //         // Within a row iterate over every column
    //     //         let record = (0..batch.num_cols())
    //     //             .map(|col_index| batch.at(col_index, row_index).unwrap_or(&[]));
    //     //     }
    //     // }

    //     let batch = row_set_cursor.fetch().unwrap().unwrap();
    //     Self {
    //         cursor: row_set_cursor,
    //         batch,
    //         ncols: schema.len(),
    //         current_row: 0,
    //         current_col: 0,
    //     }
    // }

    #[throws(ODBCSourceError)]
    fn next_loc(&mut self) -> (usize, usize) {
        let ret = (self.current_row, self.current_col);
        self.current_row += (self.current_col + 1) / self.ncols;
        self.current_col = (self.current_col + 1) % self.ncols;
        ret
    }
}

impl<'a> PartitionParser<'a> for ODBCSourcePartitionParser<'a> {
    type TypeSystem = ODBCTypeSystem;
    type Error = ODBCSourceError;

    #[throws(ODBCSourceError)]
    fn fetch_next(&mut self) -> (usize, bool) {
        // let batch = *self.rows;
        // let value = match batch {
        //     Some(b) => b.at(1, 2),
        //     None => {
        //         unimplemented!();
        //     }
        // };

        // *self.rows = self.rows.as_owner();

        // let mut cursor = self.rows.as_owner();
        // unsafe {
        //     let a = *cursor;
        // }
        // let batch = cursor.fetch().unwrap();

        self.rows = OwningHandle::new_with_fn(
            self.rows.into_owner(),
            |cursor: *const RowSetCursor<
                CursorImpl<StatementConnection<'a>>,
                &'a mut TextRowSet,
            >| unsafe {
                DummyBox(
                    (&mut *(cursor
                        as *mut RowSetCursor<
                            CursorImpl<StatementConnection<'a>>,
                            &'a mut TextRowSet,
                        >))
                        .fetch()
                        .unwrap(),
                )
            },
        );

        self.current_row = 0;
        self.current_col = 0;
        (0, false)
    }
}
