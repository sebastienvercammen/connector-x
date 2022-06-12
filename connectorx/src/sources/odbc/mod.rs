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

impl Source for ODBCSource {
    const DATA_ORDERS: &'static [DataOrder] = &[DataOrder::RowMajor];
    type Partition = ODBCSourcePartition;
    type TypeSystem = ODBCTypeSystem;
    type Error = ODBCSourceError;

    #[throws(ODBCSourceError)]
    fn set_data_order(&mut self, data_order: DataOrder) {
        if !matches!(data_order, DataOrder::RowMajor) {
            throw!(ConnectorXError::UnsupportedDataOrder(data_order));
        }
    }

    fn set_queries<Q: ToString>(&mut self, queries: &[CXQuery<Q>]) {
        self.queries = queries.iter().map(|q| q.map(Q::to_string)).collect();
    }

    fn set_origin_query(&mut self, query: Option<String>) {
        self.origin_query = query;
    }

    #[throws(ODBCSourceError)]
    fn fetch_metadata(&mut self) {
        // TODO: need to get the metadata (fill in column names to self.names, and column types to self.schema)
    }

    #[throws(ODBCSourceError)]
    fn result_rows(&mut self) -> Option<usize> {
        // TODO: get the number of rows of the entire query without fetching the query's result
        // Can take a look at sql::count_query
        None
    }

    fn names(&self) -> Vec<String> {
        self.names.clone()
    }

    fn schema(&self) -> Vec<Self::TypeSystem> {
        self.schema.clone()
    }

    #[throws(ODBCSourceError)]
    fn partition(self) -> Vec<Self::Partition> {
        let mut ret = vec![];
        for query in self.queries {
            ret.push(ODBCSourcePartition::new(
                self.dsn.clone(),
                &query,
                &self.schema,
            ));
        }
        ret
    }
}

pub struct ODBCSourcePartition {
    env: Environment,
    buffer: TextRowSet,
    dsn: String,
    query: CXQuery<String>,
    schema: Vec<ODBCTypeSystem>,
    nrows: usize,
    ncols: usize,
}

impl ODBCSourcePartition {
    pub fn new(dsn: String, query: &CXQuery<String>, schema: &[ODBCTypeSystem]) -> Self {
        Self {
            env: Environment::new().unwrap(),
            buffer: ColumnarBuffer::new(vec![]),
            dsn,
            query: query.clone(),
            schema: schema.to_vec(),
            nrows: 0,
            ncols: schema.len(),
        }
    }
}

impl SourcePartition for ODBCSourcePartition {
    type TypeSystem = ODBCTypeSystem;
    type Parser<'a> = ODBCSourcePartitionParser<'a>;
    type Error = ODBCSourceError;

    #[throws(ODBCSourceError)]
    fn result_rows(&mut self) {
        // TODO: get the number of rows of the partitioned query without fetching the query's result
        // Can take a look at sql::count_query
    }

    #[throws(ODBCSourceError)]
    fn parser(&mut self) -> Self::Parser<'_> {
        // TODO: change the fake connection (e.g. username, password) to real one
        // with input connection string (dsn)
        let connection = self
            .env
            .connect("YourDatabase", "SA", "My@Test@Password1")
            .unwrap();
        let mut cursor = connection
            .into_cursor(self.query.as_str(), ())
            .unwrap()
            .unwrap();
        self.buffer = TextRowSet::for_cursor(DB_BUFFER_SIZE, &mut cursor, Some(4096)).unwrap();
        let row_set_cursor = cursor.bind_buffer(&mut self.buffer).unwrap();

        ODBCSourcePartitionParser::new(row_set_cursor, &self.schema)?
    }

    fn nrows(&self) -> usize {
        self.nrows
    }

    fn ncols(&self) -> usize {
        self.ncols
    }
}

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
            current_col: 0,
        }
    }

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
        if self.current_row > 0 {
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
        }

        let num_rows: usize = match *self.rows {
            Some(batch) => batch.num_rows(),
            None => 0,
        };
        self.current_row = 0;
        self.current_col = 0;
        (num_rows, num_rows == 0)
    }
}

impl<'r, 'a> Produce<'r, i32> for ODBCSourcePartitionParser<'a> {
    type Error = ODBCSourceError;

    #[throws(ODBCSourceError)]
    fn produce(&'r mut self) -> i32 {
        let (row, col) = self.next_loc()?;

        let batch = *self.rows;
        let value = match batch {
            Some(b) => b.at(row, col),
            None => {
                // TODO: throw an error here
                unimplemented!();
            }
        };

        // TODO: need to figure out how to convert value from bytes to i32 and return
        let val = 0;
        val
    }
}

// TODO: implement produce for Option<i32>
