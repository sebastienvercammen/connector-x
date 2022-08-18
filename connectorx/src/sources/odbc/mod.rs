// Submodules
mod errors; // odbc errors
mod typesystem; // Maps ODBC Datatypes to Rust Datatypes
pub use self::errors::ODBCSourceError; // public so library users can access
pub use self::typesystem::ODBCTypeSystem; // public so library users can access

// Internal Libraries
use crate::{
    // constants::DB_BUFFER_SIZE, // ODBC uses Batch Size (Number of Rows)
    data_order::DataOrder,     // row or column.
    errors::ConnectorXError,   // errors
    sources::{PartitionParser, Produce, Source, SourcePartition}, // Traits required to implement Source
    sql::{count_query, limit1_query, CXQuery},                    // handles queries
    utils::DummyBox, // Rust Ownership 
};

// External Libraries 
use odbc_api::{
    buffers::{TextRowSet, BufferDescription, BufferKind, ColumnarAnyBuffer, Item},
    Cursor, CursorImpl, Environment, ResultSetMetadata, RowSetCursor, ColumnDescription, StatementConnection, DataType, Nullability
};

use std::convert::TryInto;   // Type Change
use anyhow::Error;           // Error Handling 
use fehler::{throw, throws}; // Error Handling
use owning_ref::OwningHandle; // Rust Ownership



pub struct ODBCSource {
    env: Environment,
    connection_string: String,
    origin_query: Option<String>,
    queries: Vec<CXQuery<String>>, // where is this from?
    column_names: Vec<String>,
    schema: Vec<ODBCTypeSystem>,
}

impl ODBCSource {
    #[throws(ODBCSourceError)]
    pub fn new(connection_string: &str) -> Self {
        Self {
            env: Environment::new().unwrap(),
            connection_string: connection_string.to_string(),
            origin_query: None,
            queries: vec![],
            column_names: vec![],
            schema: vec![],
        }
    }
}

impl Source for ODBCSource {
    const DATA_ORDERS: &'static [DataOrder] = &[DataOrder::RowMajor]; // DataOrder is Row
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
        let odbc_connection = self.env.connect_with_connection_string(&self.connection_string).unwrap();

        // TODO - use sql module.
        let metadata_query = "SELECT * FROM cities LIMIT 1;";
        let mut cursor = odbc_connection.execute(metadata_query, ()).unwrap().unwrap();

        let number_of_columns = cursor.num_result_cols().unwrap();
        let mut column_description = ColumnDescription::default();

        // get tables column names 
        for column_number in 1..(number_of_columns+1) {
            cursor.describe_col(column_number.try_into().unwrap(), &mut column_description).unwrap();
            
            let column_name = column_description.name_to_string().unwrap();
            self.column_names.push(column_name);
        }

        // get tables column datatypes 
        for column_number in 1..(number_of_columns+1) {
            let column_datatype = column_description.data_type;
            let column_nullable = matches!(column_description.nullability, Nullability::Nullable);
    
            let odbc_column_datatype = match column_datatype {
                DataType::Integer => ODBCTypeSystem::Integer(column_nullable),
            };
            self.schema.push(odbc_column_datatype);
        }
    }

    #[throws(ODBCSourceError)]
    fn result_rows(&mut self) -> Option<usize> {
        let odbc_connection = self.env.connect_with_connection_string(&self.connection_string).unwrap();

        // TODO: use sql module
        let count_query = "SELECT COUNT (*) FROM cities;";
        let mut cursor = odbc_connection.execute(count_query, ()).unwrap().unwrap();

        let mut cursor_row = cursor.next_row().unwrap().unwrap();
        
        let mut result_rows: i32 = 0;
        cursor_row.get_data(1, &mut result_rows).unwrap();
        let result_rows_usize: usize = result_rows as usize;
        Some(result_rows_usize)
    }

    fn names(&self) -> Vec<String> {
        self.column_names.clone()
    }

    fn schema(&self) -> Vec<Self::TypeSystem> {
        self.schema.clone()
    }

    #[throws(ODBCSourceError)]
    fn partition(self) -> Vec<Self::Partition> {
        let mut ret = vec![];
        for query in self.queries {
            ret.push(ODBCSourcePartition::new(
                self.connection_string.clone(),
                &query,
                &self.schema,
            ));
        }
        ret
    }
}

// TODO: Buffer is incorrect - Why do we need? 
// We can construct a buffer since we have the schema of the table. 
pub struct ODBCSourcePartition {
    env: Environment,
    // buffer: TextRowSet, // unique
    connection_string: String,
    query: CXQuery<String>, // only one, as source has multiple
    schema: Vec<ODBCTypeSystem>,
    nrows: usize, // unique
    ncols: usize, // unique
}



impl ODBCSourcePartition {
    pub fn new(connection_string: String, query: &CXQuery<String>, schema: &[ODBCTypeSystem]) -> Self {
        Self {
            env: Environment::new().unwrap(),
            // buffer: make_partition_buffer(),
            connection_string,
            query: query.clone(),
            schema: schema.to_vec(),
            nrows: 0,
            ncols: schema.len(), // why do we need? 
        }
    }
}

// This is rigid. It should change depending on schema. 
fn make_buffer() -> ColumnarAnyBuffer {
    let batch_size = 1000; // Maximum number of rows in each row set
    
    let buffer_description_list = [BufferDescription {
        kind: BufferKind::I32,
        nullable: true,
    }];

    // Allocates a ColumnarBuffer fitting the buffer descriptions.
    // capacity in usize
    // Iterator whose items are buffer description
    let buffer = ColumnarAnyBuffer::from_description(
        batch_size,
        buffer_description_list.iter().copied(), // See Iterators and Copy vs Clone
    );

    buffer
}


impl SourcePartition for ODBCSourcePartition {
    type TypeSystem = ODBCTypeSystem;
    type Parser<'a> = ODBCSourcePartitionParser<'a>;
    type Error = ODBCSourceError;

    #[throws(ODBCSourceError)]
    fn result_rows(&mut self) {
        // TODO: get the number of rows of the partitioned query without fetching the query's result
        // Can take a look at sql::count_query
        // ANDY: Just copy from ODBCSource resultrows once that is figured out. 
    }

    #[throws(ODBCSourceError)]
    fn parser(&mut self) -> Self::Parser<'_> {

        let odbc_connection = self.env.connect_with_connection_string(&self.connection_string).unwrap();

        let query = self.query.as_str();
        let mut cursor = odbc_connection.execute(query, ()).unwrap().unwrap();

        let mut buffer = make_buffer();

        let mut row_set_cursor = cursor.bind_buffer(&mut buffer).unwrap();

        // error[E0308]: mismatched types
        // row_set_cursor expected struct `RowSetCursor<CursorImpl<StatementConnection<'_>>, &mut ColumnarBuffer<TextColumn<u8>>>`
        // found struct `RowSetCursor<CursorImpl<StatementImpl<'_>>, &mut ColumnarBuffer<AnyColumnBuffer>>`
        // Note by Andy: I believe that the function should be redefined to take a ColumnarBuffer AnyColumnBuffer.
        ODBCSourcePartitionParser::new(row_set_cursor, &self.schema).unwrap()
    }

    fn nrows(&self) -> usize {
        self.nrows
    }

    fn ncols(&self) -> usize {
        self.ncols
    }
}





// Incorrect! 
// We want to use ColumnarBuffer<AnyColumnBuffer> instead of TextRowSet
// Statement Connection is not working. 
type ODBCCursor<'a> = RowSetCursor<CursorImpl<StatementConnection<'a>>, &'a mut TextRowSet>;
type RowSet<'a> = DummyBox<Option<&'a &'a mut TextRowSet>>;

fn handle_fn<'a>(cursor: *const ODBCCursor<'a>) -> RowSet<'a> {
    unsafe { DummyBox((&mut *(cursor as *mut ODBCCursor<'a>)).fetch().unwrap()) }
}





// I don't understand beyond this point. 
pub struct ODBCSourcePartitionParser<'a> {
    rows: Option<OwningHandle<Box<ODBCCursor<'a>>, RowSet<'a>>>,
    ncols: usize,
    current_row: usize,
    current_col: usize,
}

impl<'a> ODBCSourcePartitionParser<'a> {
    #[throws(ODBCSourceError)]
    pub fn new(cursor: ODBCCursor<'a>, schema: &[ODBCTypeSystem]) -> Self {
        let rows = OwningHandle::new_with_fn(Box::new(cursor), handle_fn);
        Self {
            rows: Some(rows),
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
            let rows = self.rows.take().expect("empty rows");
            let rows = rows.into_owner();
            self.rows = Some(OwningHandle::new_with_fn(rows, handle_fn));
        }

        let rows = self.rows.as_ref().expect("empty rows");

        let num_rows: usize = match **rows {
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
        let rows = self.rows.as_ref().expect("empty rows");

        let value = match **rows {
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

impl<'r, 'a> Produce<'r, Option<i32>> for ODBCSourcePartitionParser<'a> {
    type Error = ODBCSourceError;

    #[throws(ODBCSourceError)]
    fn produce(&'r mut self) -> Option<i32> {
        println!("What is going on");

        return Some(0);
    }
}


// Andy's Helper Functions 

fn get_data_into_rust(cursor: impl Cursor) -> Result<(), Error> {
    let mut buffer = make_buffer();

    // Bind buffer to cursor. We bind the buffer as a mutable reference here, which makes it
    // easier to reuse for other queries, but we could have taken ownership.
    let mut row_set_cursor = cursor.bind_buffer(&mut buffer)?;

    // Loop over row sets
    while let Some(row_set) = row_set_cursor.fetch()? {
        println!("num valid rows {:?}", row_set.num_rows());
        println!("num valid columns {:?}", row_set.num_cols());

        // Process names in row set
        let name_col = row_set.column(0);
        for name in name_col
            .as_text_view()
            .expect("Name column buffer expected to be text")
            .iter()
        {
            // Iterate over `Option<&CStr> ..
            // returns Some Array Int
            println!("{:?}", name);
        }

        // Process years in row set
        let year_col = row_set.column(1);
        // This does set it to i32! Or returns Enum Option None
        for year in i32::as_nullable_slice(year_col)
            .expect("Year column buffer expected to be nullable Int")
        {
            // year is an option still. It can be None if nullable value in the postgresdb.
            if let Some(x) = year {
                println!("{:?}", x);
            } else {
                println!("{:?} is a null value", year)
            }
        }
    }

    Ok(())
}