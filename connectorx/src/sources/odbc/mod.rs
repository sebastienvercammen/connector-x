// Submodules 
mod errors; // odbc errors 
mod typesystem; // Maps ODBC Datatypes to Rust Datatypes 
pub use self::errors::ODBCSourceError; // public so library users can access 
pub use self::typesystem::ODBCTypeSystem; // public so library users can access 

// Internal Libraries 
use crate::{
    constants::DB_BUFFER_SIZE, // ODBC uses Batch Size (Number of Rows)
    data_order::DataOrder, // row or column. 
    errors::ConnectorXError, // errors 
    sources::{PartitionParser, Produce, Source, SourcePartition}, // Traits required to implement Source
    sql::{count_query, limit1_query, CXQuery}, // handles queries 
};

// ODBC Library - Up to me, depending on what I need. 
use odbc_api::{
    buffers::{ColumnarBuffer, TextColumn, TextRowSet},
    Connection, Cursor, CursorImpl, Environment, ResultSetMetadata, RowSetCursor,
    StatementConnection,
};

use odbc_api::ColumnDescription;
// use odbc_api::Cursor;
use odbc_api::Nullability;
use odbc_api::buffers::{BufferDescription, BufferKind, Item, ColumnarAnyBuffer};
use odbc_api::DataType;
use anyhow::Error;


use anyhow::anyhow; // Error Handling 
use fehler::{throw, throws}; // Error Handling 

use owning_ref::OwningHandle; // Rust Ownership 
use crate::utils::DummyBox; // Rust Ownership 


pub struct ODBCSource {
    env: Environment,
    connection_string: String,
    origin_query: Option<String>,
    queries: Vec<CXQuery<String>>, // where is this from? 
    names: Vec<String>,
    schema: Vec<ODBCTypeSystem>,
}

// Implement new ODBCSource
impl ODBCSource {
    #[throws(ODBCSourceError)]
    pub fn new(conn: &str) -> Self {
        let odbc_env = Environment::new().unwrap(); // creates ODBC Environment
        Self {
            env: odbc_env,
            connection_string: conn.to_string(),
            origin_query: None,
            queries: vec![],
            names: vec![],
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
        // TODO: need to get the metadata from database without really fetching the query result
        // (fill in column names to self.names, and column types to self.schema)

        // let db_connection = odbc_env.connect("mypostgresdb", "andyw", "")?; // Issue: authentication. 
        // if let Some(mut cursor) = db_connection.execute(metadata_query, parameters)? {
        //     fetch_metadata(&mut cursor)?;
        // }
    }

    #[throws(ODBCSourceError)]
    fn result_rows(&mut self) -> Option<usize> {
        // Issue: Lifetimes. For there to be any operation on the odbc environment, 
        // we need to create a connection. However, this connection must have the same lifetime as the environment.
        
        // TODO: This is essentially the same as my function! 

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
                self.connection_string.clone(),
                &query,
                &self.schema,
            ));
        }
        ret
    }
}

pub struct ODBCSourcePartition {
    env: Environment,
    buffer: TextRowSet, // unique 
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
            buffer: ColumnarBuffer::new(vec![]),
            connection_string,
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

        let conn = self.env.connect_with_connection_string(&self.connection_string).unwrap();
        
        let metadata_query = "SELECT * FROM cities LIMIT 1;";
        let mut cursor = conn.execute(metadata_query, ()).unwrap().unwrap();
        

        // Buffer Init
        // let batch_size = 1000; // Maximum number of rows in each row set
        // let buffer_description_list = [
        //     BufferDescription {
        //         kind: BufferKind::Text { max_str_len: 80 },
        //         nullable: true,
        //     },
        //     BufferDescription {
        //         kind: BufferKind::I32,
        //         nullable: true,
        //     }
        // ];
    
        // // Allocates a ColumnarBuffer fitting the buffer descriptions.
        // // capacity in usize
        // // Iterator whose items are buffer description
        // let buffer = ColumnarAnyBuffer::from_description(
        //     batch_size,
        //     buffer_description_list.iter().copied() // See Iterators and Copy vs Clone
        // );
        let mut buffer = init_buffer();

        let mut row_set_cursor = cursor.bind_buffer(&mut buffer).unwrap();

        // TODO: change the fake connection (e.g. username, password) to real one
        // with input connection string (dsn)
        // let connection = self
        //     .env
        //     .connect("YourDatabase", "SA", "My@Test@Password1")
        //     .unwrap();
        // let mut cursor = connection
        //     .into_cursor(self.query.as_str(), ())
        //     .unwrap()
        //     .unwrap();
        // self.buffer = TextRowSet::for_cursor(DB_BUFFER_SIZE, &mut cursor, Some(4096)).unwrap();
        // let row_set_cursor = cursor.bind_buffer(&mut self.buffer).unwrap();

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
        // if self.current_row > 0 {
        //     self.rows = OwningHandle::new_with_fn(
        //         self.rows.into_owner(),
        //         |cursor: *const RowSetCursor<
        //             CursorImpl<StatementConnection<'a>>,
        //             &'a mut TextRowSet,
        //         >| unsafe {
        //             DummyBox(
        //                 (&mut *(cursor
        //                     as *mut RowSetCursor<
        //                         CursorImpl<StatementConnection<'a>>,
        //                         &'a mut TextRowSet,
        //                     >))
        //                     .fetch()
        //                     .unwrap(),
        //             )
        //         },
        //     );
        // }

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

impl<'r, 'a> Produce<'r, Option<i32>> for ODBCSourcePartitionParser<'a> {
    type Error = ODBCSourceError;

    #[throws(ODBCSourceError)]
    fn produce(&'r mut self) -> Option<i32> {
        println!("What is going on");

        return Some(0);
    }
}


// Andy Helper Functions 

// for each column in row, describe the column 
// fn fetch_metadata(cursor: &mut impl Cursor, odbc_source: &mut ODBCSource) -> Result<(), Error> {
//     let columns = cursor.num_result_cols()?;

//     let mut column_description = ColumnDescription::default(); // Returns default value - like new()


//     for column_number in 1..(columns+1) { // column number starts at 1

//         cursor.describe_col(column_number.try_into().unwrap(), &mut column_description)?; // load column description

//         let col_name = column_description.name_to_string()?;
//         odbc_source.names.push(col_name);

//         let col_datatype = column_description.data_type;
//         let col_nullable = matches!(column_description.nullability, Nullability::Nullable);

//         // This will be replaced by 
//         // let col_datatype = ODBCTypeSystem::from(col_datatype, col_nullable); 
//         // how do you add the nullablility? 
//         let col_datatype = match col_datatype {
//             DataType::Integer => ODBCTypeSystem::Integer(col_nullable),
//             DataType::WVarchar{..} => ODBCTypeSystem::WVarChar(col_nullable),
//             _ => ODBCTypeSystem::Other(),
//         };
//         odbc_source.schema.push(col_datatype);

//     }

//     Ok(())
// }

fn get_table_rows(cursor: &mut impl Cursor) -> Result<(), Error> {
    if let Some(mut cursor_row) = cursor.next_row()? {
        let mut target: i32 = 0;
        cursor_row.get_data(1, &mut target)?;
        println!("{}", target);
    } else {
        println!("count_query returns nothing");
    }
    Ok(())
}

fn get_data_into_rust(cursor: impl Cursor) -> Result<(), Error> {

    let mut buffer = init_buffer();

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

fn init_buffer() -> ColumnarAnyBuffer {
    let batch_size = 1000; // Maximum number of rows in each row set
    let buffer_description_list = [
        BufferDescription {
            kind: BufferKind::Text { max_str_len: 80 },
            nullable: true,
        },
        BufferDescription {
            kind: BufferKind::I32,
            nullable: true,
        }
    ];

    // Allocates a ColumnarBuffer fitting the buffer descriptions.
    // capacity in usize
    // Iterator whose items are buffer description
    let buffer = ColumnarAnyBuffer::from_description(
        batch_size,
        buffer_description_list.iter().copied() // See Iterators and Copy vs Clone
    );

    buffer
}
