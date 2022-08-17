// Running Tests: just test --test test_odbc

use connectorx::sources::odbc::ODBCSource;
use connectorx::prelude::ArrowDestination;
use connectorx::prelude::Dispatcher;
use connectorx::sql::CXQuery;
use connectorx::transports::ODBCArrowTransport;
// may need more preludes 

// use arrow::{
//     array::{BooleanArray, Float64Array, Int64Array, StringArray},
//     record_batch::RecordBatch,
// };

#[test] // ODBC Test 
fn test_odbc() {

    let database_url = "
        Driver={PostgreSQL ANSI};\
        Server=localhost;\
        Database=mypostgresdb;\
        UID=testuser;\
        PWD=test123;\
    ";

    let queries = [
        CXQuery::naked("select population from cities where population > 1000000"), // 3 rows 
        CXQuery::naked("select population from cities where population <= 1000000"), // 7 rows 
    ];

    let builder = ODBCSource::new(database_url).unwrap();
    let mut destination = ArrowDestination::new();
    
    let dispatcher = Dispatcher::<_, _, ODBCArrowTransport>::new(
        builder,
        &mut destination,
        &queries,
        Some(String::from("select * from cities LIMIT 1;")), // Why not a CX Query? 
    );
    dispatcher.run().unwrap();
    let result = destination.arrow().unwrap();
} 