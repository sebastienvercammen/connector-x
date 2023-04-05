use connectorx::arrow_batch_iter::ArrowBatchIter;
use connectorx::prelude::*;
use connectorx::sources::postgres::{rewrite_tls_args, BinaryProtocol as PgBinaryProtocol};
use connectorx::transports::PostgresArrowTransport;
use postgres::NoTls;
use std::boxed::Box;
use std::convert::TryFrom;

fn main() {
    // let queries = &[CXQuery::naked("select * from test_table")];
    // let queries = &[
    //     CXQuery::naked("select * from test_table where test_int < 3"),
    //     CXQuery::naked("select * from test_table where test_int >= 3"),
    // ];

    let queries = &[
        CXQuery::naked("select * from lineitem where l_orderkey < 1000000"),
        CXQuery::naked(
            "select * from lineitem where l_orderkey >= 1000000 AND l_orderkey < 2000000",
        ),
        CXQuery::naked(
            "select * from lineitem where l_orderkey >= 2000000 AND l_orderkey < 3000000",
        ),
        CXQuery::naked(
            "select * from lineitem where l_orderkey >= 3000000 AND l_orderkey < 4000000",
        ),
        CXQuery::naked(
            "select * from lineitem where l_orderkey >= 4000000 AND l_orderkey < 5000000",
        ),
        CXQuery::naked("select * from lineitem where l_orderkey >= 5000000"),
    ];

    let origin_query = None;

    let conn = "postgresql://postgres:postgres@localhost:5432/tpch";
    let source = SourceConn::try_from(conn).unwrap();
    let (config, _) = rewrite_tls_args(&source.conn).unwrap();
    let source =
        PostgresSource::<PgBinaryProtocol, NoTls>::new(config, NoTls, queries.len()).unwrap();

    let destination = ArrowDestination::new_with_batch_size(2048);

    let batch_iter: ArrowBatchIter<_, PostgresArrowTransport<PgBinaryProtocol, NoTls>> =
        ArrowBatchIter::new(source, destination, origin_query, queries).unwrap();

    let batch_iter = Box::new(batch_iter);
    let batch_iter = Box::into_raw(batch_iter);

    let ptr = batch_iter as usize;

    std::thread::scope(|s| {
        for i in 0..queries.len() {
            println!("spawn {}", i);
            s.spawn(|| unsafe {
                let iter =
                    ptr as *mut ArrowBatchIter<_, PostgresArrowTransport<PgBinaryProtocol, NoTls>>;
                let id = (*iter).prepare();
                let mut num_rows = 0;
                let mut num_batches = 0;

                while let Some(record_batch) = (*iter).next_batch(id as usize) {
                    let record_batch = record_batch;
                    println!(
                        "part {} got 1 batch, with {} rows",
                        id,
                        record_batch.num_rows()
                    );
                    num_rows += record_batch.num_rows();
                    num_batches += 1;
                    // arrow::util::pretty::print_batches(&[record_batch]).unwrap();
                }
                println!(
                    "part {} got {} batches, {} rows in total",
                    id, num_batches, num_rows
                );
            });
        }
    });

    unsafe {
        Box::from_raw(
            ptr as *mut ArrowBatchIter<_, PostgresArrowTransport<PgBinaryProtocol, NoTls>>,
        )
    };
}
