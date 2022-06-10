// What should be in here? 
pub struct ODBCSource {
    teststring: String,
    // pool
    // origin query
    // queries 
    // names
    // schema 
}

impl ODBCSource {
    pub fn new() -> Self {
        let teststring = "hello, world!";
        Self { teststring }
    }
}

impl Source for ODBCSource {

}


pub trait Source {
    // type Partition: SourcePartition<TypeSystem = Self::TypeSystem, Error = Self::Error> + Send; // Partition needs to be send to different threads for parallel execution
 
    const DATA_ORDERS: &'static [DataOrder];    /// Supported data orders, ordering by preference.
    type TypeSystem: TypeSystem;     /// The type system this `Source` associated with.
    type Error: From<ConnectorXError>;

    fn set_data_order(&mut self, data_order: DataOrder) -> Result<(), Self::Error>;

    fn set_queries<Q: ToString>(&mut self, queries: &[CXQuery<Q>]);

    fn set_origin_query(&mut self, query: Option<String>);

    fn fetch_metadata(&mut self) -> Result<(), Self::Error>;
    /// Get total number of rows if available
    fn result_rows(&mut self) -> Result<Option<usize>, Self::Error>;

    fn names(&self) -> Vec<String>;

    fn schema(&self) -> Vec<Self::TypeSystem>;

    fn partition(self) -> Result<Vec<Self::Partition>, Self::Error>;
}
