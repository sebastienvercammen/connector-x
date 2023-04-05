use crate::{prelude::*, utils::*};
use arrow::record_batch::RecordBatch;
use itertools::Itertools;
use owning_ref::OwningHandle;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};

type SourceParserHandle<'a, S> = OwningHandle<
    Box<<S as Source>::Partition>,
    DummyBox<<<S as Source>::Partition as SourcePartition>::Parser<'a>>,
>;

/// The iterator that returns arrow in `RecordBatch`
pub struct ArrowBatchIter<'a, S, TP>
where
    S: Source + 'a,
    TP: Transport<TSS = S::TypeSystem, TSD = ArrowTypeSystem, S = S, D = ArrowDestination>,
{
    dst: ArrowDestination,
    dst_parts: Vec<ArrowPartitionWriter>,
    src_parts: Vec<Option<S::Partition>>,
    src_parsers: Vec<Option<SourceParserHandle<'a, S>>>,
    dorder: DataOrder,
    schema: Vec<(S::TypeSystem, ArrowTypeSystem)>,
    part_idx: AtomicUsize,
    _phantom: PhantomData<TP>,
}

impl<'a, S, TP> ArrowBatchIter<'a, S, TP>
where
    S: Source + 'a,
    TP: Transport<TSS = S::TypeSystem, TSD = ArrowTypeSystem, S = S, D = ArrowDestination>,
{
    pub fn new(
        src: S,
        mut dst: ArrowDestination,
        origin_query: Option<String>,
        queries: &[CXQuery<String>],
    ) -> Result<Self, TP::Error> {
        let dispatcher = Dispatcher::<_, _, TP>::new(src, &mut dst, queries, origin_query);
        let (dorder, src_parts, mut dst_parts, src_schema, dst_schema) = dispatcher.prepare()?;
        let schema: Vec<_> = src_schema
            .into_iter()
            .zip_eq(dst_schema)
            .map(|(src_ty, dst_ty)| (src_ty, dst_ty))
            .collect();

        // set arrow dst partitions as detached, so we can access them without lock
        dst_parts.iter_mut().for_each(|d| d.set_detach());

        let mut src_parsers = Vec::new();
        src_parsers.resize_with(src_parts.len(), || None);

        Ok(Self {
            dst,
            dst_parts,
            src_parts: src_parts.into_iter().map(Some).collect(),
            src_parsers,
            dorder,
            schema,
            part_idx: AtomicUsize::new(0),
            _phantom: PhantomData,
        })
    }

    fn run_batch(&mut self, id: usize) -> Result<Option<RecordBatch>, TP::Error> {
        let dst = &mut self.dst_parts[id];
        let parser = (&mut self.src_parsers[id]).as_deref_mut().unwrap();

        match self.dorder {
            DataOrder::RowMajor => loop {
                let (n, is_last) = parser.fetch_next()?;
                dst.aquire_row(n)?;
                for _ in 0..n {
                    #[allow(clippy::needless_range_loop)]
                    for col in 0..dst.ncols() {
                        {
                            let (s1, s2) = self.schema[col];
                            TP::process(s1, s2, parser, dst)?;
                        }
                    }
                }
                if is_last || dst.is_batch_full() {
                    break;
                }
            },
            DataOrder::ColumnMajor => loop {
                let (n, is_last) = parser.fetch_next()?;
                dst.aquire_row(n)?;
                #[allow(clippy::needless_range_loop)]
                for col in 0..dst.ncols() {
                    for _ in 0..n {
                        {
                            let (s1, s2) = self.schema[col];
                            TP::process(s1, s2, parser, dst)?;
                        }
                    }
                }
                if is_last || dst.is_batch_full() {
                    break;
                }
            },
        }
        Ok(dst.generate_batch()?)
    }
}

pub trait RecordBatchIterator {
    fn get_schema(&self) -> (RecordBatch, &[String]);
    fn prepare(&mut self) -> i32;
    fn next_batch(&mut self, id: usize) -> Option<RecordBatch>;
}

impl<'a, S, TP> RecordBatchIterator for ArrowBatchIter<'a, S, TP>
where
    S: Source + 'a,
    TP: Transport<TSS = S::TypeSystem, TSD = ArrowTypeSystem, S = S, D = ArrowDestination>,
{
    fn get_schema(&self) -> (RecordBatch, &[String]) {
        (self.dst.empty_batch(), self.dst.names())
    }

    fn prepare(&mut self) -> i32 {
        let id = self.part_idx.fetch_add(1, Ordering::SeqCst);
        if id >= self.src_parts.len() {
            return -1;
        }
        let src_part = self.src_parts[id].take().unwrap();
        self.src_parsers[id] = Some(OwningHandle::new_with_fn(
            Box::new(src_part),
            |src_part: *const S::Partition| unsafe {
                DummyBox((*(src_part as *mut S::Partition)).parser().unwrap())
            },
        ));
        id as i32
    }

    fn next_batch(&mut self, id: usize) -> Option<RecordBatch> {
        self.run_batch(id).unwrap()
    }
}
