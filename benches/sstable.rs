use std::fs::File;
use std::ops::Range;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use wickdb::db::format::InternalKey;
use wickdb::sstable::table::{new_table_iterator, Table, TableBuilder};
use wickdb::ReadOptions;
use wickdb::{
    db::{
        filename::{generate_filename, FileType},
        format::InternalKeyComparator,
    },
    mem::{MemTable, MemTableIterator},
    BytewiseComparator, Iterator as _, Options,
};

const RANGE1: Range<u64> = 10000..99999;
const RANGE2: Range<u64> = 100..999;
const VALUE_REPEAT: usize = 128;
const MAX_MEM_SIZE: usize = 8 * 1024 * 1024;
const SEQ: u64 = 111111;
const DB_PATH: &str = "/tmp/wickdb/sstable_iter";

fn build_memtable(
    range: Range<u64>,
    max_mem_size: usize,
    repeat_value: usize,
) -> MemTable<BytewiseComparator> {
    let comparator = BytewiseComparator::default();
    let memtable = MemTable::new(max_mem_size, InternalKeyComparator::new(comparator));
    for i in range {
        memtable.add(
            i,
            wickdb::db::format::ValueType::Value,
            format!("key {}", i).as_bytes(),
            format!("value {}", i).repeat(repeat_value).as_bytes(),
        );
    }

    memtable
}

// Build a Table file from the contents of `iter`.  The generated file
// will be named according to `meta.number`.  On success, the rest of
// meta will be filled with metadata about the generated table.
// If no data is present in iter, `meta.file_size` will be set to
// zero, and no Table file will be produced.
pub fn build_table(
    db_path: &str,
    mut iter: MemTableIterator<BytewiseComparator>,
    options: Arc<Options<BytewiseComparator>>,
    seq: u64,
) -> u64 {
    let _ = std::fs::create_dir_all(db_path);
    let filename = generate_filename(db_path, FileType::Table, seq);
    println!("filename: {:?}", filename);

    let file = std::fs::File::create(&filename).unwrap();
    let icmp = InternalKeyComparator::new(options.comparator.clone());
    let mut builder = TableBuilder::new(file, icmp.clone(), &options);

    iter.seek_to_first();
    assert!(iter.valid());
    while iter.valid() {
        let key = iter.key().to_vec();
        builder.add(&key, iter.value()).unwrap();
        iter.next();
    }

    let file_size = builder
        .finish(true)
        .and_then(|_| {
            let file_size = builder.file_size();
            println!("file_size: {:?}KB", file_size / 1024);
            Ok(file_size)
        })
        .unwrap();

    iter.status().unwrap();
    file_size
}

pub fn sstable_benchmark(c: &mut Criterion) {
    let comparator = BytewiseComparator::default();
    let icmp = InternalKeyComparator::new(comparator);
    let options = Arc::new(Options::<BytewiseComparator>::default());

    // sstable with small value and range 10000..99999
    let memtable = build_memtable(RANGE1, MAX_MEM_SIZE, 1);
    let file_size = build_table(DB_PATH, memtable.iter(), options.clone(), SEQ);
    let filename = generate_filename(DB_PATH, FileType::Table, SEQ);
    let table_file = File::open(filename).unwrap();
    let table = Table::open(table_file, SEQ, file_size, options.clone(), icmp.clone()).unwrap();

    let table = Arc::new(table);
    let table_ref = &table;
    let icmp_ref = &icmp;
    c.bench_function("sstable iter with range 10000..99999 (small value)", |b| {
        b.iter(|| {
            let mut iter =
                new_table_iterator(icmp_ref.clone(), table_ref.clone(), ReadOptions::default());
            iter.seek_to_first();

            for i in RANGE1 {
                assert!(iter.valid());

                let ikey = InternalKey::decoded_from(iter.key());
                let value = iter.value();
                assert_eq!(ikey.user_key(), format!("key {}", i).as_bytes());
                assert_eq!(value, format!("value {}", i).as_bytes());

                iter.next();
            }
        })
    });
    // clear db path
    let _ = std::fs::remove_dir_all(DB_PATH);

    // sstable with big value and range 100..999
    let memtable = build_memtable(RANGE2, MAX_MEM_SIZE, VALUE_REPEAT);
    let file_size = build_table(DB_PATH, memtable.iter(), options.clone(), SEQ);
    let filename = generate_filename(DB_PATH, FileType::Table, SEQ);
    let table_file = File::open(filename).unwrap();
    let table = Table::open(table_file, SEQ, file_size, options.clone(), icmp.clone()).unwrap();

    let table = Arc::new(table);
    let table_ref = &table;
    c.bench_function("sstable iter with range 100..999 (big value)", |b| {
        b.iter(|| {
            let mut iter =
                new_table_iterator(icmp_ref.clone(), table_ref.clone(), ReadOptions::default());
            iter.seek_to_first();

            for i in RANGE2 {
                assert!(iter.valid());

                let ikey = InternalKey::decoded_from(iter.key());
                let value = iter.value();
                assert_eq!(ikey.user_key(), format!("key {}", i).as_bytes());
                assert_eq!(
                    value,
                    format!("value {}", i).as_bytes().repeat(VALUE_REPEAT)
                );

                iter.next();
            }
        })
    });
}

criterion_group!(benches, sstable_benchmark);
criterion_main!(benches);
