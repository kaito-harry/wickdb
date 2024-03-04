use std::fs::File;
use std::sync::Arc;

use wickdb::sstable::table::{self, new_table_iterator, Table, TableBuilder};
use wickdb::{
    db::{
        filename::{generate_filename, FileType},
        format::InternalKeyComparator,
    },
    mem::{MemTable, MemTableIterator},
    BytewiseComparator, Iterator as _, Options,
};
use wickdb::{file, ReadOptions};

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
    iter.seek_to_first();
    let file_name = generate_filename(db_path, FileType::Table, seq);
    println!("file_name: {}", file_name);
    let mut file_size = 0;
    let mut status = Ok(());
    if iter.valid() {
        let _ = std::fs::create_dir_all(db_path);
        let file = std::fs::File::create(&file_name).unwrap();
        let icmp = InternalKeyComparator::new(options.comparator.clone());
        let mut builder = TableBuilder::new(file, icmp.clone(), &options);
        while iter.valid() {
            let key = iter.key().to_vec();
            let s = builder.add(&key, iter.value());
            if s.is_err() {
                status = s;
                break;
            }
            iter.next();
        }
        status.unwrap();

        file_size = builder
            .finish(true)
            .and_then(|_| {
                let file_size = builder.file_size();
                println!("file_size: {}", file_size);
                Ok(file_size)
            })
            .unwrap();
    }

    iter.status().unwrap();
    file_size
}

fn main() {
    let max_mem_size = 8 * 1024 * 1024;
    let seq = 111111;
    let db_path = "/tmp/sstable_get";
    let file_path = format!("{}/{}.sst", db_path, seq);
    let comparator = BytewiseComparator::default();
    let icmp = InternalKeyComparator::new(comparator);
    let options = Arc::new(Options::<BytewiseComparator>::default());

    let memtable = MemTable::new(max_mem_size, icmp.clone());
    for i in 10000..99999 {
        let key = format!("key {}", i);
        let value = format!("value {}", i);
        memtable.add(
            i,
            wickdb::db::format::ValueType::Value,
            key.as_bytes(),
            value.as_bytes(),
        );
    }
    let iter = memtable.iter();
    let file_size = build_table("/tmp/sstable_get", iter, options.clone(), seq);

    let table_file = File::open(file_path).unwrap();
    let file_number = seq;
    let table = Table::open(
        table_file,
        file_number,
        file_size,
        options.clone(),
        icmp.clone(),
    )
    .unwrap();
    let table = Arc::new(table);
    let mut iter = new_table_iterator(icmp, table, ReadOptions::default());
    iter.seek_to_first();

    let mut expect_key = 10000;
    while iter.valid() {
        let key = iter.key();
        let value = iter.value();
        let key_str = std::str::from_utf8(&key[0..5]).unwrap();
        let value_str = std::str::from_utf8(value).unwrap();
        // assert key,value
        // assert_eq!(key_str, format!("key {}", expect_key));
        assert_eq!(value_str, format!("value {}", expect_key));
        iter.next();
        expect_key += 1;
    }
}
