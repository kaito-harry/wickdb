use std::{ops::Range, sync::Arc};

use criterion::{criterion_group, criterion_main, Criterion};
use wickdb::{
    db::format::{InternalKey, InternalKeyComparator, LookupKey},
    mem::MemTable,
    BytewiseComparator, Iterator,
};

const RANGE: Range<u64> = 10000..99999;
const MAX_MEM_SIZE: usize = 8 * 1024 * 1024;
const SEQ: u64 = 111111;

fn memtable_put_and_get() {
    let memtable = build_memtable(RANGE.clone(), MAX_MEM_SIZE);
    for i in RANGE {
        let key = format!("key {}", i);
        let value = format!("value {}", i);
        let lookup_key = LookupKey::new(key.as_bytes(), SEQ);
        let result = memtable.get(&lookup_key);
        assert_eq!(result.unwrap().unwrap().as_slice(), value.as_bytes());
    }
}

fn build_memtable(range: Range<u64>, max_mem_size: usize) -> MemTable<BytewiseComparator> {
    let comparator = BytewiseComparator::default();
    let memtable = MemTable::new(max_mem_size, InternalKeyComparator::new(comparator));
    for i in range {
        let key = format!("key {}", i);
        let value = format!("value {}", i);
        memtable.add(
            i,
            wickdb::db::format::ValueType::Value,
            key.as_bytes(),
            value.as_bytes(),
        );
    }

    memtable
}

pub fn memtable_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable");

    group.bench_function("memtable put and get", |b| {
        b.iter(|| memtable_put_and_get())
    });

    let memtable = Arc::new(build_memtable(RANGE, MAX_MEM_SIZE));
    let memtable_ref = &memtable;
    group.bench_function("memtable iter", |b| {
        b.iter(|| {
            let mut iter = memtable_ref.iter();
            iter.seek_to_first();
            for i in RANGE {
                assert!(iter.valid());

                let key = iter.key();
                let value = iter.value();
                assert_eq!(
                    InternalKey::decoded_from(key).user_key(),
                    format!("key {}", i).as_bytes()
                );
                assert_eq!(value, format!("value {}", i).as_bytes());

                iter.next();
            }
        })
    });
}

criterion_group!(benches, memtable_benchmark);
criterion_main!(benches);
