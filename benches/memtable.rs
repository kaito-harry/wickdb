use std::{ops::Range, sync::Arc};

use criterion::{criterion_group, criterion_main, Criterion};
use wickdb::{
    db::format::{InternalKeyComparator, LookupKey},
    mem::MemTable,
    BytewiseComparator, Iterator,
};

fn memtable_put_and_get() {
    let range = Range {
        start: 10000,
        end: 99999,
    };
    let max_mem_size = 8 * 1024 * 1024;
    let seq = 111111;

    let comparator = BytewiseComparator::default();
    let memtable = MemTable::new(max_mem_size, InternalKeyComparator::new(comparator));
    for i in range.clone() {
        let key = format!("key {}", i);
        let value = format!("value {}", i);
        memtable.add(
            i,
            wickdb::db::format::ValueType::Value,
            key.as_bytes(),
            value.as_bytes(),
        );
    }
    for i in range {
        let key = format!("key {}", i);
        let value = format!("value {}", i);
        let lookup_key = LookupKey::new(key.as_bytes(), seq);
        let result = memtable.get(&lookup_key);
        assert_eq!(result.unwrap().unwrap().as_slice(), value.as_bytes());
    }
}

fn build_memtable() -> MemTable<BytewiseComparator> {
    let range = Range {
        start: 10000,
        end: 99999,
    };
    let max_mem_size = 8 * 1024 * 1024;
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

    let memtable = Arc::new(build_memtable());
    let memtable_ref = &memtable;
    group.bench_function("memtable iter", |b| {
        b.iter(|| {
            let mut iter = memtable_ref.iter();
            iter.seek_to_first();
            let range = Range {
                start: 10000,
                end: 99999,
            };
            for i in range {
                assert!(iter.valid());
                let key = iter.key();
                let value = iter.value();
                let key_str = std::str::from_utf8(&key[0..5]).unwrap();
                let value_str = std::str::from_utf8(value).unwrap();
                // assert_eq!(key_str, format!("key {}", i));
                assert_eq!(value_str, format!("value {}", i));

                iter.next();
            }
        })
    });
}

criterion_group!(benches, memtable_benchmark);
criterion_main!(benches);
