//! Quick micro-benchmarks: `cargo run --release --example bench`
use std::hint::black_box;
use std::time::Instant;

use hll_node::hll::{DEFAULT_SPARSE_THRESHOLD_BYTES as T, Hll, hash_item};

fn main() {
    let n = 10_000_000u64;
    let hashes: Vec<u64> = (0..n).map(|i| hash_item(&i.to_le_bytes())).collect();

    let mut h = Hll::new(14, T).unwrap();
    let t = Instant::now();
    h.insert_hashes(&hashes);
    let ins = t.elapsed();
    println!("insert:   {:.1} M items/s ({} items, est {} err {:+.3}%)",
        n as f64 / ins.as_secs_f64() / 1e6, n, h.estimate(),
        (h.estimate() as f64 - n as f64) / n as f64 * 100.0);

    let mut a = Hll::new(14, T).unwrap();
    a.insert_hashes(&hashes[..1_000_000]);
    let mut b = Hll::new(14, T).unwrap();
    b.insert_hashes(&hashes[1_000_000..2_000_000]);
    let iters = 100_000;
    let t = Instant::now();
    for _ in 0..iters {
        let mut x = black_box(a.clone());
        black_box(x.merge(black_box(&b)).unwrap());
    }
    let with_clone = t.elapsed().as_secs_f64() / iters as f64;
    let t = Instant::now();
    for _ in 0..iters {
        black_box(black_box(a.clone()));
    }
    let clone_only = t.elapsed().as_secs_f64() / iters as f64;
    println!("merge:    {:.2} µs per 16 KiB dense merge", (with_clone - clone_only).max(0.0) * 1e6);

    let t = Instant::now();
    for _ in 0..iters { black_box(black_box(&h).estimate()); }
    println!("estimate: {:.2} µs", t.elapsed().as_secs_f64() / iters as f64 * 1e6);

    let (enc, bytes) = h.to_wire();
    println!("wire:     {:?} {} bytes (raw 16384)", enc, bytes.len());
}
