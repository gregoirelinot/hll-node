//! HyperLogLog sketch with sparse/dense representations and Ertl's improved
//! raw estimator (O. Ertl, "New cardinality estimation algorithms for
//! HyperLogLog sketches", 2017), which needs no empirical bias tables and is
//! unbiased across the whole cardinality range.
//!
//! Hash layout for a pre-hashed `u64`:
//!   * top `p` bits           -> register index
//!   * remaining `64 - p` bits -> rho = (leading zeros + 1), capped at `q + 1`
//!     where `q = 64 - p`.

use std::fmt;

/// Minimum supported precision.
pub const MIN_PRECISION: u8 = 4;
/// Maximum supported precision (index must fit in 26 bits of a sparse entry).
pub const MAX_PRECISION: u8 = 18;
/// Default sparse -> dense promotion threshold (bytes of sparse payload).
pub const DEFAULT_SPARSE_THRESHOLD_BYTES: usize = 4 * 1024;

const RHO_BITS: u32 = 6;
const RHO_MASK: u32 = (1 << RHO_BITS) - 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HllError {
    InvalidPrecision(u8),
    PrecisionMismatch { left: u8, right: u8 },
    Corrupt(&'static str),
}

impl fmt::Display for HllError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HllError::InvalidPrecision(p) => write!(
                f,
                "invalid precision {p} (supported {MIN_PRECISION}..={MAX_PRECISION})"
            ),
            HllError::PrecisionMismatch { left, right } => {
                write!(f, "precision mismatch: {left} vs {right}")
            }
            HllError::Corrupt(why) => write!(f, "corrupt HLL payload: {why}"),
        }
    }
}

impl std::error::Error for HllError {}

#[derive(Clone, PartialEq, Eq)]
enum Repr {
    /// Sorted (by register index), de-duplicated `(idx << 6) | rho` entries.
    Sparse(Vec<u32>),
    /// One byte per register.
    Dense(Box<[u8]>),
}

#[derive(Clone, PartialEq, Eq)]
pub struct Hll {
    p: u8,
    /// Max number of sparse entries before promoting to dense.
    sparse_limit: u32,
    repr: Repr,
}

impl fmt::Debug for Hll {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hll")
            .field("p", &self.p)
            .field("mode", &if self.is_dense() { "dense" } else { "sparse" })
            .field("estimate", &self.estimate())
            .finish()
    }
}

/// Wire encodings (mirror `hll.v1.SnapshotEncoding`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    DenseRaw = 0,
    DenseZstd = 1,
    Sparse = 2,
}

impl Encoding {
    pub fn from_i32(v: i32) -> Option<Self> {
        match v {
            0 => Some(Encoding::DenseRaw),
            1 => Some(Encoding::DenseZstd),
            2 => Some(Encoding::Sparse),
            _ => None,
        }
    }
}

#[inline]
fn encode(idx: u32, rho: u8) -> u32 {
    (idx << RHO_BITS) | rho as u32
}

#[inline]
fn entry_idx(e: u32) -> u32 {
    e >> RHO_BITS
}

#[inline]
fn entry_rho(e: u32) -> u8 {
    (e & RHO_MASK) as u8
}

impl Hll {
    /// New empty sketch (starts sparse).
    pub fn new(p: u8, sparse_threshold_bytes: usize) -> Result<Self, HllError> {
        if !(MIN_PRECISION..=MAX_PRECISION).contains(&p) {
            return Err(HllError::InvalidPrecision(p));
        }
        let m = 1usize << p;
        // Never let sparse grow beyond 1/4 of the dense size: past that point
        // dense is both smaller and faster.
        let limit = (sparse_threshold_bytes / 4).min(m / 4).max(1) as u32;
        Ok(Self {
            p,
            sparse_limit: limit,
            repr: Repr::Sparse(Vec::new()),
        })
    }

    pub fn precision(&self) -> u8 {
        self.p
    }

    pub fn num_registers(&self) -> usize {
        1usize << self.p
    }

    pub fn is_dense(&self) -> bool {
        matches!(self.repr, Repr::Dense(_))
    }

    #[inline]
    fn q(&self) -> u32 {
        64 - self.p as u32
    }

    /// Approximate heap footprint in bytes.
    pub fn heap_bytes(&self) -> usize {
        match &self.repr {
            Repr::Sparse(v) => v.capacity() * 4,
            Repr::Dense(d) => d.len(),
        }
    }

    #[inline]
    fn split(&self, hash: u64) -> (u32, u8) {
        let p = self.p as u32;
        let idx = (hash >> (64 - p)) as u32;
        let w = hash << p;
        let max_rho = self.q() + 1;
        let rho = if w == 0 {
            max_rho
        } else {
            (w.leading_zeros() + 1).min(max_rho)
        };
        (idx, rho as u8)
    }

    /// Insert one pre-hashed item. Returns `true` if any register changed.
    #[inline]
    pub fn insert_hash(&mut self, hash: u64) -> bool {
        let (idx, rho) = self.split(hash);
        self.set_max(idx, rho)
    }

    /// Insert many pre-hashed items. Returns `true` if any register changed.
    pub fn insert_hashes(&mut self, hashes: &[u64]) -> bool {
        let mut modified = false;
        for &h in hashes {
            modified |= self.insert_hash(h);
        }
        modified
    }

    fn set_max(&mut self, idx: u32, rho: u8) -> bool {
        match &mut self.repr {
            Repr::Dense(regs) => {
                let r = &mut regs[idx as usize];
                if rho > *r {
                    *r = rho;
                    true
                } else {
                    false
                }
            }
            Repr::Sparse(entries) => {
                match entries.binary_search_by_key(&idx, |&e| entry_idx(e)) {
                    Ok(pos) => {
                        if rho > entry_rho(entries[pos]) {
                            entries[pos] = encode(idx, rho);
                            true
                        } else {
                            false
                        }
                    }
                    Err(pos) => {
                        entries.insert(pos, encode(idx, rho));
                        if entries.len() as u32 > self.sparse_limit {
                            self.promote();
                        }
                        true
                    }
                }
            }
        }
    }

    /// Force conversion to the dense representation.
    pub fn promote(&mut self) {
        if let Repr::Sparse(entries) = &self.repr {
            let mut regs = vec![0u8; self.num_registers()].into_boxed_slice();
            for &e in entries {
                regs[entry_idx(e) as usize] = entry_rho(e);
            }
            self.repr = Repr::Dense(regs);
        }
    }

    /// CRDT merge: register-wise max. Returns `true` if `self` changed.
    pub fn merge(&mut self, other: &Hll) -> Result<bool, HllError> {
        if self.p != other.p {
            return Err(HllError::PrecisionMismatch {
                left: self.p,
                right: other.p,
            });
        }
        let modified = match (&mut self.repr, &other.repr) {
            (Repr::Dense(a), Repr::Dense(b)) => merge_dense(a, b),
            (Repr::Dense(a), Repr::Sparse(b)) => {
                let mut changed = false;
                for &e in b {
                    let r = &mut a[entry_idx(e) as usize];
                    let rho = entry_rho(e);
                    if rho > *r {
                        *r = rho;
                        changed = true;
                    }
                }
                changed
            }
            (Repr::Sparse(_), Repr::Dense(b)) => {
                self.promote();
                let Repr::Dense(a) = &mut self.repr else {
                    unreachable!()
                };
                merge_dense(a, b)
            }
            (Repr::Sparse(a), Repr::Sparse(b)) => {
                let (merged, changed) = merge_sparse(a, b);
                *a = merged;
                if a.len() as u32 > self.sparse_limit {
                    self.promote();
                }
                changed
            }
        };
        Ok(modified)
    }

    /// Register histogram `C[k]` for k in `0..=q+1`.
    fn histogram(&self) -> Vec<u32> {
        let q = self.q() as usize;
        let mut c = vec![0u32; q + 2];
        match &self.repr {
            Repr::Dense(regs) => {
                for &r in regs.iter() {
                    c[r as usize] += 1;
                }
            }
            Repr::Sparse(entries) => {
                c[0] = (self.num_registers() - entries.len()) as u32;
                for &e in entries {
                    c[entry_rho(e) as usize] += 1;
                }
            }
        }
        c
    }

    /// Cardinality estimate (Ertl's improved raw estimator).
    pub fn estimate(&self) -> u64 {
        let m = self.num_registers() as f64;
        let q = self.q() as usize;
        let c = self.histogram();
        if c[0] as usize == self.num_registers() {
            return 0;
        }
        let mut z = m * tau(1.0 - c[q + 1] as f64 / m);
        for k in (1..=q).rev() {
            z = 0.5 * (z + c[k] as f64);
        }
        z += m * sigma(c[0] as f64 / m);
        let alpha_inf = 0.5 / std::f64::consts::LN_2;
        (alpha_inf * m * m / z).round() as u64
    }

    /// Serialize for the wire. Sparse sketches are sent as-is; dense sketches
    /// are zstd-compressed when that actually saves bytes.
    pub fn to_wire(&self) -> (Encoding, Vec<u8>) {
        match &self.repr {
            Repr::Sparse(entries) => {
                let mut out = Vec::with_capacity(entries.len() * 4);
                for &e in entries {
                    out.extend_from_slice(&e.to_le_bytes());
                }
                (Encoding::Sparse, out)
            }
            Repr::Dense(regs) => match zstd::bulk::compress(regs, 1) {
                Ok(c) if c.len() < regs.len() => (Encoding::DenseZstd, c),
                _ => (Encoding::DenseRaw, regs.to_vec()),
            },
        }
    }

    /// Deserialize and validate a wire payload.
    pub fn from_wire(
        p: u8,
        encoding: Encoding,
        bytes: &[u8],
        sparse_threshold_bytes: usize,
    ) -> Result<Self, HllError> {
        let mut hll = Hll::new(p, sparse_threshold_bytes)?;
        let m = hll.num_registers();
        let max_rho = (hll.q() + 1) as u8;
        match encoding {
            Encoding::DenseRaw | Encoding::DenseZstd => {
                let regs: Vec<u8> = if encoding == Encoding::DenseZstd {
                    zstd::bulk::decompress(bytes, m)
                        .map_err(|_| HllError::Corrupt("zstd decompression failed"))?
                } else {
                    bytes.to_vec()
                };
                if regs.len() != m {
                    return Err(HllError::Corrupt("dense register count mismatch"));
                }
                if regs.iter().any(|&r| r > max_rho) {
                    return Err(HllError::Corrupt("register value out of range"));
                }
                hll.repr = Repr::Dense(regs.into_boxed_slice());
            }
            Encoding::Sparse => {
                if !bytes.len().is_multiple_of(4) {
                    return Err(HllError::Corrupt("sparse payload not a multiple of 4"));
                }
                let mut entries = Vec::with_capacity(bytes.len() / 4);
                let mut prev: Option<u32> = None;
                for chunk in bytes.chunks_exact(4) {
                    let e = u32::from_le_bytes(chunk.try_into().unwrap());
                    let idx = entry_idx(e);
                    let rho = entry_rho(e);
                    if idx as usize >= m || rho == 0 || rho > max_rho {
                        return Err(HllError::Corrupt("sparse entry out of range"));
                    }
                    if prev.is_some_and(|p| p >= idx) {
                        return Err(HllError::Corrupt("sparse entries not strictly sorted"));
                    }
                    prev = Some(idx);
                    entries.push(e);
                }
                let over = entries.len() as u32 > hll.sparse_limit;
                hll.repr = Repr::Sparse(entries);
                if over {
                    hll.promote();
                }
            }
        }
        Ok(hll)
    }
}

/// Branch-free register-wise max; LLVM auto-vectorizes both loops
/// (`vpmaxub` on x86-64, `umax`/`vmaxq_u8` on aarch64).
fn merge_dense(a: &mut [u8], b: &[u8]) -> bool {
    debug_assert_eq!(a.len(), b.len());
    let changed = a.iter().zip(b).fold(0u8, |acc, (&x, &y)| acc | (y > x) as u8) != 0;
    if changed {
        for (x, &y) in a.iter_mut().zip(b) {
            *x = (*x).max(y);
        }
    }
    changed
}

fn merge_sparse(a: &[u32], b: &[u32]) -> (Vec<u32>, bool) {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    let mut changed = false;
    while i < a.len() && j < b.len() {
        let (ia, ib) = (entry_idx(a[i]), entry_idx(b[j]));
        if ia < ib {
            out.push(a[i]);
            i += 1;
        } else if ib < ia {
            out.push(b[j]);
            j += 1;
            changed = true;
        } else {
            if entry_rho(b[j]) > entry_rho(a[i]) {
                out.push(b[j]);
                changed = true;
            } else {
                out.push(a[i]);
            }
            i += 1;
            j += 1;
        }
    }
    out.extend_from_slice(&a[i..]);
    if j < b.len() {
        changed = true;
        out.extend_from_slice(&b[j..]);
    }
    (out, changed)
}

fn sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let mut y = 1.0;
    let mut z = x;
    loop {
        x *= x;
        let z_prev = z;
        z += x * y;
        y += y;
        if z == z_prev {
            return z;
        }
    }
}

fn tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let mut y = 1.0;
    let mut z = 1.0 - x;
    loop {
        x = x.sqrt();
        let z_prev = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if z == z_prev {
            return z / 3.0;
        }
    }
}

/// Hash a raw item the same way on every node and architecture.
#[inline]
pub fn hash_item(bytes: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hashes(range: std::ops::Range<u64>, salt: u64) -> Vec<u64> {
        range
            .map(|i| hash_item(&(i ^ (salt << 48)).to_le_bytes()))
            .collect()
    }

    fn rel_err(est: u64, truth: u64) -> f64 {
        (est as f64 - truth as f64).abs() / truth as f64
    }

    #[test]
    fn empty_is_zero() {
        let h = Hll::new(14, DEFAULT_SPARSE_THRESHOLD_BYTES).unwrap();
        assert_eq!(h.estimate(), 0);
        assert!(!h.is_dense());
    }

    #[test]
    fn rejects_bad_precision() {
        assert!(Hll::new(3, 4096).is_err());
        assert!(Hll::new(19, 4096).is_err());
    }

    #[test]
    fn accuracy_across_ranges() {
        // σ ≈ 0.81% at p=14; allow ~3σ per individual sample.
        for &n in &[10u64, 100, 1_000, 10_000, 100_000, 1_000_000] {
            let mut h = Hll::new(14, DEFAULT_SPARSE_THRESHOLD_BYTES).unwrap();
            h.insert_hashes(&hashes(0..n, 7));
            let err = rel_err(h.estimate(), n);
            assert!(err < 0.025, "n={n} est={} err={err}", h.estimate());
        }
    }

    #[test]
    fn mean_error_is_unbiased() {
        // Averaged over many sketches the bias must be far below σ.
        let n = 50_000;
        let trials = 20;
        let mut sum = 0.0;
        for t in 0..trials {
            let mut h = Hll::new(14, DEFAULT_SPARSE_THRESHOLD_BYTES).unwrap();
            h.insert_hashes(&hashes(0..n, t + 100));
            sum += (h.estimate() as f64 - n as f64) / n as f64;
        }
        let bias = sum / trials as f64;
        assert!(bias.abs() < 0.005, "bias={bias}");
    }

    #[test]
    fn sparse_promotes_to_dense() {
        let mut h = Hll::new(14, DEFAULT_SPARSE_THRESHOLD_BYTES).unwrap();
        h.insert_hashes(&hashes(0..500, 1));
        assert!(!h.is_dense());
        assert!(h.heap_bytes() < 16 * 1024 / 4);
        h.insert_hashes(&hashes(0..5_000, 1));
        assert!(h.is_dense());
        assert_eq!(h.heap_bytes(), 16 * 1024);
    }

    #[test]
    fn sparse_and_dense_agree() {
        let hs = hashes(0..800, 3);
        let mut s = Hll::new(14, DEFAULT_SPARSE_THRESHOLD_BYTES).unwrap();
        s.insert_hashes(&hs);
        let mut d = s.clone();
        d.promote();
        assert!(!s.is_dense() && d.is_dense());
        assert_eq!(s.estimate(), d.estimate());
    }

    #[test]
    fn duplicate_inserts_do_not_modify() {
        let hs = hashes(0..1000, 9);
        let mut h = Hll::new(14, DEFAULT_SPARSE_THRESHOLD_BYTES).unwrap();
        assert!(h.insert_hashes(&hs));
        assert!(!h.insert_hashes(&hs));
    }

    fn build(range: std::ops::Range<u64>) -> Hll {
        let mut h = Hll::new(14, DEFAULT_SPARSE_THRESHOLD_BYTES).unwrap();
        h.insert_hashes(&hashes(range, 5));
        h
    }

    #[test]
    fn merge_equals_union_all_mode_combinations() {
        for (ra, rb) in [
            (0..300, 200..600),          // sparse + sparse
            (0..50_000, 40_000..40_300), // dense + sparse
            (0..300, 100..60_000),       // sparse + dense
            (0..50_000, 25_000..90_000), // dense + dense
        ] {
            let union = build(ra.start.min(rb.start)..ra.end.max(rb.end));
            let mut a = build(ra.clone());
            let b = build(rb.clone());
            a.merge(&b).unwrap();
            assert_eq!(a.estimate(), union.estimate(), "{ra:?} ∪ {rb:?}");
        }
    }

    #[test]
    fn merge_is_crdt() {
        let a = build(0..20_000);
        let b = build(10_000..30_000);
        let c = build(500..900);
        // commutative
        let mut ab = a.clone();
        ab.merge(&b).unwrap();
        let mut ba = b.clone();
        ba.merge(&a).unwrap();
        assert_eq!(ab.estimate(), ba.estimate());
        // associative
        let mut ab_c = ab.clone();
        ab_c.merge(&c).unwrap();
        let mut bc = b.clone();
        bc.merge(&c).unwrap();
        let mut a_bc = a.clone();
        a_bc.merge(&bc).unwrap();
        assert_eq!(ab_c.estimate(), a_bc.estimate());
        // idempotent
        let mut aa = a.clone();
        assert!(!aa.merge(&a).unwrap());
        assert_eq!(aa, a);
    }

    #[test]
    fn precision_mismatch_rejected() {
        let mut a = Hll::new(14, 4096).unwrap();
        let b = Hll::new(12, 4096).unwrap();
        assert!(matches!(
            a.merge(&b),
            Err(HllError::PrecisionMismatch { .. })
        ));
    }

    #[test]
    fn wire_roundtrip() {
        for h in [build(0..100), build(0..100_000)] {
            let (enc, bytes) = h.to_wire();
            let back = Hll::from_wire(14, enc, &bytes, DEFAULT_SPARSE_THRESHOLD_BYTES).unwrap();
            assert_eq!(back.estimate(), h.estimate());
            assert_eq!(back.is_dense(), h.is_dense());
        }
    }

    #[test]
    fn wire_rejects_corrupt() {
        assert!(Hll::from_wire(14, Encoding::DenseRaw, &[0u8; 10], 4096).is_err());
        assert!(Hll::from_wire(14, Encoding::Sparse, &[1, 2, 3], 4096).is_err());
        let mut bad = vec![0u8; 1 << 14];
        bad[0] = 60; // > q+1 = 51
        assert!(Hll::from_wire(14, Encoding::DenseRaw, &bad, 4096).is_err());
        // unsorted sparse
        let mut unsorted = Vec::new();
        unsorted.extend_from_slice(&encode(5, 1).to_le_bytes());
        unsorted.extend_from_slice(&encode(3, 1).to_le_bytes());
        assert!(Hll::from_wire(14, Encoding::Sparse, &unsorted, 4096).is_err());
    }
}
