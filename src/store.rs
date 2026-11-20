//! In-RAM HLL store: sharded `DashMap`, TTL, memory cap, delete tombstones.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;

use crate::hll::{Hll, HllError};

/// Fixed per-entry overhead estimate (map slot, key header, entry fields).
const ENTRY_OVERHEAD_BYTES: usize = 96;
/// Internal (replication/handoff) creations may exceed the client cap by this
/// factor so that durability is not sacrificed at the edge of the cap.
const INTERNAL_HEADROOM: f64 = 1.10;

pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// Memory high-watermark reached; refuse to create new HLLs.
    ResourceExhausted,
    /// The key was recently deleted; internal writes are rejected.
    Tombstoned,
    Hll(HllError),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::ResourceExhausted => write!(f, "memory high-watermark reached"),
            StoreError::Tombstoned => write!(f, "key recently deleted"),
            StoreError::Hll(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<HllError> for StoreError {
    fn from(e: HllError) -> Self {
        StoreError::Hll(e)
    }
}

/// Who is writing. Client writes express fresh intent (and clear tombstones);
/// internal writes are replication/handoff traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    Client,
    Internal,
}

pub struct HllEntry {
    pub hll: Hll,
    pub last_accessed_unix_secs: AtomicU64,
    /// 0 = never expires.
    pub expires_at: u64,
}

impl HllEntry {
    fn footprint(&self, key: &str) -> usize {
        ENTRY_OVERHEAD_BYTES + key.len() + self.hll.heap_bytes()
    }

    fn is_expired(&self, now: u64) -> bool {
        self.expires_at != 0 && self.expires_at <= now
    }
}

/// CRDT join for expiry where 0 means "never" (i.e. +inf).
pub fn merge_expiry(a: u64, b: u64) -> u64 {
    if a == 0 || b == 0 { 0 } else { a.max(b) }
}

pub struct Snapshot {
    pub hll: Hll,
    pub expires_at: u64,
}

pub struct Store {
    map: DashMap<String, HllEntry>,
    tombstones: DashMap<String, u64>,
    precision: u8,
    sparse_threshold_bytes: usize,
    max_memory_bytes: u64,
    tombstone_ttl_secs: u64,
    memory_bytes: AtomicI64,
    sparse_count: AtomicI64,
    dense_count: AtomicI64,
}

impl Store {
    pub fn new(
        precision: u8,
        sparse_threshold_bytes: usize,
        max_memory_bytes: u64,
        tombstone_ttl_secs: u64,
    ) -> Result<Self, HllError> {
        // Validate precision once up front.
        Hll::new(precision, sparse_threshold_bytes)?;
        Ok(Self {
            map: DashMap::with_shard_amount(256),
            tombstones: DashMap::new(),
            precision,
            sparse_threshold_bytes,
            max_memory_bytes,
            tombstone_ttl_secs,
            memory_bytes: AtomicI64::new(0),
            sparse_count: AtomicI64::new(0),
            dense_count: AtomicI64::new(0),
        })
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }

    pub fn sparse_threshold_bytes(&self) -> usize {
        self.sparse_threshold_bytes
    }

    pub fn new_hll(&self) -> Hll {
        Hll::new(self.precision, self.sparse_threshold_bytes).expect("validated precision")
    }

    pub fn memory_bytes(&self) -> u64 {
        self.memory_bytes.load(Ordering::Relaxed).max(0) as u64
    }

    pub fn max_memory_bytes(&self) -> u64 {
        self.max_memory_bytes
    }

    /// (sparse, dense) key counts.
    pub fn mode_counts(&self) -> (u64, u64) {
        (
            self.sparse_count.load(Ordering::Relaxed).max(0) as u64,
            self.dense_count.load(Ordering::Relaxed).max(0) as u64,
        )
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    fn can_create(&self, origin: Origin) -> bool {
        if self.max_memory_bytes == 0 {
            return true;
        }
        let limit = match origin {
            Origin::Client => self.max_memory_bytes as f64,
            Origin::Internal => self.max_memory_bytes as f64 * INTERNAL_HEADROOM,
        };
        (self.memory_bytes() as f64) < limit
    }

    fn account(&self, before: Option<(usize, bool)>, after: Option<(usize, bool)>) {
        let (b_bytes, b_dense) = before.map_or((0, None), |(b, d)| (b, Some(d)));
        let (a_bytes, a_dense) = after.map_or((0, None), |(b, d)| (b, Some(d)));
        let delta = a_bytes as i64 - b_bytes as i64;
        if delta != 0 {
            self.memory_bytes.fetch_add(delta, Ordering::Relaxed);
        }
        if b_dense != a_dense {
            match b_dense {
                Some(true) => self.dense_count.fetch_sub(1, Ordering::Relaxed),
                Some(false) => self.sparse_count.fetch_sub(1, Ordering::Relaxed),
                None => 0,
            };
            match a_dense {
                Some(true) => self.dense_count.fetch_add(1, Ordering::Relaxed),
                Some(false) => self.sparse_count.fetch_add(1, Ordering::Relaxed),
                None => 0,
            };
        }
    }

    fn is_tombstoned(&self, key: &str, now: u64) -> bool {
        // Copy out first: holding the read guard while calling remove_if on
        // the same shard would deadlock.
        let expiry = self.tombstones.get(key).map(|t| *t);
        match expiry {
            Some(t) if t > now => true,
            Some(_) => {
                drop(self.tombstones.remove_if(key, |_, t| *t <= now));
                false
            }
            None => false,
        }
    }

    fn check_tombstone(&self, key: &str, origin: Origin, now: u64) -> Result<(), StoreError> {
        match origin {
            Origin::Client => {
                self.tombstones.remove(key);
                Ok(())
            }
            Origin::Internal if self.is_tombstoned(key, now) => Err(StoreError::Tombstoned),
            Origin::Internal => Ok(()),
        }
    }

    /// Generic upsert: apply `f` to the (possibly new) entry.
    fn upsert<F>(&self, key: &str, origin: Origin, expires_at: Option<u64>, f: F) -> Result<bool, StoreError>
    where
        F: FnOnce(&mut Hll) -> Result<bool, StoreError>,
    {
        let now = now_secs();
        self.check_tombstone(key, origin, now)?;
        match self.map.entry(key.to_owned()) {
            Entry::Occupied(mut occ) => {
                let entry = occ.get_mut();
                if entry.is_expired(now) {
                    // Treat as fresh: reset sketch and expiry.
                    let before = (entry.footprint(key), entry.hll.is_dense());
                    entry.hll = self.new_hll();
                    entry.expires_at = expires_at.unwrap_or(0);
                    let modified = f(&mut entry.hll)?;
                    entry.last_accessed_unix_secs.store(now, Ordering::Relaxed);
                    self.account(Some(before), Some((entry.footprint(key), entry.hll.is_dense())));
                    return Ok(modified);
                }
                let before = (entry.footprint(key), entry.hll.is_dense());
                let modified = f(&mut entry.hll)?;
                if let Some(e) = expires_at {
                    entry.expires_at = merge_expiry(entry.expires_at, e);
                }
                entry.last_accessed_unix_secs.store(now, Ordering::Relaxed);
                self.account(Some(before), Some((entry.footprint(key), entry.hll.is_dense())));
                Ok(modified)
            }
            Entry::Vacant(vac) => {
                if !self.can_create(origin) {
                    return Err(StoreError::ResourceExhausted);
                }
                let mut hll = self.new_hll();
                let modified = f(&mut hll)?;
                let entry = HllEntry {
                    hll,
                    last_accessed_unix_secs: AtomicU64::new(now),
                    expires_at: expires_at.unwrap_or(0),
                };
                let after = (entry.footprint(key), entry.hll.is_dense());
                vac.insert(entry);
                self.account(None, Some(after));
                Ok(modified)
            }
        }
    }

    /// Insert pre-hashed items. `ttl_secs == 0` leaves the expiry unchanged
    /// (new keys then never expire); otherwise the expiry is joined with
    /// `now + ttl` (CRDT max, where "never" dominates).
    pub fn add(&self, key: &str, hashes: &[u64], ttl_secs: u32, origin: Origin) -> Result<bool, StoreError> {
        let expires_at = (ttl_secs != 0).then(|| now_secs() + ttl_secs as u64);
        self.upsert(key, origin, expires_at, |h| Ok(h.insert_hashes(hashes)))
    }

    /// CRDT-merge a snapshot into the local copy.
    pub fn merge_snapshot(&self, key: &str, other: &Hll, expires_at: u64, origin: Origin) -> Result<bool, StoreError> {
        if other.precision() != self.precision {
            return Err(StoreError::Hll(HllError::PrecisionMismatch {
                left: self.precision,
                right: other.precision(),
            }));
        }
        if expires_at != 0 && expires_at <= now_secs() {
            return Ok(false); // already expired; nothing to keep
        }
        self.upsert(key, origin, Some(expires_at), |h| Ok(h.merge(other)?))
    }

    /// Cardinality; `None` if absent or expired.
    pub fn count(&self, key: &str) -> Option<u64> {
        let now = now_secs();
        let entry = self.map.get(key)?;
        if entry.is_expired(now) {
            return None;
        }
        entry.last_accessed_unix_secs.store(now, Ordering::Relaxed);
        Some(entry.hll.estimate())
    }

    pub fn contains(&self, key: &str) -> bool {
        let now = now_secs();
        self.map.get(key).is_some_and(|e| !e.is_expired(now))
    }

    /// Clone of the current sketch, for replication / handoff / merge.
    pub fn snapshot(&self, key: &str) -> Option<Snapshot> {
        let now = now_secs();
        let entry = self.map.get(key)?;
        if entry.is_expired(now) {
            return None;
        }
        Some(Snapshot {
            hll: entry.hll.clone(),
            expires_at: entry.expires_at,
        })
    }

    /// Delete and leave a tombstone that blocks stale internal writes.
    pub fn delete(&self, key: &str) -> bool {
        if self.tombstone_ttl_secs > 0 {
            self.tombstones
                .insert(key.to_owned(), now_secs() + self.tombstone_ttl_secs);
        }
        self.remove(key)
    }

    /// Drop without tombstone (ownership GC).
    pub fn remove(&self, key: &str) -> bool {
        match self.map.remove(key) {
            Some((k, e)) => {
                let existed = !e.is_expired(now_secs());
                self.account(Some((e.footprint(&k), e.hll.is_dense())), None);
                existed
            }
            None => false,
        }
    }

    pub fn keys(&self) -> Vec<String> {
        self.map.iter().map(|e| e.key().clone()).collect()
    }

    /// Evict expired entries and stale tombstones. Returns evicted entry count.
    pub fn sweep_expired(&self) -> usize {
        let now = now_secs();
        let expired: Vec<String> = self
            .map
            .iter()
            .filter(|e| e.value().is_expired(now))
            .map(|e| e.key().clone())
            .collect();
        let mut n = 0;
        for k in expired {
            if let Some((k, e)) = self.map.remove_if(&k, |_, e| e.is_expired(now)) {
                self.account(Some((e.footprint(&k), e.hll.is_dense())), None);
                n += 1;
            }
        }
        self.tombstones.retain(|_, t| *t > now);
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hll::hash_item;

    fn store() -> Store {
        Store::new(14, 4096, 0, 60).unwrap()
    }

    fn hs(n: u64) -> Vec<u64> {
        (0..n).map(|i| hash_item(&i.to_le_bytes())).collect()
    }

    #[test]
    fn add_count_delete() {
        let s = store();
        assert!(s.add("a", &hs(1000), 0, Origin::Client).unwrap());
        let c = s.count("a").unwrap();
        assert!((c as i64 - 1000).abs() < 30, "{c}");
        assert!(s.delete("a"));
        assert_eq!(s.count("a"), None);
        assert_eq!(s.memory_bytes(), 0);
        assert_eq!(s.mode_counts(), (0, 0));
    }

    #[test]
    fn tombstone_blocks_internal_but_not_client() {
        let s = store();
        s.add("a", &hs(10), 0, Origin::Client).unwrap();
        s.delete("a");
        let h = s.new_hll();
        assert_eq!(
            s.merge_snapshot("a", &h, 0, Origin::Internal),
            Err(StoreError::Tombstoned)
        );
        assert_eq!(
            s.add("a", &hs(5), 0, Origin::Internal),
            Err(StoreError::Tombstoned)
        );
        assert!(s.add("a", &hs(5), 0, Origin::Client).unwrap());
        // Client write cleared the tombstone.
        assert!(s.add("a", &hs(50), 0, Origin::Internal).is_ok());
    }

    #[test]
    fn memory_cap_rejects_new_client_keys() {
        let s = Store::new(14, 4096, 1, 60).unwrap();
        s.add("a", &hs(10), 0, Origin::Client).unwrap();
        assert_eq!(
            s.add("b", &hs(10), 0, Origin::Client),
            Err(StoreError::ResourceExhausted)
        );
        // Existing keys keep accepting writes.
        assert!(s.add("a", &hs(20), 0, Origin::Client).is_ok());
    }

    #[test]
    fn mode_accounting_tracks_promotion() {
        let s = store();
        s.add("a", &hs(10), 0, Origin::Client).unwrap();
        assert_eq!(s.mode_counts(), (1, 0));
        s.add("a", &hs(10_000), 0, Origin::Client).unwrap();
        assert_eq!(s.mode_counts(), (0, 1));
        assert!(s.memory_bytes() >= 16 * 1024);
    }

    #[test]
    fn ttl_expiry_and_sweep() {
        let s = store();
        s.add("a", &hs(10), 1, Origin::Client).unwrap();
        // Force expiry by rewriting the entry.
        s.map.get_mut("a").unwrap().expires_at = now_secs() - 1;
        assert_eq!(s.count("a"), None);
        assert_eq!(s.sweep_expired(), 1);
        assert!(s.is_empty());
        assert_eq!(s.memory_bytes(), 0);
    }

    #[test]
    fn expiry_join() {
        assert_eq!(merge_expiry(0, 5), 0);
        assert_eq!(merge_expiry(5, 9), 9);
    }
}
