//! Versioned mutable entries on top of the write-once GHA cache.
//!
//! The GHA cache is strictly write-once per key, but the manifest must be
//! updatable. The SaveMutable pattern (from go-actions-cache) fakes
//! mutability with a monotonically increasing index suffix:
//!
//! ```text
//! load:  prefix-match "m#"         -> newest entry = current version "m#N"
//! save:  reserve "m#N+1"
//!          already_exists -> another writer holds N+1: wait, re-load,
//!                            re-merge, retry
//!          reserved       -> upload new blob, finalize
//! ```
//!
//! A reservation (even unfinalized) blocks its key forever, so a writer that
//! crashes between reserve and finalize would deadlock the sequence. After
//! `stale_skip_after` consecutive conflicts on the same index the writer
//! assumes a crashed peer and skips over that index.
//!
//! Consistency model (verified against the real service): **reservations
//! are strongly consistent within a ref scope** (two writers in the same
//! scope can never both reserve the same key) but **lookups are eventually
//! consistent** (a
//! just-finalized entry may not be returned by load() for a while).
//! Cache entry uniqueness is per (key, version, ref scope): writers in
//! different scopes (a PR job and a default-branch job) can both reserve
//! and finalize the same key. Their lineages fork; the PR-scoped fork is
//! discarded with its branch (see "PR scope isolation" in the README), so
//! the no-lost-writes merge guarantee below is scope-local. The
//! conflict-retry loop therefore re-loads until it sees the version that
//! blocked it; the stale-skip window must be comfortably larger than the
//! observed propagation lag, otherwise lag would be misdiagnosed as a
//! crashed writer and the lagging version's changes dropped (benign for a
//! lossy cache — those paths get rebuilt — but wasteful).

use std::time::Duration;

use bytes::Bytes;

use crate::gha::twirp::{DownloadUrl, Reservation, TwirpClient};
use crate::gha::{Error, blob};

/// Cache key for version `index` of family `prefix`, e.g. `m#5`.
fn key_for(prefix: &str, index: u64) -> String {
    format!("{prefix}#{index}")
}

/// Restore-key prefix that matches every version of the family, e.g. `m#`.
fn search_prefix(prefix: &str) -> String {
    format!("{prefix}#")
}

/// Extract the numeric index from a full key, e.g. `m#5` -> 5.
fn parse_index(prefix: &str, key: &str) -> Result<u64, Error> {
    let family_prefix = search_prefix(prefix);
    let suffix = key.strip_prefix(&family_prefix).ok_or_else(|| {
        Error::InvalidResponse(format!(
            "matched key {key:?} does not start with {family_prefix:?}"
        ))
    })?;
    suffix
        .parse()
        .map_err(|_| Error::InvalidResponse(format!("matched key {key:?} has a non-numeric index")))
}

/// A loaded mutable entry.
#[derive(Debug, Clone)]
pub struct MutableEntry {
    /// Full cache key, e.g. `m#5`.
    pub key: String,
    /// Parsed index, e.g. 5.
    pub index: u64,
    /// Entry contents.
    pub data: Bytes,
}

/// Handle for one mutable, versioned cache entry family (e.g. `m`).
pub struct SaveMutable<'a> {
    twirp: &'a TwirpClient,
    http: &'a reqwest::Client,
    prefix: String,
    /// Delay between conflict retries.
    retry_delay: Duration,
    /// Give up after this many conflicting save attempts.
    max_attempts: u32,
    /// Skip over an index after this many conflicts on it
    /// (crashed-writer recovery).
    stale_skip_after: u32,
}

impl<'a> SaveMutable<'a> {
    pub fn new(
        twirp: &'a TwirpClient,
        http: &'a reqwest::Client,
        prefix: impl Into<String>,
    ) -> Self {
        Self {
            twirp,
            http,
            prefix: prefix.into(),
            retry_delay: Duration::from_secs(3),
            max_attempts: 60,
            // 20 conflicts x 3s = 60s before an index is judged abandoned:
            // far above the lookup propagation lag observed in CI, so a
            // finalized-but-not-yet-visible version is never skipped over.
            stale_skip_after: 20,
        }
    }

    /// Tune retry behavior (tests use short delays).
    pub fn with_retry(
        mut self,
        retry_delay: Duration,
        max_attempts: u32,
        stale_skip_after: u32,
    ) -> Self {
        self.retry_delay = retry_delay;
        self.max_attempts = max_attempts;
        self.stale_skip_after = stale_skip_after;
        self
    }

    /// Load the newest entry of this family, or `None` if there is none yet.
    pub async fn load(&self) -> Result<Option<MutableEntry>, Error> {
        let prefix = search_prefix(&self.prefix);
        let lookup = self
            .twirp
            .get_download_url(&prefix, &[prefix.as_str()])
            .await?;
        let (url, matched_key) = match lookup {
            DownloadUrl::Miss => return Ok(None),
            DownloadUrl::Hit { url, matched_key } => (url, matched_key),
        };
        let index = parse_index(&self.prefix, &matched_key)?;

        // The signed URL was just issued, but refresh anyway if it raced
        // with an expiry. The refresh re-runs the prefix lookup, which may
        // resolve to a *newer* version (a concurrent writer finalized in
        // between), so the refreshed key must replace the original one:
        // key, index and data must always describe the same version.
        let twirp = self.twirp;
        let refreshed_key = std::sync::Arc::new(std::sync::Mutex::new(None::<String>));
        let refreshed_in = std::sync::Arc::clone(&refreshed_key);
        let data = blob::get_with_refresh(self.http, &url, None, async move || {
            match twirp.get_download_url(&prefix, &[prefix.as_str()]).await? {
                DownloadUrl::Hit { url, matched_key } => {
                    *refreshed_in.lock().expect("not poisoned") = Some(matched_key);
                    Ok(url)
                }
                DownloadUrl::Miss => Err(Error::InvalidResponse(format!(
                    "entry {prefix:?} disappeared while downloading"
                ))),
            }
        })
        .await?;

        let (key, index) = match refreshed_key.lock().expect("not poisoned").take() {
            Some(key) => {
                let index = parse_index(&self.prefix, &key)?;
                (key, index)
            }
            None => (matched_key, index),
        };

        Ok(Some(MutableEntry { key, index, data }))
    }

    /// Save a new version produced by `merge`.
    ///
    /// `merge` receives the current entry (if any) and returns the new
    /// contents. It may be called multiple times: every conflict with a
    /// concurrent writer triggers a re-load and re-merge so no writer's
    /// changes get lost. Returns the index of the newly written version.
    pub async fn save<F>(&self, merge: F) -> Result<u64, Error>
    where
        F: FnMut(Option<&MutableEntry>) -> Result<Vec<u8>, Error>,
    {
        self.save_with_floor(0, merge).await
    }

    /// Like [`Self::save`], but never reserves an index at or below `floor`.
    ///
    /// Callers that know a version `floor` exists (because they committed
    /// it themselves) pass it here so that an eventually-consistent load
    /// — which may still return an older version — does not make the
    /// writer reserve an index that is already taken and spin in the
    /// conflict loop until the lookup catches up.
    pub async fn save_with_floor<F>(&self, floor: u64, mut merge: F) -> Result<u64, Error>
    where
        F: FnMut(Option<&MutableEntry>) -> Result<Vec<u8>, Error>,
    {
        let mut attempts: u32 = 0;
        // Indexes at or below this are blocked by (presumably crashed) writers.
        let mut skip_through: u64 = floor;
        // Conflicts are counted per index, not as a single consecutive
        // streak: eventually consistent loads can oscillate between two
        // versions (non-monotonic reads), and a streak that resets on every
        // index change would never reach the stale-skip threshold.
        let mut conflicts: std::collections::BTreeMap<u64, u32> = std::collections::BTreeMap::new();

        loop {
            let current = self.load().await?;
            let data = Bytes::from(merge(current.as_ref())?);

            let base = current.as_ref().map(|entry| entry.index).unwrap_or(0);
            let index = base.max(skip_through) + 1;
            let key = key_for(&self.prefix, index);

            match self.twirp.create_cache_entry(&key).await? {
                Reservation::Created { upload_url } => {
                    self.twirp
                        .upload_and_finalize(self.http, &key, upload_url, data)
                        .await?;
                    return Ok(index);
                }
                Reservation::AlreadyExists => {
                    attempts += 1;
                    let streak = conflicts.entry(index).or_insert(0);
                    *streak += 1;
                    if *streak >= self.stale_skip_after {
                        // Nobody finalized this index after several waits:
                        // assume the writer holding the reservation crashed
                        // and skip over it.
                        skip_through = index;
                        conflicts.remove(&index);
                    } else if attempts >= self.max_attempts {
                        // Checked only when no skip fired: an attempt that
                        // skips a dead index is progress, not grounds for
                        // giving up, however many conflicts preceded it.
                        return Err(Error::Conflict { key, attempts });
                    }
                    tokio::time::sleep(self.retry_delay).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // No reqwest::Client in these tests: TLS client construction requires
    // system CA certs, which do not exist in the Nix build sandbox.

    #[test]
    fn key_layout() {
        assert_eq!(key_for("m", 1), "m#1");
        assert_eq!(key_for("m", 42), "m#42");
        assert_eq!(search_prefix("m"), "m#");
    }

    #[test]
    fn parse_index_accepts_only_numeric_suffixes() {
        assert_eq!(parse_index("m", "m#7").unwrap(), 7);
        assert_eq!(parse_index("m", "m#123456").unwrap(), 123456);
        assert!(parse_index("m", "m#").is_err());
        assert!(parse_index("m", "m#abc").is_err());
        assert!(parse_index("m", "other#1").is_err());
    }
}
