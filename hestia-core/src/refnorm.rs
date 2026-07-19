//! Reference normalization.
//!
//! Store paths embed the 32-char base32 hashes of their references (and
//! their own self-reference) in file contents. When a dependency is
//! rebuilt its hash changes, so every chunk covering an occurrence churns
//! even when nothing else changed, defeating cross-rebuild chunk dedup.
//!
//! v2 rewrites those occurrences to zeros before chunking, so the stored
//! chunk is identical across rebuilds, and records each occurrence in a
//! per-file position table ([`Rewrite`]). [`RefTable::restore`] copies the
//! real hash back on NAR reassembly. The hashes come from the path's
//! `references` (already in the `PathEntry`); a reference's index is its
//! position in the sorted, deduplicated set, so write and read derive
//! identical indices from the same list.

use aho_corasick::{AhoCorasick, MatchKind};
use bytes::Bytes;

use crate::manifest::{Rewrite, StorePath};

/// Length of a base32-encoded store path hash.
pub const HASH_LEN: usize = 32;

/// Written over a reference occurrence; the value is irrelevant (the
/// position table restores the real bytes), zeros compress best.
const SENTINEL: [u8; HASH_LEN] = [0u8; HASH_LEN];

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("rewrite references index {index} but the path has {len} references")]
    IndexOutOfRange { index: usize, len: usize },

    #[error("rewrite at offset {offset} does not fit in {len}-byte file content")]
    OffsetOutOfRange { offset: usize, len: usize },
}

/// Sorted, deduplicated reference hashes for one path, plus an
/// Aho-Corasick automaton over them. The vector position is the
/// [`Rewrite::ref_index`]; the automaton's pattern id equals that position,
/// since patterns are added in the same sorted order.
#[derive(Debug, Clone)]
pub struct RefTable {
    hashes: Vec<[u8; HASH_LEN]>,
    /// Aho-Corasick automaton over the hashes, built on first `normalize`:
    /// only the write side scans, and the read side (`restore`) builds a
    /// RefTable per served path, so paying the build cost there would be
    /// pure waste.
    scanner: std::sync::OnceLock<AhoCorasick>,
}

impl RefTable {
    pub fn new(references: &[StorePath]) -> Self {
        let mut hashes: Vec<[u8; HASH_LEN]> = references
            .iter()
            .map(|path| {
                let text = path.hash().to_string();
                debug_assert_eq!(text.len(), HASH_LEN);
                let mut buf = [0u8; HASH_LEN];
                buf.copy_from_slice(text.as_bytes());
                buf
            })
            .collect();
        hashes.sort_unstable();
        hashes.dedup();

        Self {
            hashes,
            scanner: std::sync::OnceLock::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    /// Replace every reference-hash occurrence with the sentinel, returning
    /// the normalized bytes and the position table to restore them. Offsets
    /// index the file content, identical in normalized and original bytes
    /// (the sentinel is hash-length).
    pub fn normalize(&self, data: &[u8]) -> (Bytes, Vec<Rewrite>) {
        if self.hashes.is_empty() {
            return (Bytes::copy_from_slice(data), Vec::new());
        }
        let scanner = self.scanner.get_or_init(|| {
            // All patterns are hash-length, so LeftmostLongest yields the
            // non-overlapping matches restore expects; its SIMD prefilter
            // skips the long non-matching runs of file content.
            AhoCorasick::builder()
                .match_kind(MatchKind::LeftmostLongest)
                .build(&self.hashes)
                .expect("aho-corasick build over fixed-length hashes")
        });
        let mut out = Vec::with_capacity(data.len());
        let mut rewrites = Vec::new();
        // Start of the unmatched run not yet copied into `out`.
        let mut last = 0;
        for m in scanner.find_iter(data) {
            out.extend_from_slice(&data[last..m.start()]);
            rewrites.push(Rewrite {
                offset: out.len() as u64,
                ref_index: m.pattern().as_u32(),
            });
            out.extend_from_slice(&SENTINEL);
            last = m.end();
        }
        out.extend_from_slice(&data[last..]);
        (Bytes::from(out), rewrites)
    }

    /// Undo [`Self::normalize`]: copy each recorded reference hash back into
    /// the concatenated (normalized) file content.
    pub fn restore(&self, data: &mut [u8], rewrites: &[Rewrite]) -> Result<(), Error> {
        for rewrite in rewrites {
            let index = rewrite.ref_index as usize;
            let hash = self.hashes.get(index).ok_or(Error::IndexOutOfRange {
                index,
                len: self.hashes.len(),
            })?;
            let offset = rewrite.offset as usize;
            let end = offset
                .checked_add(HASH_LEN)
                .filter(|&end| end <= data.len())
                .ok_or(Error::OffsetOutOfRange {
                    offset,
                    len: data.len(),
                })?;
            data[offset..end].copy_from_slice(hash);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store_path(hash32: &str, name: &str) -> StorePath {
        format!("{hash32}-{name}")
            .parse()
            .expect("valid store path")
    }

    const GLIBC_A: &str = "0d71ygfwbmy1xjlbj1v027dfmy9cjm9c";
    const GLIBC_B: &str = "1a2b3c4d5f6g7h8j9k0lmnpqrsvwxyz1";
    const SELF: &str = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    #[test]
    fn round_trip_restores_original() {
        let refs = vec![
            store_path(GLIBC_A, "glibc-2.40"),
            store_path(SELF, "hello-1.0"),
        ];
        let table = RefTable::new(&refs);

        let mut content = b"prefix ".to_vec();
        content.extend_from_slice(GLIBC_A.as_bytes());
        content.extend_from_slice(b"/lib and self ");
        content.extend_from_slice(SELF.as_bytes());
        content.extend_from_slice(b" suffix padding past the hash-length bound");

        let (normalized, rewrites) = table.normalize(&content);
        assert_eq!(rewrites.len(), 2);
        assert!(!contains(&normalized, GLIBC_A.as_bytes()));
        assert!(!contains(&normalized, SELF.as_bytes()));

        let mut restored = normalized.to_vec();
        table.restore(&mut restored, &rewrites).unwrap();
        assert_eq!(restored, content);
    }

    #[test]
    fn normalization_is_hash_independent() {
        // Two builds differing only in a dependency's hash normalize to
        // identical bytes: the dedup win.
        let build_a = RefTable::new(&[store_path(GLIBC_A, "glibc")]);
        let build_b = RefTable::new(&[store_path(GLIBC_B, "glibc")]);

        let mut a = b"header ".to_vec();
        a.extend_from_slice(GLIBC_A.as_bytes());
        a.extend_from_slice(b" trailer bytes beyond the hash window");
        let mut b = b"header ".to_vec();
        b.extend_from_slice(GLIBC_B.as_bytes());
        b.extend_from_slice(b" trailer bytes beyond the hash window");

        let (na, ra) = build_a.normalize(&a);
        let (nb, rb) = build_b.normalize(&b);
        assert_eq!(na, nb);
        assert_eq!(ra, rb);
    }

    #[test]
    fn empty_table_is_a_noop() {
        let table = RefTable::new(&[]);
        let data = b"no references here, just content bytes to scan".to_vec();
        let (normalized, rewrites) = table.normalize(&data);
        assert_eq!(normalized.as_ref(), data.as_slice());
        assert!(rewrites.is_empty());
    }

    #[test]
    fn restore_rejects_out_of_range() {
        let table = RefTable::new(&[store_path(GLIBC_A, "x")]);
        let mut data = vec![0u8; 10];
        assert!(matches!(
            table.restore(
                &mut data,
                &[Rewrite {
                    offset: 0,
                    ref_index: 0
                }]
            ),
            Err(Error::OffsetOutOfRange { .. })
        ));
        let mut data = vec![0u8; 64];
        assert!(matches!(
            table.restore(
                &mut data,
                &[Rewrite {
                    offset: 0,
                    ref_index: 5
                }]
            ),
            Err(Error::IndexOutOfRange { .. })
        ));
    }
}
