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

/// Write the sentinel over each scanned occurrence.
fn overwrite_with_sentinel(data: &mut [u8], rewrites: &[Rewrite]) {
    for rewrite in rewrites {
        let offset = rewrite.offset as usize;
        data[offset..offset + HASH_LEN].copy_from_slice(&SENTINEL);
    }
}

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

    /// Find every reference-hash occurrence. Offsets index the file
    /// content; the sentinel is hash-length, so offsets are identical in
    /// original and normalized bytes.
    fn scan(&self, data: &[u8]) -> Vec<Rewrite> {
        if self.hashes.is_empty() {
            return Vec::new();
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
        scanner
            .find_iter(data)
            .map(|m| Rewrite {
                offset: m.start() as u64,
                ref_index: m.pattern().as_u32(),
            })
            .collect()
    }

    /// Replace every reference-hash occurrence with the sentinel, returning
    /// the normalized bytes and the position table to restore them.
    ///
    /// Reference-free files (empty table or no occurrences) are returned as
    /// a cheap clone of `data`, so mmap-backed file bytes stay zero-copy.
    pub fn normalize(&self, data: &Bytes) -> (Bytes, Vec<Rewrite>) {
        let rewrites = self.scan(data);
        if rewrites.is_empty() {
            return (data.clone(), rewrites);
        }
        let mut out = data.to_vec();
        overwrite_with_sentinel(&mut out, &rewrites);
        (Bytes::from(out), rewrites)
    }

    /// [`Self::normalize`] operating on a mutable buffer the caller owns
    /// (e.g. a copy-on-write mapping): occurrences are overwritten in
    /// place, so only the touched pages cost memory.
    pub fn normalize_in_place(&self, data: &mut [u8]) -> Vec<Rewrite> {
        let rewrites = self.scan(data);
        overwrite_with_sentinel(data, &rewrites);
        rewrites
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
        let content = Bytes::from(content);

        let (normalized, rewrites) = table.normalize(&content);
        assert_eq!(rewrites.len(), 2);
        assert!(!contains(&normalized, GLIBC_A.as_bytes()));
        assert!(!contains(&normalized, SELF.as_bytes()));

        let mut restored = normalized.to_vec();
        table.restore(&mut restored, &rewrites).unwrap();
        assert_eq!(restored, content);

        // In-place normalization must produce identical output.
        let mut in_place = content.to_vec();
        let in_place_rewrites = table.normalize_in_place(&mut in_place);
        assert_eq!(in_place, normalized);
        assert_eq!(in_place_rewrites, rewrites);
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

        let (na, ra) = build_a.normalize(&Bytes::from(a));
        let (nb, rb) = build_b.normalize(&Bytes::from(b));
        assert_eq!(na, nb);
        assert_eq!(ra, rb);
    }

    #[test]
    fn empty_table_is_a_noop() {
        let table = RefTable::new(&[]);
        let data = Bytes::from_static(b"no references here, just content bytes to scan");
        let (normalized, rewrites) = table.normalize(&data);
        assert_eq!(normalized, data);
        assert!(rewrites.is_empty());
    }

    #[test]
    fn reference_free_content_is_not_copied() {
        // Files without any occurrence must share the input buffer (cheap
        // Bytes clone), so mmap-backed large files stay zero-copy.
        let table = RefTable::new(&[store_path(GLIBC_A, "glibc")]);
        let data = Bytes::from(vec![b'x'; 256 * 1024]);
        let (normalized, rewrites) = table.normalize(&data);
        assert!(rewrites.is_empty());
        assert_eq!(
            normalized.as_ptr(),
            data.as_ptr(),
            "expected a zero-copy clone"
        );
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
