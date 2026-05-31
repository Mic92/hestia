//! Upstream-signature filtering.
//!
//! Hestia only stores locally-built paths. Anything carrying a signature
//! from a trusted upstream cache (cache.nixos.org by default) is already
//! served by that cache and would only waste GHA cache quota, so the write
//! pipeline skips it.
//!
//! The check is on the signature's *key name* only — no cryptographic
//! verification. That is deliberate: a forged signature name in the local
//! store could only cause a path to be skipped (not served), and verifying
//! would require shipping upstream public keys.

use harmonia_utils_signature::Signature;

/// Key names hestia treats as upstream caches unless configured otherwise.
pub const DEFAULT_UPSTREAM_KEYS: &[&str] = &["cache.nixos.org-1"];

/// Decides whether a path counts as "already served by an upstream cache".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamFilter {
    trusted_keys: Vec<String>,
}

impl Default for UpstreamFilter {
    /// Filter with the default upstream key set ([`DEFAULT_UPSTREAM_KEYS`]).
    fn default() -> Self {
        Self::new(DEFAULT_UPSTREAM_KEYS.iter().map(|key| key.to_string()))
    }
}

impl UpstreamFilter {
    /// Filter that trusts exactly `keys` (replaces the default set).
    pub fn new(keys: impl IntoIterator<Item = String>) -> Self {
        Self {
            trusted_keys: keys.into_iter().collect(),
        }
    }

    /// Filter that trusts nothing: no path is ever skipped as upstream.
    /// Used by tests that push paths which happen to be upstream-signed.
    pub fn none() -> Self {
        Self {
            trusted_keys: Vec::new(),
        }
    }

    /// The key names this filter trusts.
    pub fn trusted_keys(&self) -> &[String] {
        &self.trusted_keys
    }

    /// True if `key_name` exactly matches a trusted upstream key.
    pub fn matches_key(&self, key_name: &str) -> bool {
        self.trusted_keys.iter().any(|key| key == key_name)
    }

    /// True if any of `signatures` was made by a trusted upstream key,
    /// i.e. the signed path should be skipped by the write pipeline.
    pub fn is_upstream_signed<'a>(
        &self,
        signatures: impl IntoIterator<Item = &'a Signature>,
    ) -> bool {
        signatures
            .into_iter()
            .any(|signature| self.matches_key(signature.name()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Real signature taken from `nix path-info --json` of
    /// /nix/store/4bwbk4an4bx7cb8xwffghvjjyfyl7m2i-bash-interactive-5.3p9
    /// (signed by the NixOS Hydra build farm).
    const REAL_UPSTREAM_SIG: &str = "cache.nixos.org-1:W9iYjq3JBzNBdhDJK7m+llyRfzYztsbx+301hnv89TsoxPMabUN0GMrtjszZ3dPbVQY54coVB5JQsD7gB4pvAA==";

    /// A locally-generated signature (same shape, different key name).
    const LOCAL_SIG: &str = "my-ci-key-1:W9iYjq3JBzNBdhDJK7m+llyRfzYztsbx+301hnv89TsoxPMabUN0GMrtjszZ3dPbVQY54coVB5JQsD7gB4pvAA==";

    fn parse(sig: &str) -> Signature {
        sig.parse().expect("test signature should parse")
    }

    #[test]
    fn real_cache_nixos_org_signature_is_upstream() {
        let filter = UpstreamFilter::default();
        let signature = parse(REAL_UPSTREAM_SIG);
        assert_eq!(signature.name(), "cache.nixos.org-1");
        assert!(filter.is_upstream_signed([&signature]));
    }

    #[test]
    fn locally_signed_path_is_not_upstream() {
        let filter = UpstreamFilter::default();
        let signature = parse(LOCAL_SIG);
        assert!(!filter.is_upstream_signed([&signature]));
    }

    #[test]
    fn unsigned_path_is_not_upstream() {
        let filter = UpstreamFilter::default();
        assert!(!filter.is_upstream_signed([]));
    }

    #[test]
    fn any_upstream_signature_among_many_is_enough() {
        let filter = UpstreamFilter::default();
        let local = parse(LOCAL_SIG);
        let upstream = parse(REAL_UPSTREAM_SIG);
        assert!(filter.is_upstream_signed([&local, &upstream]));
    }

    #[test]
    fn key_name_match_is_exact_not_prefix() {
        // "cache.nixos.org-1" must not match a hypothetical
        // "cache.nixos.org-12" key (or vice versa).
        let filter = UpstreamFilter::new(vec!["cache.nixos.org-12".to_string()]);
        let signature = parse(REAL_UPSTREAM_SIG);
        assert!(!filter.is_upstream_signed([&signature]));

        assert!(!UpstreamFilter::default().matches_key("cache.nixos.org-12"));
        assert!(!UpstreamFilter::default().matches_key("cache.nixos.org"));
    }

    #[test]
    fn empty_filter_skips_nothing() {
        let filter = UpstreamFilter::none();
        let signature = parse(REAL_UPSTREAM_SIG);
        assert!(!filter.is_upstream_signed([&signature]));
        assert!(filter.trusted_keys().is_empty());
    }

    #[test]
    fn configured_keys_replace_the_default() {
        let filter = UpstreamFilter::new(vec![
            "company-cache-1".to_string(),
            "cache.nixos.org-1".to_string(),
        ]);
        assert!(filter.matches_key("company-cache-1"));
        assert!(filter.matches_key("cache.nixos.org-1"));

        // A filter configured without cache.nixos.org does not match it.
        let custom_only = UpstreamFilter::new(vec!["company-cache-1".to_string()]);
        assert!(!custom_only.matches_key("cache.nixos.org-1"));
    }

    #[test]
    fn signatures_from_nix_path_info_json_parse() {
        // `nix path-info --json` emits signatures as "name:base64" strings;
        // harmonia's Signature deserializes both that and the structured
        // {keyName, sig} form. The filter must work with either source.
        let json = format!(r#"["{REAL_UPSTREAM_SIG}", "{LOCAL_SIG}"]"#);
        let signatures: Vec<Signature> = serde_json::from_str(&json).unwrap();
        assert_eq!(signatures.len(), 2);
        assert!(UpstreamFilter::default().is_upstream_signed(&signatures));
    }
}
