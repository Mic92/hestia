//! GHCR deletes nothing over the OCI API. Its REST packages API deletes
//! by version id, and ids are only found by paging the versions list, so
//! a `version id → key` ledger is kept under a tag and extended each GC
//! run by paging newest-first until a page brings nothing new. Untagged
//! versions (content objects) cost one manifest GET each to learn the key.

use std::collections::BTreeMap;

use minicbor::{Decode, Encode};
use reqwest::{Method, StatusCode, header};
use serde::Deserialize;

use super::oci::Oci;
use super::{Error, Listed};
use crate::gha::blob::status_error;
use crate::gha::rest::parse_timestamp;

const PER_PAGE: usize = 100;
pub const LEDGER_TAG: &str = "x-ledger";

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct Version {
    #[n(0)]
    pub key: String,
    #[n(1)]
    pub created: u64,
}

#[derive(Debug, Default, Encode, Decode)]
pub struct Ledger {
    #[n(0)]
    pub versions: BTreeMap<u64, Version>,
}

impl Ledger {
    pub fn decode(bytes: &[u8]) -> Ledger {
        minicbor::decode(bytes).unwrap_or_default()
    }

    pub fn encode(&self) -> Vec<u8> {
        minicbor::to_vec(self).expect("Vec write")
    }

    /// Untagged versions, i.e. content objects.
    pub fn objects(&self) -> Vec<Listed> {
        self.versions
            .values()
            .filter(|v| !matches!(v.key.split_once('-'), Some(("g" | "h" | "c" | "x", _))))
            .map(|v| Listed {
                key: v.key.clone(),
                created: Some(v.created),
                last_accessed: None,
            })
            .collect()
    }

    fn id_of(&self, key: &str) -> Option<u64> {
        self.versions
            .iter()
            .find(|(_, v)| v.key == key)
            .map(|(id, _)| *id)
    }
}

#[derive(Deserialize)]
struct ApiVersion {
    id: u64,
    /// The manifest digest.
    name: String,
    created_at: String,
    metadata: Option<Metadata>,
}
#[derive(Deserialize)]
struct Metadata {
    container: Option<Container>,
}
#[derive(Deserialize)]
struct Container {
    tags: Vec<String>,
}

#[derive(Clone)]
pub struct Packages {
    http: reqwest::Client,
    api: String,
    owner: String,
    package: String,
    token: String,
}

impl Packages {
    /// `name` is the repository path under `ghcr.io`, `<owner>/<package…>`.
    pub fn new(http: reqwest::Client, api: &str, name: &str, token: String) -> Option<Packages> {
        let (owner, package) = name.split_once('/')?;
        Some(Packages {
            http,
            api: api.trim_end_matches('/').to_owned(),
            owner: owner.to_owned(),
            package: package.replace('/', "%2F"),
            token,
        })
    }

    /// Orgs and users have different paths and the name does not say which.
    async fn call(&self, method: Method, path: &str) -> Result<Option<reqwest::Response>, Error> {
        for scope in ["orgs", "users"] {
            let url = format!(
                "{}/{scope}/{}/packages/container/{}/{path}",
                self.api, self.owner, self.package
            );
            let r = self
                .http
                .request(method.clone(), &url)
                .bearer_auth(&self.token)
                .header(header::ACCEPT, "application/vnd.github+json")
                .header("X-GitHub-Api-Version", "2022-11-28")
                .header(header::USER_AGENT, "hestia")
                .send()
                .await?;
            match r.status() {
                StatusCode::NOT_FOUND => continue,
                s if s.is_success() => return Ok(Some(r)),
                _ => return Err(status_error(&url, r).await),
            }
        }
        Ok(None)
    }

    /// Page newest-first until a page holds nothing new.
    pub async fn sync(&self, oci: &Oci, ledger: &mut Ledger) -> Result<(), Error> {
        let mut page = 1;
        loop {
            let path = format!("versions?per_page={PER_PAGE}&page={page}&state=active");
            let Some(r) = self.call(Method::GET, &path).await? else {
                return Ok(());
            };
            let versions: Vec<ApiVersion> = r.json().await?;
            let mut learnt = false;
            for v in &versions {
                if ledger.versions.contains_key(&v.id) {
                    continue;
                }
                let tag = v
                    .metadata
                    .as_ref()
                    .and_then(|m| m.container.as_ref())
                    .and_then(|c| c.tags.first().cloned());
                let key = match tag {
                    Some(t) => Some(t),
                    None => oci.key_of(&v.name).await?,
                };
                // Not ours (no annotation): leave it alone.
                let Some(key) = key else { continue };
                learnt = true;
                ledger.versions.insert(
                    v.id,
                    Version {
                        key,
                        created: parse_timestamp(&v.created_at).unwrap_or(0),
                    },
                );
            }
            if versions.len() < PER_PAGE || !learnt {
                return Ok(());
            }
            page += 1;
        }
    }

    pub async fn delete(&self, ledger: &mut Ledger, key: &str) -> Result<bool, Error> {
        let Some(id) = ledger.id_of(key) else {
            return Ok(false);
        };
        let deleted = self
            .call(Method::DELETE, &format!("versions/{id}"))
            .await?
            .is_some();
        ledger.versions.remove(&id);
        Ok(deleted)
    }
}
