use futures::{Future, StreamExt, TryFutureExt};
use minimint::outcome::{MismatchingVariant, TransactionStatus, TryIntoOutcome};
use minimint::transaction::Transaction;
use minimint_api::{OutPoint, PeerId, TransactionId};
use reqwest::Url;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use thiserror::Error;

#[derive(Debug)]
/// Mint API client that will try to run queries against all `members` expecting equal
/// results from at least `min_eq_results` of them. Members that return differing results are
/// returned as a member faults list.
pub struct MintApi {
    federation_member_api_hosts: Vec<(PeerId, Url)>,
    min_eq_results: usize,
    http_client: reqwest::Client,
}

pub type Result<T> = std::result::Result<T, ApiError>;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("HTTP error: {0}")]
    HttpError(reqwest::Error),
    #[error("Accepted transaction errored on execution: {0}")]
    TransactionError(String),
    #[error("Out point out of range, transaction got {0} outputs, requested element {1}")]
    OutPointOutOfRange(usize, usize),
    #[error("Returned output type did not match expectation: {0}")]
    WrongOutputType(MismatchingVariant),
    #[error("Could not achieve consensus over the API result")]
    TooManyFaultyPeers,
}

pub type MemberFaults = Vec<PeerId>;

type ParHttpFuture<'a, T> = Pin<Box<dyn Future<Output = (PeerId, reqwest::Result<T>)> + Send + 'a>>;

impl MintApi {
    /// Creates a new API client
    ///
    /// # Panics
    /// * if `members` is empty
    /// * if `min_eq_results` is 0 or greater than `members.len()`
    pub fn new(members: Vec<(PeerId, Url)>, min_eq_results: usize) -> MintApi {
        assert!(
            min_eq_results > 0,
            "Without results we can't return anything, has to be at least 1."
        );
        assert!(!members.is_empty(), "No federation members to query.");
        assert!(
            members.len() >= min_eq_results,
            "Too few federation members to get minimum number of (equal) results."
        );

        MintApi {
            federation_member_api_hosts: members,
            min_eq_results,
            http_client: Default::default(),
        }
    }

    /// Fetch the outcome of an entire transaction
    pub async fn fetch_tx_outcome(&self, tx: TransactionId) -> Result<TransactionStatus> {
        self.get(&format!("/transaction/{}", tx)).await.0
    }

    /// Fetch the outcome of a single transaction output
    pub async fn fetch_output_outcome<T>(&self, out_point: OutPoint) -> Result<T>
    where
        T: TryIntoOutcome,
    {
        match self.fetch_tx_outcome(out_point.txid).await? {
            TransactionStatus::Error(e) => Err(ApiError::TransactionError(e)),
            TransactionStatus::Accepted { outputs, .. } => {
                let outputs_len = outputs.len();
                outputs
                    .into_iter()
                    .nth(out_point.out_idx as usize) // avoid clone as would be necessary with .get(…)
                    .ok_or(ApiError::OutPointOutOfRange(
                        outputs_len,
                        out_point.out_idx as usize,
                    ))
                    .and_then(|output| output.try_into_variant().map_err(ApiError::WrongOutputType))
            }
        }
    }

    /// Submit a transaction to all federtion members
    pub async fn submit_transaction(&self, tx: Transaction) -> Result<TransactionId> {
        self.put("/transaction", tx).await.0
    }

    /// Send a GET request to all federation members and make sure that there is consensus about the
    /// return value between members.
    ///
    /// # Panics
    /// If `api_endpoint` is not a valid relative URL.
    pub async fn get<T>(&self, api_endpoint: &str) -> (Result<T>, MemberFaults)
    where
        T: serde::de::DeserializeOwned + Eq + Hash,
    {
        self.parallel_consistent_http_op(|http_client, id, base_url| {
            Box::pin(async move {
                let request_url = base_url.join(api_endpoint).expect("Invalid API endpoint");
                let response = http_client
                    .get(request_url)
                    .send()
                    .and_then(|resp| resp.json())
                    .await;
                (id, response)
            })
        })
        .await
    }

    /// Send a PUT request to all federation members and make sure that there is consensus about the
    /// return value between members.
    ///
    /// # Panics
    /// If `api_endpoint` is not a valid relative URL.
    pub async fn put<S, R>(&self, api_endpoint: &str, data: S) -> (Result<R>, MemberFaults)
    where
        S: Serialize + Clone + Send + Sync,
        R: DeserializeOwned + Eq + Hash,
    {
        self.parallel_consistent_http_op(|http_client, id, base_url| {
            let cloned_data = data.clone();
            Box::pin(async move {
                let request_url = base_url.join(api_endpoint).expect("Invalid API endpoint");
                let response = http_client
                    .put(request_url)
                    .json(&cloned_data)
                    .send()
                    .and_then(|resp| resp.json())
                    .await;
                (id, response)
            })
        })
        .await
    }

    /// This function is used to run the same HTTP request against multiple endpoint belonging to
    /// different federation members. As soon as `min_eq_results` equal results have been received
    /// it returns the result and a list of federation members that returned a different result. If
    /// no consensus is achieved it returns `Err(ApiError::TooManyFaultyPeers)`.
    ///
    /// The HTTP request to be sent is generated by the function `make_request` that is given a HTTP
    /// client, the id of the federation member and their base url as arguments.
    async fn parallel_consistent_http_op<'a, T, F>(
        &'a self,
        make_request: F,
    ) -> (Result<T>, MemberFaults)
    where
        F: Fn(&'a reqwest::Client, PeerId, &'a Url) -> ParHttpFuture<'a, T>,
        T: serde::de::DeserializeOwned + Eq + Hash,
    {
        let mut requests = futures::stream::iter(self.federation_member_api_hosts.iter())
            .then(|(id, member)| make_request(&self.http_client, *id, member));

        let mut results = HashMap::<_, Vec<PeerId>>::new();
        while let Some((member_id, result)) = requests.next().await {
            results
                .entry(ResultWrapper(result))
                .or_default()
                .push(member_id);

            let agreement = results
                .values()
                .any(|members| members.len() >= self.min_eq_results);

            if agreement {
                // this only works under the assumption that min_eq_results is greater than max_faulty
                let faulty_members = results
                    .values()
                    .filter(|members| members.len() < self.min_eq_results)
                    .flatten()
                    .copied()
                    .collect();

                let result = results
                    .into_iter()
                    .find_map(|(result, members)| {
                        if members.len() >= self.min_eq_results {
                            Some(result.0)
                        } else {
                            None
                        }
                    })
                    .expect("We made sure there is agreement first");

                return (result.map_err(ApiError::HttpError), faulty_members);
            }
        }

        (Err(ApiError::TooManyFaultyPeers), vec![])
    }
}

fn result_eq<T: PartialEq>(a: &reqwest::Result<T>, b: &reqwest::Result<T>) -> bool {
    match (a, b) {
        (Ok(a), Ok(b)) => a == b,
        (Err(a), Err(b)) => {
            if a.is_status() && b.is_status() {
                a.status() == b.status()
            } else {
                false
            }
        }
        (_, _) => false,
    }
}

#[derive(Debug)]
struct ResultWrapper<T>(reqwest::Result<T>);

impl<T> PartialEq for ResultWrapper<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        result_eq(&self.0, &other.0)
    }
}

impl<T> Eq for ResultWrapper<T> where T: Eq + PartialEq {}

impl<T> Hash for ResultWrapper<T>
where
    T: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        match &self.0 {
            Ok(res) => res.hash(state),
            Err(e) => e.status().hash(state),
        }
    }
}

impl From<reqwest::Error> for ApiError {
    fn from(e: reqwest::Error) -> Self {
        ApiError::HttpError(e)
    }
}
