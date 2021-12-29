mod api;

use crate::api::ApiError;
use bitcoin::{Address, Script, Transaction};
use futures::future::JoinAll;
use minimint::config::ClientConfig;
use minimint::modules::mint::tiered::coins::Coins;
use minimint::modules::mint::{
    BlindToken, Coin, CoinNonce, InvalidAmountTierError, Keys, SigResponse, SignRequest,
};
use minimint::modules::wallet::tweakable::Tweakable;
use minimint::modules::wallet::txoproof::{PegInProof, PegInProofError, TxOutProof};
use minimint::modules::wallet::PegOut;
use minimint::transaction as mint_tx;
use minimint_api::db::batch::{BatchItem, DbBatch};
use minimint_api::db::{
    Database, DatabaseKey, DatabaseKeyPrefix, DatabaseKeyPrefixConst, DecodingError, RawDatabase,
};
use minimint_api::encoding::{Decodable, Encodable};
use minimint_api::{Amount, TransactionId};
use minimint_api::{OutPoint, PeerId};
use miniscript::DescriptorTrait;
use rand::{CryptoRng, RngCore};
use reqwest::StatusCode;
use secp256k1_zkp::{All, Secp256k1, Signing};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tbs::{blind_message, unblind_signature, AggregatePublicKey, BlindedMessage, BlindingKey};
use thiserror::Error;
use tokio::time::Duration;
use tracing::debug;

pub const DB_PREFIX_COIN: u8 = 0x20;
pub const DB_PREFIX_OUTPUT_FINALIZATION_DATA: u8 = 0x21;
pub const DB_PREFIX_PEG_IN: u8 = 0x22;

pub struct MintClient {
    cfg: ClientConfig,
    db: Arc<dyn RawDatabase>,
    api: api::MintApi, // TODO: fin way to mock out for testability
    secp: Secp256k1<All>,
}

/// Client side representation of one coin in an issuance request that keeps all necessary
/// information to generate one spendable coin once the blind signature arrives.
#[derive(Debug, Clone, Deserialize, Serialize, Encodable, Decodable)]
pub struct CoinRequest {
    /// Spend key from which the coin nonce (corresponding public key) is derived
    spend_key: [u8; 32], // FIXME: either make KeyPair Serializable or add secret key newtype
    /// Nonce belonging to the secret key
    nonce: CoinNonce,
    /// Key to unblind the blind signature supplied by the mint for this coin
    blinding_key: BlindingKey,
}

/// Client side representation of a coin reissuance that keeps all necessary information to
/// generate spendable coins once the blind signatures arrive.
#[derive(Debug, Clone, Deserialize, Serialize, Encodable, Decodable)]
pub struct CoinFinalizationData {
    /// Finalization data for all coin outputs in this request
    coins: Coins<CoinRequest>,
}

/// Represents a coin that can be spent by us (i.e. we can sign a transaction with the secret key
/// belonging to the nonce.
#[derive(Debug, Clone, Deserialize, Serialize, Encodable, Decodable)]
pub struct SpendableCoin {
    pub coin: Coin,
    pub spend_key: [u8; 32],
}

#[derive(Debug, Clone, Encodable, Decodable)]
pub struct OutputFinalizationKey(OutPoint);

impl DatabaseKeyPrefixConst for OutputFinalizationKey {
    const DB_PREFIX: u8 = DB_PREFIX_OUTPUT_FINALIZATION_DATA;
}

#[derive(Debug, Clone, Encodable, Decodable)]
pub struct OutputFinalizationKeyPrefix;

impl DatabaseKeyPrefixConst for OutputFinalizationKeyPrefix {
    const DB_PREFIX: u8 = DB_PREFIX_OUTPUT_FINALIZATION_DATA;
}

#[derive(Debug, Clone)]
pub struct CoinKey {
    amount: Amount,
    nonce: CoinNonce,
}

#[derive(Debug, Clone)]
pub struct CoinKeyPrefix;

#[derive(Debug, Clone)]
pub struct PegInKey {
    peg_in_script: Script,
}

#[derive(Debug, Clone)]
pub struct PegInPrefixKey;

impl MintClient {
    pub fn new(cfg: ClientConfig, db: Arc<dyn RawDatabase>, secp: Secp256k1<All>) -> Self {
        let api = api::MintApi::new(
            cfg.api_endpoints
                .iter()
                .enumerate()
                .map(|(id, url)| {
                    let peer_id = PeerId::from(id as u16); // FIXME: potentially wrong, currently works imo
                    let url = url.parse().expect("Invalid URL in config");
                    (peer_id, url)
                })
                .collect(),
            cfg.api_endpoints.len(), // TODO: relax full agreement requirement
        );

        MintClient { cfg, db, api, secp }
    }

    pub async fn peg_in<R: RngCore + CryptoRng>(
        &self,
        txout_proof: TxOutProof,
        btc_transaction: Transaction,
        mut rng: R,
    ) -> Result<TransactionId, ClientError> {
        let (output_idx, secret_tweak_key_bytes) = btc_transaction
            .output
            .iter()
            .enumerate()
            .find_map(|(idx, out)| {
                debug!("Output script: {}", out.script_pubkey);
                self.db
                    .get_value::<_, [u8; 32]>(&PegInKey {
                        peg_in_script: out.script_pubkey.clone(),
                    })
                    .expect("DB error")
                    .map(|tweak_secret| (idx, tweak_secret))
            })
            .ok_or(ClientError::NoMatchingPegInFound)?;
        let secret_tweak_key = secp256k1_zkp::schnorrsig::KeyPair::from_seckey_slice(
            &self.secp,
            &secret_tweak_key_bytes,
        )
        .expect("sec key was generated and saved by us");
        let public_tweak_key =
            secp256k1_zkp::schnorrsig::PublicKey::from_keypair(&self.secp, &secret_tweak_key);

        let peg_in_proof = PegInProof::new(
            txout_proof,
            btc_transaction,
            output_idx as u32,
            public_tweak_key,
        )
        .map_err(ClientError::PegInProofError)?;

        peg_in_proof
            .verify(&self.secp, &self.cfg.wallet.peg_in_descriptor)
            .expect("Invalid proof");
        let sats = peg_in_proof.tx_output().value;

        let amount = Amount::from_sat(sats).saturating_sub(self.cfg.fee_consensus.fee_peg_in_abs);
        if amount == Amount::ZERO {
            return Err(ClientError::PegInAmountTooSmall);
        }

        let (coin_finalization_data, sig_req) =
            CoinFinalizationData::new(amount, &self.cfg.mint.tbs_pks, &self.secp, &mut rng);

        let inputs = vec![mint_tx::Input::Wallet(Box::new(peg_in_proof))];
        let outputs = vec![mint_tx::Output::Mint(
            sig_req
                .0
                .into_iter()
                .map(|(amt, token)| (amt, BlindToken(token)))
                .collect(),
        )];

        let peg_in_req_sig = {
            let hash = mint_tx::Transaction::tx_hash_from_parts(&inputs, &outputs);
            let sec_key = secp256k1_zkp::schnorrsig::KeyPair::from_seckey_slice(
                &self.secp,
                &secret_tweak_key_bytes,
            )
            .expect("We checked key validity before saving to DB");

            minimint::transaction::agg_sign(&[sec_key], hash.as_hash(), &self.secp, &mut rng)
        };

        let mint_transaction = mint_tx::Transaction {
            inputs,
            outputs,
            signature: peg_in_req_sig,
        };

        let tx_id = mint_transaction.tx_hash();
        let issuance_key = OutputFinalizationKey(OutPoint {
            txid: tx_id,
            out_idx: 0,
        });

        self.db
            .insert_entry(&issuance_key, &coin_finalization_data)
            .expect("DB error");

        let mint_tx_id = self.api.submit_transaction(mint_transaction).await?;
        assert_eq!(
            tx_id, mint_tx_id,
            "Federation is faulty, returned wrong tx id."
        );
        Ok(tx_id)
    }

    pub async fn fetch_coins(&self, outpoint: OutPoint) -> Result<(), ClientError> {
        let issuance = self
            .db
            .get_value::<_, CoinFinalizationData>(&OutputFinalizationKey(outpoint))
            .expect("DB error")
            .ok_or(ClientError::FinalizationError(
                CoinFinalizationError::UnknowinIssuance,
            ))?;

        let bsig = self
            .api
            .fetch_output_outcome::<Option<SigResponse>>(outpoint)
            .await?
            .ok_or(ClientError::OutputNotReadyYet(outpoint))?;

        let coins = issuance.finalize(bsig, &self.cfg.mint.tbs_pks)?;

        let mut batch = DbBatch::new();
        batch.autocommit(|tx| {
            tx.append_from_iter(coins.into_iter().map(
                |(amount, coin): (Amount, SpendableCoin)| {
                    let key = CoinKey {
                        amount,
                        nonce: coin.coin.0.clone(),
                    };
                    let value = coin;
                    BatchItem::insert_new(key, value)
                },
            ));
            tx.append_delete(OutputFinalizationKey(outpoint));
        });
        self.db.apply_batch(batch).expect("DB error");

        Ok(())
    }

    pub async fn fetch_all_coins(&self) -> Result<Vec<TransactionId>, ClientError> {
        self.db
            .find_by_prefix::<_, OutputFinalizationKey, CoinFinalizationData>(
                &OutputFinalizationKeyPrefix,
            )
            .map(|res| {
                let (id, _) = res.expect("DB error");
                async move {
                    loop {
                        match self.fetch_coins(id.0).await {
                            Ok(()) => return Ok(id.0.txid),
                            // TODO: make mint error more expressive (currently any HTTP error) and maybe use custom return type instead of error for retrying
                            Err(e) if e.is_retryable_fetch_coins() => {
                                tokio::time::sleep(Duration::from_secs(1)).await
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
            })
            .collect::<JoinAll<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<TransactionId>, ClientError>>()
    }

    pub fn coins(&self) -> Coins<SpendableCoin> {
        self.db
            .find_by_prefix::<_, CoinKey, SpendableCoin>(&CoinKeyPrefix)
            .map(|res| {
                let (key, spendable_coin) = res.expect("DB error");
                (key.amount, spendable_coin)
            })
            .collect()
    }

    pub fn spend_coins(&self, coins: &Coins<SpendableCoin>) {
        let mut batch = DbBatch::new();
        batch.autocommit(|tx| {
            tx.append_from_iter(coins.iter().map(|(amount, coin)| {
                BatchItem::delete(CoinKey {
                    amount,
                    nonce: coin.coin.0.clone(),
                })
            }))
        });

        self.db.apply_batch(batch).expect("DB error");
    }

    pub async fn reissue<R: RngCore + CryptoRng>(
        &self,
        coins: Coins<SpendableCoin>,
        mut rng: R,
    ) -> Result<TransactionId, ClientError> {
        let (coin_finalization_data, sig_req) = CoinFinalizationData::new(
            coins.amount(),
            &self.cfg.mint.tbs_pks, // TODO: cache somewhere
            &self.secp,
            &mut rng,
        );

        let (spend_keys, coins): (Vec<_>, Coins<_>) = coins
            .into_iter()
            .map(|(amt, coin)| (coin.spend_key, (amt, coin.coin)))
            .unzip();

        let inputs = vec![mint_tx::Input::Mint(coins)];
        let outputs = vec![mint_tx::Output::Mint(sig_req.into())];

        // TODO: abstract away tx building somehow
        let signature = {
            let hash = mint_tx::Transaction::tx_hash_from_parts(&inputs, &outputs);
            let sec_keys = spend_keys
                .into_iter()
                .map(|key| {
                    secp256k1_zkp::schnorrsig::KeyPair::from_seckey_slice(&self.secp, &key)
                        .expect("We checked key validity before saving to DB")
                })
                .collect::<Vec<_>>();

            minimint::transaction::agg_sign(&sec_keys, hash.as_hash(), &self.secp, &mut rng)
        };

        let transaction = mint_tx::Transaction {
            inputs,
            outputs,
            signature,
        };

        let tx_id = transaction.tx_hash();
        let issuance_key = OutputFinalizationKey(OutPoint {
            txid: tx_id,
            out_idx: 0,
        });
        self.db
            .insert_entry(&issuance_key, &coin_finalization_data)
            .expect("DB error");

        let mint_tx_id = self.api.submit_transaction(transaction).await?;
        assert_eq!(
            tx_id, mint_tx_id,
            "Federation is faulty, returned wrong tx id."
        );
        Ok(tx_id)
    }

    pub async fn peg_out<R: RngCore + CryptoRng>(
        &self,
        amt: bitcoin::Amount,
        address: bitcoin::Address,
        mut rng: R,
    ) -> Result<TransactionId, ClientError> {
        let coins = self
            .coins()
            .select_coins(Amount::from(amt) + self.cfg.fee_consensus.fee_peg_out_abs)
            .ok_or(ClientError::NotEnoughCoins)?;

        // mark spent in DB
        // TODO: make contingent on success of payment
        self.spend_coins(&coins);

        let (spend_keys, coins): (Vec<_>, Coins<_>) = coins
            .into_iter()
            .map(|(amt, coin)| (coin.spend_key, (amt, coin.coin)))
            .unzip();

        let inputs = vec![mint_tx::Input::Mint(coins)];
        let outputs = vec![mint_tx::Output::Wallet(PegOut {
            recipient: address,
            amount: amt,
        })];

        let signature = {
            // FIXME: deduplicate tx signing code
            let hash = mint_tx::Transaction::tx_hash_from_parts(&inputs, &outputs);
            let sec_keys = spend_keys
                .into_iter()
                .map(|key| {
                    secp256k1_zkp::schnorrsig::KeyPair::from_seckey_slice(&self.secp, &key)
                        .expect("We checked key validity before saving to DB")
                })
                .collect::<Vec<_>>();

            minimint::transaction::agg_sign(&sec_keys, hash.as_hash(), &self.secp, &mut rng)
        };

        let transaction = mint_tx::Transaction {
            inputs,
            outputs,
            signature,
        };
        let tx_id = transaction.tx_hash();

        let mint_tx_id = self.api.submit_transaction(transaction).await?;
        assert_eq!(
            tx_id, mint_tx_id,
            "Federation is faulty, returned wrong tx id."
        );
        Ok(tx_id)
    }

    pub fn get_new_pegin_address<R: RngCore + CryptoRng>(&self, mut rng: R) -> Address {
        let peg_in_sec_key = secp256k1_zkp::schnorrsig::KeyPair::new(&self.secp, &mut rng);
        let peg_in_pub_key =
            secp256k1_zkp::schnorrsig::PublicKey::from_keypair(&self.secp, &peg_in_sec_key);

        // TODO: check at startup that no bare descriptor is used in config
        // TODO: check if there are other failure cases
        let script = self
            .cfg
            .wallet
            .peg_in_descriptor
            .tweak(&peg_in_pub_key, &self.secp)
            .script_pubkey();
        debug!("Peg-in script: {}", script);
        let address = Address::from_script(&script, self.cfg.wallet.network)
            .expect("Script from descriptor should have an address");

        self.db
            .insert_entry(
                &PegInKey {
                    peg_in_script: script,
                },
                &peg_in_sec_key.serialize_secret(),
            )
            .expect("DB error");

        address
    }
}

impl CoinFinalizationData {
    /// Generate a new `IssuanceRequest` and the associates [`SignRequest`]
    pub fn new<K, C>(
        amount: Amount,
        amount_tiers: &Keys<K>,
        ctx: &Secp256k1<C>,
        mut rng: impl RngCore + CryptoRng,
    ) -> (CoinFinalizationData, SignRequest)
    where
        C: Signing,
    {
        let (requests, blinded_nonces): (Coins<_>, Coins<_>) =
            Coins::represent_amount(amount, amount_tiers)
                .into_iter()
                .map(|(amt, ())| {
                    let (request, blind_msg) = CoinRequest::new(ctx, &mut rng);
                    ((amt, request), (amt, blind_msg))
                })
                .unzip();

        debug!(
            "Generated issuance request for {} ({} coins, tiers {:?})",
            amount,
            requests.coin_count(),
            requests.coins.keys().collect::<Vec<_>>()
        );

        let sig_req = SignRequest(blinded_nonces);
        let issuance_req = CoinFinalizationData { coins: requests };

        (issuance_req, sig_req)
    }

    /// Finalize the issuance request using a [`SigResponse`] from the mint containing the blind
    /// signatures for all coins in this `IssuanceRequest`. It also takes the mint's
    /// [`AggregatePublicKey`] to validate the supplied blind signatures.
    pub fn finalize(
        &self,
        bsigs: SigResponse,
        mint_pub_key: &Keys<AggregatePublicKey>,
    ) -> Result<Coins<SpendableCoin>, CoinFinalizationError> {
        if !self.coins.structural_eq(&bsigs.0) {
            return Err(CoinFinalizationError::WrongMintAnswer);
        }

        self.coins
            .iter()
            .zip(bsigs.0)
            .enumerate()
            .map(|(idx, ((amt, coin_req), (_amt, bsig)))| {
                let sig = unblind_signature(coin_req.blinding_key, bsig);
                let coin = Coin(coin_req.nonce.clone(), sig);
                if coin.verify(*mint_pub_key.tier(&amt)?) {
                    let coin = SpendableCoin {
                        coin,
                        spend_key: coin_req.spend_key,
                    };

                    Ok((amt, coin))
                } else {
                    Err(CoinFinalizationError::InvalidSignature(idx))
                }
            })
            .collect()
    }

    pub fn coin_count(&self) -> usize {
        self.coins.coins.values().map(|v| v.len()).sum()
    }
}

impl CoinRequest {
    /// Generate a request session for a single coin and returns it plus the corresponding blinded
    /// message
    fn new<C>(
        ctx: &Secp256k1<C>,
        mut rng: impl RngCore + CryptoRng,
    ) -> (CoinRequest, BlindedMessage)
    where
        C: Signing,
    {
        let spend_key = secp256k1_zkp::schnorrsig::KeyPair::new(ctx, &mut rng);
        let nonce = CoinNonce(secp256k1_zkp::schnorrsig::PublicKey::from_keypair(
            ctx, &spend_key,
        ));

        let (blinding_key, blinded_nonce) = blind_message(nonce.to_message());

        let cr = CoinRequest {
            spend_key: spend_key.serialize_secret(),
            nonce,
            blinding_key,
        };

        (cr, blinded_nonce)
    }
}

#[derive(Error, Debug)]
pub enum CoinFinalizationError {
    #[error("The returned answer does not fit the request")]
    WrongMintAnswer,
    #[error("The blind signature at index {0} is invalid")]
    InvalidSignature(usize),
    #[error("Expected signatures for issuance request {0}, got signatures for request {1}")]
    InvalidIssuanceId(TransactionId, TransactionId),
    #[error("Invalid amount tier {0:?}")]
    InvalidAmountTier(Amount),
    #[error("The client does not know this issuance")]
    UnknowinIssuance,
}

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("Error querying federation: {0}")]
    MintApiError(ApiError),
    #[error("Could not finalize issuance request: {0}")]
    FinalizationError(CoinFinalizationError),
    #[error("Could not find an ongoing matching peg-in")]
    NoMatchingPegInFound,
    #[error("Peg-in amount must be greater than peg-in fee")]
    PegInAmountTooSmall,
    #[error("Inconsistent peg-in proof: {0}")]
    PegInProofError(PegInProofError),
    #[error("The client's wallet has not enough coins or they are not in the right denomination")]
    NotEnoughCoins,
    #[error("The transaction outcome received from the mint did not contain a result for output {0} yet")]
    OutputNotReadyYet(OutPoint),
    #[error("The transaction outcome returned by the mint contains too few outputs (output {0})")]
    InvalidOutcomeWrongStructure(OutPoint),
    #[error("The transaction outcome returned by the mint has an invalid type (output {0})")]
    InvalidOutcomeType(OutPoint),
}

impl From<InvalidAmountTierError> for CoinFinalizationError {
    fn from(e: InvalidAmountTierError) -> Self {
        CoinFinalizationError::InvalidAmountTier(e.0)
    }
}

impl From<CoinFinalizationError> for ClientError {
    fn from(e: CoinFinalizationError) -> Self {
        ClientError::FinalizationError(e)
    }
}

impl From<ApiError> for ClientError {
    fn from(e: ApiError) -> Self {
        ClientError::MintApiError(e)
    }
}

impl DatabaseKeyPrefix for CoinKey {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(9);
        bytes.push(DB_PREFIX_COIN);
        bytes.extend_from_slice(&self.amount.milli_sat.to_be_bytes()[..]);
        bytes.extend_from_slice(&self.nonce.to_bytes());

        bytes
    }
}

impl DatabaseKey for CoinKey {
    fn from_bytes(data: &[u8]) -> Result<Self, DecodingError> {
        if data.len() < 9 {
            Err(DecodingError::wrong_length(9, data.len()))
        } else if data[0] != DB_PREFIX_COIN {
            Err(DecodingError::wrong_prefix(DB_PREFIX_COIN, data[0]))
        } else {
            let mut amount_bytes = [0u8; 8];
            amount_bytes.copy_from_slice(&data[1..9]);
            let amount = Amount {
                milli_sat: u64::from_be_bytes(amount_bytes),
            };

            let nonce = CoinNonce::from_bytes(&data[9..]);

            Ok(CoinKey { amount, nonce })
        }
    }
}

impl DatabaseKeyPrefix for CoinKeyPrefix {
    fn to_bytes(&self) -> Vec<u8> {
        vec![DB_PREFIX_COIN]
    }
}

impl DatabaseKeyPrefix for PegInKey {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![DB_PREFIX_PEG_IN];
        bytes.extend_from_slice(&self.peg_in_script[..]);
        bytes
    }
}

impl DatabaseKey for PegInKey {
    fn from_bytes(data: &[u8]) -> Result<Self, DecodingError> {
        if data.is_empty() {
            Err(DecodingError::wrong_length(1, data.len()))
        } else if data[0] != DB_PREFIX_PEG_IN {
            Err(DecodingError::wrong_prefix(DB_PREFIX_PEG_IN, data[0]))
        } else {
            Ok(PegInKey {
                peg_in_script: Script::from(data[1..].to_vec()),
            })
        }
    }
}

impl DatabaseKeyPrefix for PegInPrefixKey {
    fn to_bytes(&self) -> Vec<u8> {
        vec![DB_PREFIX_PEG_IN]
    }
}

impl ClientError {
    pub fn is_retryable_fetch_coins(&self) -> bool {
        match self {
            ClientError::MintApiError(ApiError::HttpError(e)) => {
                e.status() == Some(StatusCode::NOT_FOUND)
            }
            ClientError::OutputNotReadyYet(_) => true,
            _ => false,
        }
    }
}
