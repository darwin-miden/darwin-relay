//! Real `EthClient` over alloy 1.x. Talks to whatever JSON-RPC URL
//! the binary is configured with — anvil for local dev, Sepolia for
//! staging, mainnet for prod.
//!
//! Resolves the per-basket `DarwinBasketToken` contract address by
//! looking up `basket_id` in the `BasketRegistry` map passed in at
//! construction time. Production swap: read this from the
//! `DarwinStrategy` registry on-chain.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use alloy::network::{Ethereum, EthereumWallet};
use alloy::primitives::{Address, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use async_trait::async_trait;

use super::{EthClient, EthError};

sol! {
    #[sol(rpc)]
    contract DarwinRelayDeposit {
        function claimDeposit(uint256 id) external;
        function confirmDeposit(uint256 id, uint256 basketAmountMinted) external;
        function refundDeposit(uint256 id, string calldata reason) external;
    }

    #[sol(rpc)]
    contract DarwinBasketToken {
        function mintTo(address to, uint256 grossAmount) external returns (uint256 netMinted, uint256 feeMinted);
    }
}

/// Map from basket-id hex (`0x` + 64 hex chars, = keccak256(symbol))
/// to the deployed `DarwinBasketToken` contract address.
pub type BasketRegistry = HashMap<String, Address>;

/// Configuration the binary uses to bring up the live ETH side. All
/// fields are required; the binary falls back to mock if any is
/// unset.
#[derive(Debug, Clone)]
pub struct LiveEthConfig {
    /// HTTP(S) JSON-RPC URL for write transactions (claim, confirm,
    /// refund, mintTo). Example: https://ethereum-sepolia-rpc.publicnode.com
    pub rpc_http: String,
    /// WebSocket JSON-RPC URL for event subscription. Example:
    /// wss://ethereum-sepolia-rpc.publicnode.com
    pub rpc_ws: String,
    /// Operator EOA private key (0x… 32 bytes). Signs every write tx.
    pub operator_key_hex: String,
    /// Deployed `DarwinRelayDeposit` contract address.
    pub relay_address: Address,
    /// Optional basket-id → DarwinBasketToken map. For iter 4 we run
    /// with an empty map and let MintTo legs route to Refunded —
    /// proves the ETH wire works even though basket tokens aren't
    /// deployed yet.
    pub baskets: BasketRegistry,
}

impl LiveEthConfig {
    pub fn from_env() -> Option<Self> {
        let rpc_http = std::env::var("DARWIN_RELAY_ETH_RPC_HTTP").ok()?;
        let rpc_ws = std::env::var("DARWIN_RELAY_ETH_RPC_WS").ok()?;
        let operator_key_hex = std::env::var("DARWIN_RELAY_ETH_OPERATOR_KEY").ok()?;
        let relay_address: Address = std::env::var("DARWIN_RELAY_ETH_CONTRACT")
            .ok()?
            .parse()
            .ok()?;
        let baskets = std::env::var("DARWIN_RELAY_ETH_BASKETS")
            .ok()
            .and_then(|s| serde_json::from_str::<HashMap<String, String>>(&s).ok())
            .map(|m| {
                m.into_iter()
                    .filter_map(|(k, v)| v.parse::<Address>().ok().map(|a| (k, a)))
                    .collect()
            })
            .unwrap_or_default();
        Some(Self {
            rpc_http,
            rpc_ws,
            operator_key_hex,
            relay_address,
            baskets,
        })
    }
}

pub struct AlloyEthClient<P>
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    relay_address: Address,
    baskets: Arc<BasketRegistry>,
    provider: P,
}

impl<P> std::fmt::Debug for AlloyEthClient<P>
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlloyEthClient")
            .field("relay_address", &self.relay_address)
            .field("baskets", &self.baskets.len())
            .finish()
    }
}

impl<P> AlloyEthClient<P>
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    /// Construct from a pre-built provider. Useful for tests that wire
    /// a Provider against anvil and for the binary that builds one
    /// from URL + key.
    pub fn new(provider: P, relay_address: Address, baskets: BasketRegistry) -> Self {
        Self {
            relay_address,
            baskets: Arc::new(baskets),
            provider,
        }
    }

    fn resolve_basket(&self, basket_id: &str) -> Result<Address, EthError> {
        self.baskets
            .get(basket_id)
            .copied()
            .ok_or_else(|| EthError::Permanent(format!("unknown basket_id {basket_id}")))
    }
}

/// Convenience constructor that returns an `AlloyEthClient` over the
/// default alloy HTTP provider stack (wallet-filled, gas-filled,
/// nonce-filled). Used by the binary; tests typically use
/// `AlloyEthClient::new` with anvil's provider.
pub async fn connect_http_alloy_eth_client(
    rpc_url: &str,
    operator_key_hex: &str,
    relay_address: Address,
    baskets: BasketRegistry,
) -> Result<
    AlloyEthClient<impl Provider<Ethereum> + Clone + Send + Sync + 'static>,
    EthError,
> {
    let signer: PrivateKeySigner = operator_key_hex
        .strip_prefix("0x")
        .unwrap_or(operator_key_hex)
        .parse()
        .map_err(|e| EthError::Permanent(format!("operator key parse: {e}")))?;
    let wallet = EthereumWallet::from(signer);
    let url = rpc_url
        .parse()
        .map_err(|e| EthError::Permanent(format!("rpc url parse: {e}")))?;
    let provider = ProviderBuilder::new().wallet(wallet).connect_http(url);
    Ok(AlloyEthClient::new(provider, relay_address, baskets))
}

#[async_trait]
impl<P> EthClient for AlloyEthClient<P>
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    async fn claim_deposit(&self, deposit_id: u64) -> Result<String, EthError> {
        let contract = DarwinRelayDeposit::new(self.relay_address, self.provider.clone());
        let pending = contract
            .claimDeposit(U256::from(deposit_id))
            .send()
            .await
            .map_err(|e| EthError::Transient(format!("claim_deposit send: {e}")))?;
        let tx_hash = *pending.tx_hash();
        // Wait for inclusion before returning so the next FSM step
        // (which reads or transitions on-chain state) sees this tx's
        // effect. Without this, fast back-to-back calls (claim →
        // mintTo → confirm) race the on-chain status check inside
        // `confirmDeposit` and revert with BadStatus.
        let _ = pending
            .with_required_confirmations(1)
            .get_receipt()
            .await
            .map_err(|e| EthError::Transient(format!("claim_deposit await: {e}")))?;
        Ok(format_hash(tx_hash.as_slice()))
    }

    async fn confirm_deposit(
        &self,
        deposit_id: u64,
        basket_amount: u128,
    ) -> Result<String, EthError> {
        let contract = DarwinRelayDeposit::new(self.relay_address, self.provider.clone());
        let pending = contract
            .confirmDeposit(U256::from(deposit_id), U256::from(basket_amount))
            .send()
            .await
            .map_err(|e| EthError::Transient(format!("confirm_deposit send: {e}")))?;
        let tx_hash = *pending.tx_hash();
        let _ = pending
            .with_required_confirmations(1)
            .get_receipt()
            .await
            .map_err(|e| EthError::Transient(format!("confirm_deposit await: {e}")))?;
        Ok(format_hash(tx_hash.as_slice()))
    }

    async fn refund_deposit(&self, deposit_id: u64, reason: &str) -> Result<String, EthError> {
        let contract = DarwinRelayDeposit::new(self.relay_address, self.provider.clone());
        let pending = contract
            .refundDeposit(U256::from(deposit_id), reason.to_string())
            .send()
            .await
            .map_err(|e| EthError::Transient(format!("refund_deposit send: {e}")))?;
        let tx_hash = *pending.tx_hash();
        let _ = pending
            .with_required_confirmations(1)
            .get_receipt()
            .await
            .map_err(|e| EthError::Transient(format!("refund_deposit await: {e}")))?;
        Ok(format_hash(tx_hash.as_slice()))
    }

    async fn mint_basket_to(
        &self,
        basket_id: &str,
        user: &str,
        amount: u128,
    ) -> Result<String, EthError> {
        let basket_addr = self.resolve_basket(basket_id)?;
        let user_addr = Address::from_str(user)
            .map_err(|e| EthError::Permanent(format!("user addr parse: {e}")))?;
        let contract = DarwinBasketToken::new(basket_addr, self.provider.clone());
        let pending = contract
            .mintTo(user_addr, U256::from(amount))
            .send()
            .await
            .map_err(|e| EthError::Transient(format!("mint_basket_to send: {e}")))?;
        let tx_hash = *pending.tx_hash();
        let _ = pending
            .with_required_confirmations(1)
            .get_receipt()
            .await
            .map_err(|e| EthError::Transient(format!("mint_basket_to await: {e}")))?;
        Ok(format_hash(tx_hash.as_slice()))
    }
}

fn format_hash(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_hash_round_trip() {
        let bytes = [0xab; 32];
        let hex = format_hash(&bytes);
        assert_eq!(hex.len(), 2 + 64);
        assert!(hex.starts_with("0xab"));
    }

    #[test]
    fn basket_registry_lookup() {
        let mut reg = BasketRegistry::new();
        let addr = Address::from([1u8; 20]);
        reg.insert("0xdcc".into(), addr);
        assert_eq!(reg.get("0xdcc"), Some(&addr));
        assert_eq!(reg.get("0xdag"), None);
    }
}
