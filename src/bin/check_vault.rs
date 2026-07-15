//! Read an account's fungible-vault balance for a given faucet asset.
//! Used to check the retired v9.3 controller's leftover dUSDC before a
//! defensive sweep.
//!
//!   check_vault --account 0x… [--faucet 0x…(dUSDC default)]

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use miden_client::account::AccountId;
use miden_client::asset::FungibleAsset;
use miden_client::builder::ClientBuilder;
use miden_client::keystore::FilesystemKeyStore;
use miden_client_sqlite_store::SqliteStore;

const DEFAULT_DUSDC: &str = "0xfc90f0f4da30e51168453b60eafed7";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let mut account: Option<String> = None;
    let mut faucet = DEFAULT_DUSDC.to_string();
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--account" => account = Some(args.next().context("--account value")?),
            "--faucet" => faucet = args.next().context("--faucet value")?,
            _ => {}
        }
    }
    let account = AccountId::from_hex(&account.context("--account required")?)?;
    let faucet = AccountId::from_hex(&faucet)?;

    let home = std::env::var("HOME")?;
    let store_path = std::env::var("DARWIN_RELAY_V2_MIDEN_STORE").unwrap_or_else(|_| {
        format!("{home}/data/darwin/.relay-miden-testnet/.miden/store.sqlite3")
    });
    let keystore_path = std::env::var("DARWIN_RELAY_V2_MIDEN_KEYSTORE").unwrap_or_else(|_| {
        format!("{home}/data/darwin/.relay-miden-testnet/.miden/keystore")
    });
    let store = SqliteStore::new(PathBuf::from(&store_path)).await?;
    let mut client = ClientBuilder::<FilesystemKeyStore>::new()
        .grpc_client(&miden_client::rpc::Endpoint::testnet(), None)
        .store(Arc::new(store))
        .filesystem_keystore(PathBuf::from(&keystore_path))?
        .build()
        .await?;

    // Track the (public) account if not already, then sync its state.
    let _ = client.import_account_by_id(account).await;
    client.sync_state().await?;

    let acct = client
        .get_account(account)
        .await?
        .context("account not in store after import + sync")?;
    let bal: u64 = FungibleAsset::new(faucet, 0)
        .map(|fa| fa.vault_key())
        .ok()
        .and_then(|k| acct.vault().get_balance(k).ok())
        .map(u64::from)
        .unwrap_or(0);
    println!(
        "VAULT_BALANCE account={} faucet={} dUSDC_base_units={}",
        account.to_hex(),
        faucet.to_hex(),
        bal
    );
    Ok(())
}
