//! v10 spike — emit a MINT-request note at the network basket faucet.
//!
//! The confidential deposit's core: the network faucet, driven by the
//! NTX builder, mints basket tokens into a PRIVATE P2ID note the user
//! alone can claim — so who-holds-what stays confidential. This uses the
//! standard MINT note (private mode) which calls the faucet's
//! mint_and_send. No collateral drain yet (that's the full deposit's
//! extra leg); this proves the mint-to-private-note mechanism end to end.
//!
//!   --print-root   → the MINT note script root (allowlist it on the faucet)
//!   --faucet <ID> --recipient <ACCT> --amount N → emit + wait + consume
//!
//! Usage:
//!   cargo run --release --features v2-worker --bin send_mint_request -- \
//!       --faucet 0x06da94817962025116db9faa484fe1 \
//!       --recipient 0x66e7105ea36a7491325480accb7331 --amount 500000

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use miden_client::account::AccountId;
use miden_client::asset::FungibleAsset;
use miden_client::builder::ClientBuilder;
use miden_client::keystore::FilesystemKeyStore;
use miden_client::note::{Note, NoteType, PartialNoteMetadata};
use miden_client::transaction::TransactionRequestBuilder;
use miden_client_sqlite_store::SqliteStore;
use miden_protocol::note::{NoteAttachment, NoteAttachments, NoteTag};
use miden_standards::note::{
    MintNote, MintNoteStorage, NetworkAccountTarget, NoteExecutionHint, P2idNoteStorage,
};
use rand::RngCore;

const DEFAULT_SENDER: &str = "0x66e7105ea36a7491325480accb7331";

fn rand_word() -> Result<miden_client::Word> {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    Ok(miden_client::Word::try_from(
        bytes
            .chunks_exact(8)
            .map(|c| {
                let mut b = [0u8; 8];
                b.copy_from_slice(c);
                miden_client::Felt::new(u64::from_le_bytes(b) & 0xFFFF_FFFE_FFFF_FFFF)
                    .expect("goldilocks")
            })
            .collect::<Vec<_>>()
            .as_slice(),
    )?)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let mut faucet: Option<String> = None;
    let mut sender = DEFAULT_SENDER.to_string();
    let mut recipient: Option<String> = None;
    let mut amount: u64 = 500_000;
    let mut print_root = false;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--faucet" => faucet = Some(args.next().context("--faucet value")?),
            "--sender" => sender = args.next().context("--sender value")?,
            "--recipient" => recipient = Some(args.next().context("--recipient value")?),
            "--amount" => amount = args.next().context("--amount value")?.parse()?,
            "--print-root" => print_root = true,
            _ => {}
        }
    }

    println!("MINT note script root: {}", MintNote::script_root());
    if print_root {
        return Ok(());
    }

    let faucet = AccountId::from_hex(&faucet.context("--faucet required")?)?;
    let sender = AccountId::from_hex(&sender)?;
    let recipient = AccountId::from_hex(&recipient.unwrap_or_else(|| DEFAULT_SENDER.to_string()))?;

    // The DCC asset the faucet will mint.
    let mint_asset = FungibleAsset::new(faucet, amount)?;

    // Private payback: the minted DCC lands in a P2ID note only the
    // recipient can consume. We precompute its recipient digest (same
    // pattern as the redeem payback) so we can reconstruct + consume it.
    let payback_serial = rand_word()?;
    let payback_recipient = P2idNoteStorage::new(recipient).into_recipient(payback_serial);
    let payback_tag = NoteTag::with_account_target(recipient);

    let mint_storage = MintNoteStorage::new_private(
        payback_recipient.digest(),
        mint_asset,
        payback_tag.into(),
    );

    // Route the MINT note at the network faucet.
    let na_target = NetworkAccountTarget::new(faucet, NoteExecutionHint::Always)
        .map_err(|e| anyhow::anyhow!("NetworkAccountTarget: {e:?}"))?;
    let attachments = NoteAttachments::new(vec![NoteAttachment::from(na_target)])
        .map_err(|e| anyhow::anyhow!("NoteAttachments: {e:?}"))?;

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
    client.sync_state().await?;

    let mint_note = MintNote::create(
        faucet,
        sender,
        mint_storage,
        attachments,
        client.rng(),
    )
    .map_err(|e| anyhow::anyhow!("MintNote::create: {e:?}"))?;
    let note_id = mint_note.id();

    println!("Emitting MINT request at network faucet…");
    println!("    faucet    : {}", faucet.to_hex());
    println!("    mint      : {amount} DCC → private note for {}", recipient.to_hex());
    println!("    note id   : {note_id}");

    let req = TransactionRequestBuilder::new()
        .own_output_notes(vec![mint_note])
        .build()?;
    let result = client.execute_transaction(sender, req).await?;
    let tx_id = result.executed_transaction().id();
    let prover = client.prover();
    let proven = client.prove_transaction_with(&result, prover.clone()).await?;
    let height = client.submit_proven_transaction(proven, &result).await?;
    client.apply_transaction(&result, height).await?;
    println!("    emit tx   : {tx_id} (height {height})");

    // Reconstruct + import the private payback (the minted DCC note), wait
    // for the NTB to create it, then consume into the recipient wallet.
    let payback_note = Note::new(
        miden_client::note::NoteAssets::new(vec![
            miden_client::asset::Asset::Fungible(mint_asset),
        ])?,
        PartialNoteMetadata::new(faucet, NoteType::Private).with_tag(payback_tag),
        P2idNoteStorage::new(recipient).into_recipient(payback_serial),
    );
    let payback_id = payback_note.id();
    println!("    minted note id: {payback_id}");
    let details: miden_protocol::note::NoteDetails = payback_note.clone().into();
    client
        .import_notes(&[miden_protocol::note::NoteFile::NoteDetails {
            details,
            after_block_num: height,
            tag: Some(payback_tag),
        }])
        .await?;

    println!();
    println!("Waiting for the network to mint the private DCC note…");
    for i in 0..25 {
        tokio::time::sleep(std::time::Duration::from_secs(6)).await;
        client.sync_state().await?;
        if let Some(rec) = client.get_input_note(payback_id).await? {
            if rec.is_committed() {
                println!("    minted after ~{}s — consuming…", (i + 1) * 6);
                let creq = TransactionRequestBuilder::new()
                    .build_consume_notes(vec![payback_note.clone()])
                    .map_err(|e| anyhow::anyhow!("consume req: {e:?}"))?;
                let cres = client.execute_transaction(recipient, creq).await?;
                let ctx = cres.executed_transaction().id();
                let cproven = client.prove_transaction_with(&cres, prover.clone()).await?;
                let cheight = client.submit_proven_transaction(cproven, &cres).await?;
                client.apply_transaction(&cres, cheight).await?;
                println!("    ✓ {amount} DCC minted into {} — consume tx {ctx} (height {cheight})", recipient.to_hex());
                println!();
                println!("CONFIDENTIAL DEPOSIT PROVEN: the network faucet minted");
                println!("basket tokens into a private note; the recipient holds");
                println!("them in their own account — no public per-user ledger.");
                return Ok(());
            }
        }
    }
    println!("    not minted after 150s — check network-note-status {note_id}");
    Ok(())
}
