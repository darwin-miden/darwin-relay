//! v10 confidential redeem — emit the confidential_redeem_note carrying
//! basket tokens at the faucet-network account. The NTX builder burns the
//! tokens (reduces supply) and releases the pro-rata dUSDC from the vault
//! into a PRIVATE note for the redeemer. Full redeem in one network tx.
//!
//!   --print-root                                → note script root
//!   --faucet <ID> --recipient <ACCT> --amount N → emit + wait + consume
//!
//! Spike: releases `amount` dUSDC 1:1 for `amount` burned tokens.
//!
//!   cargo run --release --features v2-worker --bin send_confidential_redeem -- \
//!       --faucet 0x… --recipient 0x66e7105ea36a… --amount 100000

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use miden_assembly::ast::{Module, ModuleKind};
use miden_assembly::{Assembler, DefaultSourceManager, Path as MidenPath};
use miden_client::account::AccountId;
use miden_client::asset::{Asset, FungibleAsset};
use miden_client::builder::ClientBuilder;
use miden_client::keystore::FilesystemKeyStore;
use miden_client::note::{
    Note, NoteAssets, NoteRecipient, NoteScript, NoteStorage, NoteTag, NoteType,
    PartialNoteMetadata,
};
use miden_client::transaction::TransactionRequestBuilder;
use miden_client_sqlite_store::SqliteStore;
use miden_core_lib::CoreLibrary;
use miden_protocol::note::{NoteAttachment, NoteAttachments};
use miden_protocol::transaction::TransactionKernel;
use miden_standards::StandardsLib;
use miden_standards::note::{NetworkAccountTarget, NoteExecutionHint, P2idNoteStorage};
use rand::RngCore;

const NOTE_MASM: &str = include_str!("../../asm/confidential_redeem_note.masm");
const MATH_MASM: &str = include_str!("../../asm/lib/math.masm");
const DEFAULT_SENDER: &str = "0x66e7105ea36a7491325480accb7331";
const DEFAULT_DUSDC: &str = "0xfc90f0f4da30e51168453b60eafed7";

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

fn compile_note_script() -> Result<NoteScript> {
    let core_lib = CoreLibrary::default();
    let standards_lib = StandardsLib::default();
    let sm: Arc<dyn miden_assembly::SourceManager> = Arc::new(DefaultSourceManager::default());
    let math_module = Module::parser(ModuleKind::Library)
        .parse_str(MidenPath::new("darwin::math"), MATH_MASM, sm.clone())
        .map_err(|e| anyhow::anyhow!("parse math.masm: {e}"))?;
    let math_lib = Assembler::default()
        .with_static_library(core_lib.as_ref())
        .map_err(|e| anyhow::anyhow!("attach core_lib: {e}"))?
        .assemble_library([math_module])
        .map_err(|e| anyhow::anyhow!("assemble math.masm: {e}"))?;
    let program = TransactionKernel::assembler()
        .with_static_library(core_lib.as_ref())
        .map_err(|e| anyhow::anyhow!("attach core_lib: {e}"))?
        .with_static_library(standards_lib.as_ref())
        .map_err(|e| anyhow::anyhow!("attach standards_lib: {e}"))?
        .with_static_library(math_lib.as_ref())
        .map_err(|e| anyhow::anyhow!("attach math_lib: {e}"))?
        .assemble_program(NOTE_MASM)
        .map_err(|e| anyhow::anyhow!("assemble note: {e:#}"))?;
    Ok(NoteScript::new(program))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let mut faucet: Option<String> = None;
    let mut sender = DEFAULT_SENDER.to_string();
    let mut recipient: Option<String> = None;
    let mut collateral_faucet = DEFAULT_DUSDC.to_string();
    let mut amount: u64 = 100_000;
    let mut print_root = false;
    let mut emit_json = false;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--faucet" => faucet = Some(args.next().context("--faucet value")?),
            "--sender" => sender = args.next().context("--sender value")?,
            "--recipient" => recipient = Some(args.next().context("--recipient value")?),
            "--collateral" => collateral_faucet = args.next().context("--collateral value")?,
            "--amount" => amount = args.next().context("--amount value")?.parse()?,
            "--print-root" => print_root = true,
            "--emit-json" => emit_json = true,
            _ => {}
        }
    }

    let note_script = compile_note_script()?;
    println!("confidential redeem note root: {}", note_script.root());
    if print_root {
        return Ok(());
    }

    let faucet = AccountId::from_hex(&faucet.context("--faucet required")?)?;
    let sender = AccountId::from_hex(&sender)?;
    let recipient = AccountId::from_hex(&recipient.unwrap_or_else(|| DEFAULT_SENDER.to_string()))?;
    let collateral_faucet = AccountId::from_hex(&collateral_faucet)?;

    // Private payback P2ID for the released dUSDC.
    let payback_serial = rand_word()?;
    let payback_recipient = P2idNoteStorage::new(recipient).into_recipient(payback_serial);
    let payback_tag = NoteTag::with_account_target(recipient);
    let release_asset = FungibleAsset::new(collateral_faucet, amount)?;

    let mut storage_felts: Vec<miden_client::Felt> = Vec::new();
    storage_felts.extend_from_slice(payback_recipient.digest().as_elements()); // 100..103
    storage_felts.push(NoteType::Private.into()); // 104
    storage_felts.push(payback_tag.into()); // 105
    storage_felts.push(miden_client::Felt::new(0).unwrap()); // 106 pad
    storage_felts.push(miden_client::Felt::new(0).unwrap()); // 107 pad
    storage_felts.extend_from_slice(&Asset::Fungible(release_asset).as_elements()); // 108..115

    // The note carries the basket tokens to burn.
    let burn_asset = FungibleAsset::new(faucet, amount)?;
    let assets = NoteAssets::new(vec![Asset::Fungible(burn_asset)])?;
    let na_target = NetworkAccountTarget::new(faucet, NoteExecutionHint::Always)
        .map_err(|e| anyhow::anyhow!("NetworkAccountTarget: {e:?}"))?;
    let attachments = NoteAttachments::new(vec![NoteAttachment::from(na_target)])
        .map_err(|e| anyhow::anyhow!("NoteAttachments: {e:?}"))?;
    let metadata = PartialNoteMetadata::new(sender, NoteType::Public)
        .with_tag(NoteTag::with_account_target(faucet));
    let note_recipient =
        NoteRecipient::new(rand_word()?, note_script, NoteStorage::new(storage_felts)?);
    let note = Note::with_attachments(assets, metadata, note_recipient, attachments);
    let note_id = note.id();

    if emit_json {
        use base64::Engine as _;
        use miden_protocol::utils::serde::Serializable;
        let note_b64 = base64::engine::general_purpose::STANDARD.encode(note.to_bytes());
        // Private payback: the released dUSDC the browser imports + consumes.
        let release_payback = FungibleAsset::new(collateral_faucet, amount)?;
        let payback_note = Note::new(
            NoteAssets::new(vec![Asset::Fungible(release_payback)])?,
            PartialNoteMetadata::new(faucet, NoteType::Private).with_tag(payback_tag),
            P2idNoteStorage::new(recipient).into_recipient(payback_serial),
        );
        let payback_file = miden_protocol::note::NoteFile::NoteDetails {
            details: payback_note.clone().into(),
            after_block_num: 0u32.into(),
            tag: Some(payback_tag),
        };
        let payback_b64 =
            base64::engine::general_purpose::STANDARD.encode(payback_file.to_bytes());
        println!(
            "{}",
            serde_json::json!({
                "noteId": note_id.to_string(),
                "noteB64": note_b64,
                "paybackId": payback_note.id().to_string(),
                "paybackFileB64": payback_b64,
                "releaseAmount": amount.to_string(),
            })
        );
        return Ok(());
    }

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

    println!("Emitting confidential redeem at network faucet…");
    println!("    faucet    : {}", faucet.to_hex());
    println!("    burn      : {amount} basket tokens → release {amount} dUSDC (1:1 spike)");
    println!("    recipient : {} (private)", recipient.to_hex());
    println!("    note id   : {note_id}");

    let req = TransactionRequestBuilder::new().own_output_notes(vec![note]).build()?;
    let result = client.execute_transaction(sender, req).await?;
    let tx_id = result.executed_transaction().id();
    let prover = client.prover();
    let proven = client.prove_transaction_with(&result, prover.clone()).await?;
    let height = client.submit_proven_transaction(proven, &result).await?;
    client.apply_transaction(&result, height).await?;
    println!("    emit tx   : {tx_id} (height {height})");

    let payback_note = Note::new(
        NoteAssets::new(vec![Asset::Fungible(release_asset)])?,
        PartialNoteMetadata::new(faucet, NoteType::Private).with_tag(payback_tag),
        P2idNoteStorage::new(recipient).into_recipient(payback_serial),
    );
    let payback_id = payback_note.id();
    println!("    released note id: {payback_id}");
    let details: miden_protocol::note::NoteDetails = payback_note.clone().into();
    client
        .import_notes(&[miden_protocol::note::NoteFile::NoteDetails {
            details,
            after_block_num: height,
            tag: Some(payback_tag),
        }])
        .await?;

    println!();
    println!("Waiting for the network to burn tokens + release the private dUSDC note…");
    for i in 0..40 {
        tokio::time::sleep(std::time::Duration::from_secs(6)).await;
        // Testnet occasionally returns a transient sync race
        // ("block_to > chain tip") — don't abort the whole redeem on it,
        // just skip this tick and retry on the next.
        if let Err(e) = client.sync_state().await {
            println!("    (sync retry: {})", e.to_string().lines().next().unwrap_or(""));
            continue;
        }
        let maybe = match client.get_input_note(payback_id).await {
            Ok(m) => m,
            Err(e) => {
                println!("    (get_note retry: {})", e.to_string().lines().next().unwrap_or(""));
                continue;
            }
        };
        if let Some(rec) = maybe {
            if rec.is_committed() {
                println!("    released after ~{}s — consuming…", (i + 1) * 6);
                let creq = TransactionRequestBuilder::new()
                    .build_consume_notes(vec![payback_note.clone()])
                    .map_err(|e| anyhow::anyhow!("consume req: {e:?}"))?;
                let cres = client.execute_transaction(recipient, creq).await?;
                let ctx = cres.executed_transaction().id();
                let cproven = client.prove_transaction_with(&cres, prover.clone()).await?;
                let cheight = client.submit_proven_transaction(cproven, &cres).await?;
                client.apply_transaction(&cres, cheight).await?;
                println!("    ✓ {amount} dUSDC released to {} — consume tx {ctx}", recipient.to_hex());
                println!();
                println!("CONFIDENTIAL REDEEM PROVEN: basket tokens burned, dUSDC");
                println!("released from the vault to a private note — one network tx.");
                return Ok(());
            }
        }
    }
    println!("    not released after ~240s — check network-note-status {note_id}");
    Ok(())
}
