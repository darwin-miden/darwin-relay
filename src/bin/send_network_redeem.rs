//! Emit a network REDEEM request note at the v9.3 network controller.
//!
//! The note carries no assets — it's a request the NTX builder executes
//! against the controller: debit the (user, basket) slot-10 position and
//! pay `amount` dUSDC from the controller's vault to the recipient
//! wallet via a payback P2ID note (swap-note pattern).
//!
//!   --print-root                       → note script root for the allowlist
//!   --target <CTRL> --recipient <ACCT> → emit from the relay wallet
//!   --emit-json --sender <ACCT>        → serialized note for the browser
//!
//!     cargo run --release --features v2-worker --bin send_network_redeem -- \
//!         --target 0x… --recipient 0x66e7105ea36a… --amount 50000 --basket DCC

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
use miden_standards::note::{NetworkAccountTarget, NoteExecutionHint, P2idNoteStorage};
use miden_standards::StandardsLib;
use rand::RngCore;

const NOTE_MASM: &str = include_str!("../../asm/network_redeem_note.masm");
const MATH_MASM: &str = include_str!("../../asm/lib/math.masm");

const DEFAULT_SENDER: &str = "0x66e7105ea36a7491325480accb7331";
const DEFAULT_DUSDC: &str = "0xfc90f0f4da30e51168453b60eafed7";
const BASKETS: &[(&str, &str)] = &[
    ("DCC", "0x4eb76287e07e90714a86ae2b89d700"),
    ("DAG", "0xed4219cb5ebf3d911c27dc6b24baa2"),
    ("DCO", "0xc58107b160df13d1157b707e3f0a3d"),
];

fn evm_to_user_felts(evm: &str) -> Result<(u64, u64)> {
    let hex = evm.trim_start_matches("0x");
    anyhow::ensure!(hex.len() == 40, "EVM address must be 20 bytes");
    let bytes: Vec<u8> = (0..20)
        .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16))
        .collect::<Result<_, _>>()?;
    let le_u64 = |slice: &[u8]| {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(slice);
        u64::from_le_bytes(buf) & 0x7FFF_FFFF_FFFF_FFFF
    };
    Ok((le_u64(&bytes[12..20]), le_u64(&bytes[4..12])))
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

fn rand_word() -> Result<miden_client::Word> {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    Ok(miden_client::Word::try_from(
        bytes
            .chunks_exact(8)
            .map(|chunk| {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(chunk);
                miden_client::Felt::new(u64::from_le_bytes(buf) & 0xFFFF_FFFE_FFFF_FFFF)
                    .expect("masked to Goldilocks safe range")
            })
            .collect::<Vec<_>>()
            .as_slice(),
    )?)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let mut target: Option<String> = None;
    let mut sender = DEFAULT_SENDER.to_string();
    let mut recipient: Option<String> = None;
    let mut faucet = DEFAULT_DUSDC.to_string();
    let mut user_evm = "0xf6d3C9Ed2115A5197F96f6189F6D63B51022Fe16".to_string();
    let mut basket = "DCC".to_string();
    let mut amount: u64 = 50_000;
    let mut print_root = false;
    let mut emit_json = false;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--target" => target = Some(args.next().context("--target value")?),
            "--sender" => sender = args.next().context("--sender value")?,
            "--recipient" => recipient = Some(args.next().context("--recipient value")?),
            "--faucet" => faucet = args.next().context("--faucet value")?,
            "--user-evm" => user_evm = args.next().context("--user-evm value")?,
            "--basket" => basket = args.next().context("--basket value")?,
            "--amount" => amount = args.next().context("--amount value")?.parse()?,
            "--print-root" => print_root = true,
            "--emit-json" => emit_json = true,
            _ => {}
        }
    }

    let note_script = compile_note_script()?;
    println!("note script root: {}", note_script.root());
    if print_root {
        return Ok(());
    }

    let target = AccountId::from_hex(&target.context("--target required")?)?;
    let sender = AccountId::from_hex(&sender)?;
    // Payback recipient defaults to the sender wallet.
    let recipient = AccountId::from_hex(&recipient.unwrap_or_else(|| DEFAULT_SENDER.to_string()))?;
    let faucet = AccountId::from_hex(&faucet)?;
    let basket_hex = BASKETS
        .iter()
        .find(|(sym, _)| *sym == basket)
        .with_context(|| format!("unknown basket {basket}"))?
        .1;
    let basket_faucet = AccountId::from_hex(basket_hex)?;
    let (user_suffix, user_prefix) = evm_to_user_felts(&user_evm)?;

    // --- payback P2ID pre-computation (swap-note pattern) ---
    let payback_serial = rand_word()?;
    let payback_recipient = P2idNoteStorage::new(recipient).into_recipient(payback_serial);
    let payback_tag = NoteTag::with_account_target(recipient);
    let release_asset = Asset::Fungible(FungibleAsset::new(faucet, amount)?);

    // --- redeem request note storage ---
    let mut storage_felts: Vec<miden_client::Felt> = Vec::new();
    storage_felts.extend_from_slice(&release_asset.as_elements()); // 100..107
    storage_felts.extend_from_slice(payback_recipient.digest().as_elements()); // 108..111
    // Private: a public output note needs its full details in the
    // executor's advice provider, which the NTB doesn't have — only the
    // redeemer knows them (it computed the recipient). Privacy bonus.
    storage_felts.push(NoteType::Private.into()); // 112
    storage_felts.push(payback_tag.into()); // 113
    storage_felts.push(miden_client::Felt::new(amount).expect("amount fits felt")); // 114
    storage_felts.push(miden_client::Felt::new(user_suffix).expect("63-bit")); // 115
    storage_felts.push(miden_client::Felt::new(user_prefix).expect("63-bit")); // 116
    storage_felts.push(basket_faucet.suffix()); // 117
    storage_felts.push(basket_faucet.prefix().as_felt()); // 118

    let na_target = NetworkAccountTarget::new(target, NoteExecutionHint::Always)
        .map_err(|e| anyhow::anyhow!("NetworkAccountTarget: {e:?}"))?;
    let attachments = NoteAttachments::new(vec![NoteAttachment::from(na_target)])
        .map_err(|e| anyhow::anyhow!("NoteAttachments: {e:?}"))?;
    let metadata = PartialNoteMetadata::new(sender, NoteType::Public)
        .with_tag(NoteTag::with_account_target(target));
    let note_recipient =
        NoteRecipient::new(rand_word()?, note_script, NoteStorage::new(storage_felts)?);
    // Request note carries NO assets.
    let note = Note::with_attachments(
        NoteAssets::new(vec![])?,
        metadata,
        note_recipient,
        attachments,
    );
    let note_id = note.id();

    if emit_json {
        use base64::Engine as _;
        use miden_protocol::utils::serde::Serializable;
        let b64 = base64::engine::general_purpose::STANDARD.encode(note.to_bytes());
        println!(
            "{}",
            serde_json::json!({
                "noteId": note_id.to_string(),
                "noteB64": b64,
                "paybackTag": payback_tag.as_u32(),
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

    println!("Connecting miden-client (testnet)…");
    let store = SqliteStore::new(PathBuf::from(&store_path)).await?;
    let endpoint = miden_client::rpc::Endpoint::testnet();
    let mut client = ClientBuilder::<FilesystemKeyStore>::new()
        .grpc_client(&endpoint, None)
        .store(Arc::new(store))
        .filesystem_keystore(PathBuf::from(&keystore_path))?
        .build()
        .await?;
    client.sync_state().await?;

    println!("Emitting network redeem request…");
    println!("    controller : {}", target.to_hex());
    println!("    debit      : {amount} base units, basket {basket}, user {user_evm}");
    println!("    payback to : {}", recipient.to_hex());
    println!("    note id    : {note_id}");

    let req = TransactionRequestBuilder::new()
        .own_output_notes(vec![note])
        .build()?;
    let result = client.execute_transaction(sender, req).await?;
    let tx_id = result.executed_transaction().id();
    let prover = client.prover();
    let proven = client.prove_transaction_with(&result, prover.clone()).await?;
    let height = client.submit_proven_transaction(proven, &result).await?;
    client.apply_transaction(&result, height).await?;

    println!();
    println!("✓ redeem request emitted");
    println!("    emit tx : {tx_id}");
    println!("    height  : {height}");

    // Reconstruct the private payback note (we know serial + script +
    // storage), import its details, and wait for the NTB to create it.
    let payback_assets = NoteAssets::new(vec![release_asset])?;
    let payback_metadata = PartialNoteMetadata::new(target, NoteType::Private)
        .with_tag(payback_tag);
    let payback_note = Note::new(
        payback_assets,
        payback_metadata,
        P2idNoteStorage::new(recipient).into_recipient(payback_serial),
    );
    let payback_id = payback_note.id();
    println!("    payback note id: {payback_id}");

    let details: miden_protocol::note::NoteDetails = payback_note.clone().into();
    client
        .import_notes(&[miden_protocol::note::NoteFile::NoteDetails {
            details,
            after_block_num: height,
            tag: Some(payback_tag),
        }])
        .await?;

    println!();
    println!("Waiting for the network to create the payback note…");
    for i in 0..20 {
        tokio::time::sleep(std::time::Duration::from_secs(6)).await;
        client.sync_state().await?;
        let rec = client.get_input_note(payback_id).await?;
        if let Some(rec) = rec {
            if rec.is_committed() {
                println!("    payback committed after ~{}s — consuming…", (i + 1) * 6);
                let creq = TransactionRequestBuilder::new()
                    .build_consume_notes(vec![payback_note.clone()])
                    .map_err(|e| anyhow::anyhow!("consume req: {e:?}"))?;
                let cres = client.execute_transaction(recipient, creq).await?;
                let ctx = cres.executed_transaction().id();
                let cproven = client.prove_transaction_with(&cres, prover.clone()).await?;
                let cheight = client.submit_proven_transaction(cproven, &cres).await?;
                client.apply_transaction(&cres, cheight).await?;
                println!("    ✓ payback consumed — tx {ctx} at height {cheight}");
                return Ok(());
            }
        }
    }
    println!("    payback not committed after 120s — check network-note-status {note_id}");

    Ok(())
}
