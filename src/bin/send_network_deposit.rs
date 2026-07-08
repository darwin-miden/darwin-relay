//! Emit an atomic_deposit_note (v2, v0.15 MAST roots) at a NETWORK
//! controller and let the Network Transaction Builder execute it.
//!
//! This is the full network-driven deposit: the note drains its dUSDC
//! into the controller vault (receive_asset) AND accumulates the user's
//! per-(user, basket) slot-10 position (get/set_user_position) — all
//! executed by the network, no operator key, no relay transaction.
//!
//! Two-phase usage:
//!   1. --print-root                 → compile the note script, print its
//!                                     root; feed it to deploy_v9_network
//!                                     --allow-root <root>.
//!   2. --target <ACCOUNT> [...]     → emit the note from the relay wallet.
//!
//! The position mirrors the self-custody rail's accounting: raw dUSDC
//! base units per (user EVM, basket) key — fee_factor = nav_scale = 1 so
//! mint_amount == deposit_value.
//!
//!     cargo run --release --features v2-worker --bin send_network_deposit -- \
//!         --target 0x… --amount 100000 --user-evm 0xf6d3…Fe16 --basket DCC

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
use miden_standards::note::{NetworkAccountTarget, NoteExecutionHint};
use rand::RngCore;

const NOTE_MASM: &str = include_str!("../../asm/atomic_deposit_note_v2_015.masm");
const MATH_MASM: &str = include_str!("../../asm/lib/math.masm");

const DEFAULT_SENDER: &str = "0x66e7105ea36a7491325480accb7331";
const DEFAULT_DUSDC: &str = "0xfc90f0f4da30e51168453b60eafed7";
// v0.15 basket-token faucets (same table as the frontend).
const BASKETS: &[(&str, &str)] = &[
    ("DCC", "0x4eb76287e07e90714a86ae2b89d700"),
    ("DAG", "0xed4219cb5ebf3d911c27dc6b24baa2"),
    ("DCO", "0xc58107b160df13d1157b707e3f0a3d"),
];

/// Same encoding as the frontend's evmToUserIdFelts: EVM address bytes
/// 12..20 → suffix, 4..12 → prefix, both LE u64 masked to 63 bits.
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
        .with_static_library(math_lib.as_ref())
        .map_err(|e| anyhow::anyhow!("attach math_lib: {e}"))?
        .assemble_program(NOTE_MASM)
        .map_err(|e| anyhow::anyhow!("assemble note: {e}"))?;
    Ok(NoteScript::new(program))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let mut target: Option<String> = None;
    let mut sender = DEFAULT_SENDER.to_string();
    let mut faucet = DEFAULT_DUSDC.to_string();
    let mut user_evm = "0xf6d3C9Ed2115A5197F96f6189F6D63B51022Fe16".to_string();
    let mut basket = "DCC".to_string();
    let mut amount: u64 = 100_000;
    let mut print_root = false;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--target" => target = Some(args.next().context("--target value")?),
            "--sender" => sender = args.next().context("--sender value")?,
            "--faucet" => faucet = args.next().context("--faucet value")?,
            "--user-evm" => user_evm = args.next().context("--user-evm value")?,
            "--basket" => basket = args.next().context("--basket value")?,
            "--amount" => amount = args.next().context("--amount value")?.parse()?,
            "--print-root" => print_root = true,
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
    let faucet = AccountId::from_hex(&faucet)?;
    let basket_hex = BASKETS
        .iter()
        .find(|(sym, _)| *sym == basket)
        .with_context(|| format!("unknown basket {basket}"))?
        .1;
    let basket_faucet = AccountId::from_hex(basket_hex)?;
    let (user_suffix, user_prefix) = evm_to_user_felts(&user_evm)?;

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

    // fee_factor = nav_scale = 1 → mint_amount = deposit_value: the
    // position accumulates raw dUSDC base units, mirroring the
    // self-custody rail's accounting.
    let storage_felts = vec![
        miden_client::Felt::new(amount).expect("amount fits felt"),
        miden_client::Felt::new(1).expect("1 fits felt"),
        miden_client::Felt::new(1).expect("1 fits felt"),
        miden_client::Felt::new(user_suffix).expect("masked to 63 bits"),
        miden_client::Felt::new(user_prefix).expect("masked to 63 bits"),
        basket_faucet.suffix(),
        basket_faucet.prefix().as_felt(),
    ];

    let mut serial_bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut serial_bytes);
    let serial_num = miden_client::Word::try_from(
        serial_bytes
            .chunks_exact(8)
            .map(|chunk| {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(chunk);
                miden_client::Felt::new(u64::from_le_bytes(buf) & 0xFFFF_FFFE_FFFF_FFFF)
                    .expect("masked to Goldilocks safe range")
            })
            .collect::<Vec<_>>()
            .as_slice(),
    )?;

    let assets = NoteAssets::new(vec![Asset::Fungible(FungibleAsset::new(faucet, amount)?)])?;
    let na_target = NetworkAccountTarget::new(target, NoteExecutionHint::Always)
        .map_err(|e| anyhow::anyhow!("NetworkAccountTarget: {e:?}"))?;
    let attachments = NoteAttachments::new(vec![NoteAttachment::from(na_target)])
        .map_err(|e| anyhow::anyhow!("NoteAttachments: {e:?}"))?;
    let metadata = PartialNoteMetadata::new(sender, NoteType::Public)
        .with_tag(NoteTag::with_account_target(target));
    let recipient = NoteRecipient::new(serial_num, note_script, NoteStorage::new(storage_felts)?);
    let note = Note::with_attachments(assets, metadata, recipient, attachments);
    let note_id = note.id();

    println!("Emitting network atomic deposit note…");
    println!("    target : {} (network controller)", target.to_hex());
    println!("    deposit: {amount} dUSDC base units → basket {basket}");
    println!("    user   : {user_evm} (suffix={user_suffix} prefix={user_prefix})");
    println!("    note id: {note_id}");

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
    println!("✓ deposit note emitted");
    println!("    emit tx : {tx_id}");
    println!("    height  : {height}");
    println!();
    println!("Watch: miden-client network-note-status {note_id}");

    Ok(())
}
