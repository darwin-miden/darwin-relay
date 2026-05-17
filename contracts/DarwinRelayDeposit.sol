// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import {Ownable} from "@openzeppelin/contracts/access/Ownable.sol";

/// @title DarwinRelayDeposit
/// @notice ETH-side escrow for the Darwin relay flow. An ETH-native user
///         locks USDC here, naming a Darwin basket and a recipient. The
///         off-chain `darwin-relay` service watches the
///         `RelayDepositRequested` event, bridges the USDC into the
///         Darwin operator's Miden account, mints the corresponding
///         private basket position on Miden, then calls back into this
///         contract via `confirmDeposit` to release the escrow and mint
///         the wrapped ERC20 to the user.
///
/// The user never touches Miden. They deposit USDC and end up holding
/// a `DarwinBasketToken` ERC20 on Ethereum.
///
/// State machine:
///
///     Requested  ─── relay claims ───►  InFlight ─── confirmDeposit ───►  Settled
///         │                                 │                                 │
///         │ user cancels before claim       │ relay refunds on bridge fail    │
///         ▼                                 ▼                                 ▼
///     Cancelled                          Refunded                          (final)
contract DarwinRelayDeposit is Ownable {
    using SafeERC20 for IERC20;

    /// Stablecoin accepted as deposit currency (USDC on the target
    /// network — Sepolia for dev, Ethereum mainnet for prod).
    IERC20 public immutable depositToken;

    /// Address authorised to claim, confirm and refund deposits. The
    /// darwin-relay service signs as this address. Distinct from the
    /// contract owner (admin) so the operator key can be rotated
    /// without changing the owner.
    address public relayOperator;

    /// Maximum time between Request and InFlight claim before the user
    /// can self-cancel and recover their deposit.
    uint64 public claimWindow;

    enum Status {
        Unknown,
        Requested,
        InFlight,
        Settled,
        Cancelled,
        Refunded
    }

    struct Deposit {
        Status status;
        address user;
        uint256 amount;
        bytes32 basketId; // keccak256(symbol), matches darwin-bridge-adapter::DarwinStrategy
        bytes32 midenRecipient; // optional: hex of a Miden address if the user has one; else zero
        uint64 requestedAt;
    }

    uint256 private _nextId;
    mapping(uint256 => Deposit) private _deposits;

    event RelayDepositRequested(
        uint256 indexed id,
        address indexed user,
        bytes32 indexed basketId,
        uint256 amount,
        bytes32 midenRecipient,
        uint64 requestedAt
    );
    event RelayDepositClaimed(uint256 indexed id, address indexed operator);
    event RelayDepositSettled(uint256 indexed id, uint256 basketAmountMinted);
    event RelayDepositCancelled(uint256 indexed id);
    event RelayDepositRefunded(uint256 indexed id, string reason);

    event RelayOperatorChanged(address indexed previous, address indexed next);
    event ClaimWindowChanged(uint64 previous, uint64 next);

    error ZeroAmount();
    error ZeroAddress();
    error NotRelayOperator();
    error NotUser();
    error BadStatus(Status expected, Status actual);
    error ClaimWindowNotElapsed();
    error UnknownDeposit(uint256 id);

    modifier onlyRelay() {
        if (msg.sender != relayOperator) revert NotRelayOperator();
        _;
    }

    constructor(IERC20 depositToken_, address relayOperator_, address admin) Ownable(admin) {
        if (address(depositToken_) == address(0)) revert ZeroAddress();
        if (relayOperator_ == address(0)) revert ZeroAddress();
        if (admin == address(0)) revert ZeroAddress();
        depositToken = depositToken_;
        relayOperator = relayOperator_;
        claimWindow = 1 hours;
        _nextId = 1;
    }

    // ----------------- user surface -----------------

    /// User locks `amount` of depositToken, naming a basket and an
    /// optional Miden recipient. Returns the deposit id (use it to
    /// poll status / cancel).
    function deposit(uint256 amount, bytes32 basketId, bytes32 midenRecipient)
        external
        returns (uint256 id)
    {
        if (amount == 0) revert ZeroAmount();
        depositToken.safeTransferFrom(msg.sender, address(this), amount);
        id = _nextId++;
        _deposits[id] = Deposit({
            status: Status.Requested,
            user: msg.sender,
            amount: amount,
            basketId: basketId,
            midenRecipient: midenRecipient,
            requestedAt: uint64(block.timestamp)
        });
        emit RelayDepositRequested(
            id, msg.sender, basketId, amount, midenRecipient, uint64(block.timestamp)
        );
    }

    /// User self-cancels a deposit that the relay hasn't claimed within
    /// `claimWindow`. Refunds the locked amount.
    function cancelDeposit(uint256 id) external {
        Deposit storage d = _deposits[id];
        if (d.status == Status.Unknown) revert UnknownDeposit(id);
        if (d.status != Status.Requested) {
            revert BadStatus(Status.Requested, d.status);
        }
        if (msg.sender != d.user) revert NotUser();
        if (uint64(block.timestamp) < d.requestedAt + claimWindow) {
            revert ClaimWindowNotElapsed();
        }
        d.status = Status.Cancelled;
        depositToken.safeTransfer(d.user, d.amount);
        emit RelayDepositCancelled(id);
    }

    // ----------------- relay operator surface -----------------

    /// Relay claims a Requested deposit, signalling it's now bridging
    /// the funds to Miden. Locks the deposit so the user can't cancel
    /// while in flight.
    function claimDeposit(uint256 id) external onlyRelay {
        Deposit storage d = _deposits[id];
        if (d.status != Status.Requested) {
            revert BadStatus(Status.Requested, d.status);
        }
        d.status = Status.InFlight;
        emit RelayDepositClaimed(id, msg.sender);
    }

    /// Relay confirms the Miden mint succeeded. The locked depositToken
    /// moves to the relay operator's treasury (it's the collateral
    /// backing the wrapped ERC20 that gets minted to the user
    /// off-contract by the relay's separate `DarwinBasketToken.mintTo`
    /// call). `basketAmountMinted` is logged for audit; the actual
    /// ERC20 mint is performed by the relay via a separate tx so we
    /// don't couple this contract to the basket-token implementation.
    function confirmDeposit(uint256 id, uint256 basketAmountMinted) external onlyRelay {
        Deposit storage d = _deposits[id];
        if (d.status != Status.InFlight) {
            revert BadStatus(Status.InFlight, d.status);
        }
        d.status = Status.Settled;
        depositToken.safeTransfer(relayOperator, d.amount);
        emit RelayDepositSettled(id, basketAmountMinted);
    }

    /// Relay refunds a deposit when the bridge/Miden side failed.
    /// Caller is responsible for ensuring no double-spend on the
    /// Miden side (e.g. by tracking bridge attestations off-chain).
    function refundDeposit(uint256 id, string calldata reason) external onlyRelay {
        Deposit storage d = _deposits[id];
        if (d.status != Status.InFlight && d.status != Status.Requested) {
            revert BadStatus(Status.InFlight, d.status);
        }
        d.status = Status.Refunded;
        depositToken.safeTransfer(d.user, d.amount);
        emit RelayDepositRefunded(id, reason);
    }

    // ----------------- admin -----------------

    function setRelayOperator(address next) external onlyOwner {
        if (next == address(0)) revert ZeroAddress();
        emit RelayOperatorChanged(relayOperator, next);
        relayOperator = next;
    }

    function setClaimWindow(uint64 next) external onlyOwner {
        emit ClaimWindowChanged(claimWindow, next);
        claimWindow = next;
    }

    // ----------------- views -----------------

    function getDeposit(uint256 id) external view returns (Deposit memory) {
        return _deposits[id];
    }

    function nextId() external view returns (uint256) {
        return _nextId;
    }
}
