// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Script, console2} from "forge-std/Script.sol";
import {DarwinRelayDeposit} from "../DarwinRelayDeposit.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {MockUSDC} from "../test/MockUSDC.sol";

/// @notice One-shot Sepolia deploy of the Darwin relay escrow.
///
/// Required env vars:
///   PRIVATE_KEY      Deployer EOA. Becomes the admin (owner) of the
///                    deployed DarwinRelayDeposit. The relay operator
///                    can be the same address or a separate one (see
///                    `RELAY_OPERATOR` below).
///   RELAY_OPERATOR   Address authorised to call claim/confirm/refund
///                    on the escrow. Optional — defaults to the
///                    deployer.
///   DEPOSIT_TOKEN    ERC20 used as the escrow currency. Optional —
///                    if unset the script deploys a MockUSDC alongside
///                    (useful on Sepolia where there's no canonical
///                    USDC).
///
/// Usage:
///   forge script contracts/script/DeployRelay.s.sol \
///     --rpc-url $SEPOLIA_RPC_URL \
///     --broadcast \
///     --verify --etherscan-api-key $ETHERSCAN_API_KEY
contract DeployRelay is Script {
    function run() external {
        uint256 deployerKey = vm.envUint("PRIVATE_KEY");
        address deployer = vm.addr(deployerKey);
        address operator = _envAddressOr("RELAY_OPERATOR", deployer);

        vm.startBroadcast(deployerKey);

        // Resolve or deploy the deposit token. Sepolia has no canonical
        // USDC, so we deploy a MockUSDC by default.
        address depositTokenAddr = _envAddressOr("DEPOSIT_TOKEN", address(0));
        if (depositTokenAddr == address(0)) {
            MockUSDC token = new MockUSDC();
            depositTokenAddr = address(token);
            console2.log("Deployed MockUSDC at", depositTokenAddr);
            // Seed the deployer with 1M MockUSDC for easy testing
            token.mint(deployer, 1_000_000 * 1e6);
            console2.log("Minted 1_000_000 MockUSDC to deployer", deployer);
        } else {
            console2.log("Using existing deposit token at", depositTokenAddr);
        }

        DarwinRelayDeposit relay = new DarwinRelayDeposit(
            IERC20(depositTokenAddr),
            operator,
            deployer
        );

        vm.stopBroadcast();

        console2.log("=== DarwinRelayDeposit deployed ===");
        console2.log("  address ", address(relay));
        console2.log("  admin   ", deployer);
        console2.log("  operator", operator);
        console2.log("  token   ", depositTokenAddr);
        console2.log("");
        console2.log("Record these in darwin-relay/state/sepolia.toml:");
        console2.log("  [relay]");
        console2.log("  address = ", address(relay));
        console2.log("  operator = ", operator);
        console2.log("  deposit_token = ", depositTokenAddr);
    }

    function _envAddressOr(string memory key, address fallback_) internal view returns (address) {
        try vm.envAddress(key) returns (address a) {
            return a;
        } catch {
            return fallback_;
        }
    }
}
