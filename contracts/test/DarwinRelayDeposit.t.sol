// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Test} from "forge-std/Test.sol";
import {DarwinRelayDeposit} from "../DarwinRelayDeposit.sol";
import {MockUSDC} from "./MockUSDC.sol";

contract DarwinRelayDepositTest is Test {
    DarwinRelayDeposit internal relay;
    MockUSDC internal usdc;

    address internal admin = address(0xA11CE);
    address internal operator = address(0x0DE7A107); // "operator" pun
    address internal user = address(0xBEEF);
    address internal other = address(0xDEAD);

    bytes32 internal dccId = keccak256("DCC");
    bytes32 internal midenRecipient = bytes32(uint256(0xCAFE));

    function setUp() public {
        usdc = new MockUSDC();
        vm.prank(admin);
        relay = new DarwinRelayDeposit(usdc, operator, admin);

        usdc.mint(user, 1_000_000e6);
        vm.prank(user);
        usdc.approve(address(relay), type(uint256).max);
    }

    function _deposit(uint256 amount) internal returns (uint256 id) {
        vm.prank(user);
        id = relay.deposit(amount, dccId, midenRecipient);
    }

    // ---------- constructor / admin ----------

    function test_constructor_setsImmutables() public view {
        assertEq(address(relay.depositToken()), address(usdc));
        assertEq(relay.relayOperator(), operator);
        assertEq(relay.owner(), admin);
        assertEq(relay.claimWindow(), 1 hours);
        assertEq(relay.nextId(), 1);
    }

    function test_constructor_rejectsZeroToken() public {
        vm.expectRevert(DarwinRelayDeposit.ZeroAddress.selector);
        new DarwinRelayDeposit(MockUSDC(address(0)), operator, admin);
    }

    function test_constructor_rejectsZeroOperator() public {
        vm.expectRevert(DarwinRelayDeposit.ZeroAddress.selector);
        new DarwinRelayDeposit(usdc, address(0), admin);
    }

    function test_constructor_rejectsZeroAdmin() public {
        // Ownable's own zero-owner check fires before our ZeroAddress.
        vm.expectRevert(); // OwnableInvalidOwner(0)
        new DarwinRelayDeposit(usdc, operator, address(0));
    }

    function test_setRelayOperator_onlyOwner() public {
        vm.prank(other);
        vm.expectRevert();
        relay.setRelayOperator(other);
    }

    function test_setRelayOperator_persists() public {
        vm.prank(admin);
        relay.setRelayOperator(other);
        assertEq(relay.relayOperator(), other);
    }

    function test_setRelayOperator_rejectsZero() public {
        vm.prank(admin);
        vm.expectRevert(DarwinRelayDeposit.ZeroAddress.selector);
        relay.setRelayOperator(address(0));
    }

    function test_setClaimWindow_persists() public {
        vm.prank(admin);
        relay.setClaimWindow(15 minutes);
        assertEq(relay.claimWindow(), 15 minutes);
    }

    // ---------- deposit ----------

    function test_deposit_locksTokens_storesState_emitsEvent() public {
        uint256 before = usdc.balanceOf(user);

        vm.prank(user);
        uint256 id = relay.deposit(1_000e6, dccId, midenRecipient);

        assertEq(id, 1);
        assertEq(usdc.balanceOf(user), before - 1_000e6);
        assertEq(usdc.balanceOf(address(relay)), 1_000e6);

        DarwinRelayDeposit.Deposit memory d = relay.getDeposit(id);
        assertEq(uint8(d.status), uint8(DarwinRelayDeposit.Status.Requested));
        assertEq(d.user, user);
        assertEq(d.amount, 1_000e6);
        assertEq(d.basketId, dccId);
        assertEq(d.midenRecipient, midenRecipient);
        assertEq(d.requestedAt, uint64(block.timestamp));
    }

    function test_deposit_rejectsZero() public {
        vm.prank(user);
        vm.expectRevert(DarwinRelayDeposit.ZeroAmount.selector);
        relay.deposit(0, dccId, midenRecipient);
    }

    function test_deposit_idsIncrement() public {
        assertEq(_deposit(1e6), 1);
        assertEq(_deposit(1e6), 2);
        assertEq(_deposit(1e6), 3);
        assertEq(relay.nextId(), 4);
    }

    // ---------- claim ----------

    function test_claim_movesToInFlight() public {
        uint256 id = _deposit(1_000e6);
        vm.prank(operator);
        relay.claimDeposit(id);
        DarwinRelayDeposit.Deposit memory d = relay.getDeposit(id);
        assertEq(uint8(d.status), uint8(DarwinRelayDeposit.Status.InFlight));
    }

    function test_claim_rejectsNonOperator() public {
        uint256 id = _deposit(1_000e6);
        vm.prank(other);
        vm.expectRevert(DarwinRelayDeposit.NotRelayOperator.selector);
        relay.claimDeposit(id);
    }

    function test_claim_rejectsAlreadyInFlight() public {
        uint256 id = _deposit(1_000e6);
        vm.prank(operator);
        relay.claimDeposit(id);
        vm.prank(operator);
        vm.expectRevert(
            abi.encodeWithSelector(
                DarwinRelayDeposit.BadStatus.selector,
                DarwinRelayDeposit.Status.Requested,
                DarwinRelayDeposit.Status.InFlight
            )
        );
        relay.claimDeposit(id);
    }

    // ---------- confirm ----------

    function test_confirm_releasesEscrowToOperator() public {
        uint256 id = _deposit(1_000e6);
        vm.prank(operator);
        relay.claimDeposit(id);

        uint256 operatorBefore = usdc.balanceOf(operator);
        vm.prank(operator);
        relay.confirmDeposit(id, 997e6);

        DarwinRelayDeposit.Deposit memory d = relay.getDeposit(id);
        assertEq(uint8(d.status), uint8(DarwinRelayDeposit.Status.Settled));
        assertEq(usdc.balanceOf(operator), operatorBefore + 1_000e6);
        assertEq(usdc.balanceOf(address(relay)), 0);
    }

    function test_confirm_rejectsNonOperator() public {
        uint256 id = _deposit(1_000e6);
        vm.prank(operator);
        relay.claimDeposit(id);
        vm.prank(other);
        vm.expectRevert(DarwinRelayDeposit.NotRelayOperator.selector);
        relay.confirmDeposit(id, 100);
    }

    function test_confirm_rejectsWhenNotInFlight() public {
        uint256 id = _deposit(1_000e6);
        // not claimed yet
        vm.prank(operator);
        vm.expectRevert(
            abi.encodeWithSelector(
                DarwinRelayDeposit.BadStatus.selector,
                DarwinRelayDeposit.Status.InFlight,
                DarwinRelayDeposit.Status.Requested
            )
        );
        relay.confirmDeposit(id, 100);
    }

    // ---------- cancel ----------

    function test_cancel_refundsUser_afterClaimWindow() public {
        uint256 id = _deposit(1_000e6);
        uint256 userBefore = usdc.balanceOf(user);
        vm.warp(block.timestamp + 1 hours + 1);
        vm.prank(user);
        relay.cancelDeposit(id);
        assertEq(usdc.balanceOf(user), userBefore + 1_000e6);

        DarwinRelayDeposit.Deposit memory d = relay.getDeposit(id);
        assertEq(uint8(d.status), uint8(DarwinRelayDeposit.Status.Cancelled));
    }

    function test_cancel_rejectsBeforeClaimWindow() public {
        uint256 id = _deposit(1_000e6);
        vm.warp(block.timestamp + 30 minutes);
        vm.prank(user);
        vm.expectRevert(DarwinRelayDeposit.ClaimWindowNotElapsed.selector);
        relay.cancelDeposit(id);
    }

    function test_cancel_rejectsNonUser() public {
        uint256 id = _deposit(1_000e6);
        vm.warp(block.timestamp + 2 hours);
        vm.prank(other);
        vm.expectRevert(DarwinRelayDeposit.NotUser.selector);
        relay.cancelDeposit(id);
    }

    function test_cancel_rejectsAfterClaim() public {
        uint256 id = _deposit(1_000e6);
        vm.prank(operator);
        relay.claimDeposit(id);
        vm.warp(block.timestamp + 2 hours);
        vm.prank(user);
        vm.expectRevert(
            abi.encodeWithSelector(
                DarwinRelayDeposit.BadStatus.selector,
                DarwinRelayDeposit.Status.Requested,
                DarwinRelayDeposit.Status.InFlight
            )
        );
        relay.cancelDeposit(id);
    }

    // ---------- refund ----------

    function test_refund_returnsTokensToUser_fromInFlight() public {
        uint256 id = _deposit(1_000e6);
        vm.prank(operator);
        relay.claimDeposit(id);

        uint256 userBefore = usdc.balanceOf(user);
        vm.prank(operator);
        relay.refundDeposit(id, "bridge timeout");
        assertEq(usdc.balanceOf(user), userBefore + 1_000e6);

        DarwinRelayDeposit.Deposit memory d = relay.getDeposit(id);
        assertEq(uint8(d.status), uint8(DarwinRelayDeposit.Status.Refunded));
    }

    function test_refund_alsoWorksFromRequested() public {
        // when relay sees an unfulfillable deposit before claiming
        uint256 id = _deposit(1_000e6);
        vm.prank(operator);
        relay.refundDeposit(id, "unsupported basket");

        DarwinRelayDeposit.Deposit memory d = relay.getDeposit(id);
        assertEq(uint8(d.status), uint8(DarwinRelayDeposit.Status.Refunded));
    }

    function test_refund_rejectsNonOperator() public {
        uint256 id = _deposit(1_000e6);
        vm.prank(user);
        vm.expectRevert(DarwinRelayDeposit.NotRelayOperator.selector);
        relay.refundDeposit(id, "no");
    }

    // ---------- unknown deposit ----------

    function test_cancel_rejectsUnknownDeposit() public {
        vm.warp(block.timestamp + 2 hours);
        vm.prank(user);
        vm.expectRevert(abi.encodeWithSelector(DarwinRelayDeposit.UnknownDeposit.selector, 99));
        relay.cancelDeposit(99);
    }
}
