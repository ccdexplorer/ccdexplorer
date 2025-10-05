# ruff: noqa: F403, F405, E402
# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalIterable=false
import pytest
import datetime as dt

from ccdexplorer.grpc_client.CCD_Types import (
    CCD_TransactionType,
    CCD_NewRelease,
    CCD_ContractAddress,
)
from ccdexplorer.grpc_client import GRPCClient

from ccdexplorer.domain.generic import NET
from ccdexplorer.cis import CIS
from rich import print
import cbor2


@pytest.fixture
def grpcclient():
    return GRPCClient()


def tx_at_index_from(
    tx_index: int, block_hash: str, grpcclient: GRPCClient, net: NET = NET.MAINNET
):
    block = grpcclient.get_block_transaction_events(block_hash, net)
    if tx_index == -1:
        return None
    else:
        return block.transaction_summaries[tx_index]


def test_tx_effect_account_transfer_1(grpcclient: GRPCClient):
    block_hash = "c3c37fca3b0990b271c419007316a4a0db33a17ddf24267067f5ab26c97bbb98"
    tx = tx_at_index_from(0, block_hash, grpcclient)

    assert tx.index == 0
    assert tx.energy_cost == 501
    assert tx.hash == "b1f887ec6fd0982d3d99e984dbb537136c7f5f244e4b451d2f239599f9af97dd"
    assert tx.type == CCD_TransactionType(
        **{"type": "account_transaction", "contents": "account_transfer"}
    )
    assert tx.account_transaction.cost == 27834
    assert tx.account_transaction.sender == "4s8JqcmEDzonyNg6h3RNGt7w2EtTw6ZD48QVjXZzsbDTQ2zpDy"
    assert tx.account_transaction.outcome == "success"
    assert tx.account_transaction.effects.account_transfer.amount == 100000000
    assert (
        tx.account_transaction.effects.account_transfer.receiver
        == "38HtRptPmxnhcsvpvmwtRKnSvEqkbfgj2itNRFMp2aihfGe2zs"
    )


def test_tx_effect_account_transfer(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(2102600)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)

    assert tx.index == 0
    assert tx.energy_cost == 501
    assert tx.account_transaction.effects.account_transfer.amount == 0


def test_tx_effect_account_transfer_with_memo(grpcclient: GRPCClient):
    block_hash = "687e1e80a32daa4c9a0f82970de0cdebd1e87184534befb7a56a9d6a18bfa5d5"
    tx = tx_at_index_from(15, block_hash, grpcclient)

    assert tx.hash == "3b9f1aa46da7e766367d5c1eb075920463bdf1b03bc20e62f586418681a32e89"
    assert (
        tx.account_transaction.effects.account_transfer.memo == "704578706c6f72657220636f6666656520"
    )


def test_tx_effect_account_transfer_with_memo_CBOR(grpcclient: GRPCClient):
    block_hash = "4d2c3362d6f67060db7e7edc0fd5ac6fc2976ba9be06d3dac1915195621b6cf4"
    tx = tx_at_index_from(0, block_hash, grpcclient)

    assert tx.hash == "f45440c584229d00b3d002180fc460934afe932f6e661511fa106e5da352edcb"
    assert (
        tx.account_transaction.effects.account_transfer.memo == "6f313934363333383534323639333231"
    )
    assert (
        cbor2.loads(bytes.fromhex(tx.account_transaction.effects.account_transfer.memo))
        == "194633854269321"
        #   194633854269321
    )


def test_tx_effect_account_transfer_with_memo_CBOR2(grpcclient: GRPCClient):
    block_hash = "502b5aba2f7d14506bfa447cc95fddf74b61c350cabe12a757b3155998ddcf92"
    tx = tx_at_index_from(0, block_hash, grpcclient)

    assert tx.hash == "c6abed84e4660090981d2f7a1c98e6c3859ba64e06cd79037f39d59f32da975a"
    assert (
        tx.account_transaction.effects.account_transfer.memo
        == "781f636f6666656520666f7220636f6e636f726469756d206578706c6f72657221"
    )
    assert (
        cbor2.loads(bytes.fromhex(tx.account_transaction.effects.account_transfer.memo))
        == "coffee for concordium explorer!"
    )


def test_tx_effect_transferred_with_schedule(grpcclient: GRPCClient):
    block_hash = "0552a84e42477639e6cf26077ec65c77c047317b7b8a5b523eacc0a2c6630bdc"
    tx = tx_at_index_from(17, block_hash, grpcclient)
    print(tx)
    assert tx.hash == "0c4d4a621ebcf7fc28e8de22b6cd6ec7fd30cf9b02ead0b932af1cab606ca4bd"
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "transferred_with_schedule"

    assert tx.account_transaction.effects.transferred_with_schedule.amount == [
        CCD_NewRelease(
            **{
                "timestamp": dt.datetime(2022, 7, 12, 21, 0, tzinfo=dt.timezone.utc),
                "amount": 676304149996,
            }
        ),
        CCD_NewRelease(
            **{
                "timestamp": dt.datetime(2022, 8, 5, 21, 0, tzinfo=dt.timezone.utc),
                "amount": 99332616666,
            }
        ),
        CCD_NewRelease(
            **{
                "timestamp": dt.datetime(2022, 9, 5, 21, 0, tzinfo=dt.timezone.utc),
                "amount": 99332616666,
            }
        ),
        CCD_NewRelease(
            **{
                "timestamp": dt.datetime(2022, 10, 5, 21, 0, tzinfo=dt.timezone.utc),
                "amount": 99332616672,
            }
        ),
    ]


def test_tx_effect_module_deployed(grpcclient: GRPCClient):
    block_hash = "3978615d4e41ff299cd0bef39aa2b0f17fa937453c2e4920e42b2b8516d51bcc"
    tx = tx_at_index_from(2, block_hash, grpcclient)

    assert tx.hash == "73e28abf31c4e1fe55761f6a7c64e599d4ffae6d669b48e79a0d992ef4e022f1"
    # assert tx.type == {"type": "account_transaction", "contents": "module_deployed"}
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "module_deployed"

    assert (
        tx.account_transaction.effects.module_deployed
        == "c08518063a847e3afe0f5bb2fda73400956fe26b5a7577e9c94f4f28a204f607"
    )


def test_tx_effect_data_registered(grpcclient: GRPCClient):
    block_hash = "3978615d4e41ff299cd0bef39aa2b0f17fa937453c2e4920e42b2b8516d51bcc"

    tx = tx_at_index_from(1, block_hash, grpcclient)

    assert tx.hash == "99650e40bcdcffa6f3b9adc5f3526a1f7e9b73e18767e8209ba559d717bfe019"
    # assert tx.type == {"type": "account_transaction", "contents": "data_registered"}
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "data_registered"

    assert (
        tx.account_transaction.effects.data_registered
        == "986a321415d93f7954bdbff5192bdac2139fe1312527dfbd594c1a6f74eb428a"
    )


def test_tx_effect_contract_initialized(grpcclient: GRPCClient):
    block_hash = "860099a7d0ae8b4a357f1877e5e13737418050b70df49191a441971f2a6cb8cc"
    tx = tx_at_index_from(3, block_hash, grpcclient)
    print(tx)
    assert tx.hash == "b6a3ed913bd76209404e9883b581223f6a788e75a6070e9b94c867ad59bf932c"
    # assert tx.type == {
    #     "type": "account_transaction",
    #     "contents": "contract_initialized",
    # }
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "contract_initialized"

    ctx = tx.account_transaction.effects.contract_initialized
    assert ctx.contract_version == 1
    assert ctx.origin_ref == "c08518063a847e3afe0f5bb2fda73400956fe26b5a7577e9c94f4f28a204f607"
    assert ctx.address.index == 7074
    assert ctx.init_name == "init_BictoryCnsPriceOracle"


def test_tx_effect_contract_update_issued(grpcclient: GRPCClient):
    """
    This transaction has 27 effects, testing only the first. Needs expansion # TODO
    """
    block_hash = "81776954af88d14c8e09efe8183795323b4a097bbdff803ba01083e831bdff7f"
    tx = tx_at_index_from(3, block_hash, grpcclient)
    print(tx)
    assert tx.hash == "13e53da58c8416651ab2f5b9773b6982dac6c2a0eed65369d6cb22db23e53254"
    assert tx.type == CCD_TransactionType(
        **{"type": "account_transaction", "contents": "contract_update_issued"}
    )
    ctx = tx.account_transaction.effects.contract_update_issued
    assert ctx.effects[0].interrupted.address == CCD_ContractAddress(
        **{"index": 7076, "subindex": 0}
    )
    print(tx.dict(exclude_none=True))


def test_tx_effect_baker_resumed(grpcclient: GRPCClient):
    block_hash = "0437b1a5c44392ebee662e4d9b245315d9f48061a19f43b8e59790717331e1e1"
    tx = tx_at_index_from(0, block_hash, grpcclient)

    assert tx.hash == "4eb360c799bde2450ba1626d56eacb5b7e0ff4fd2f54048f202264073921100d"
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "baker_configured"
    effects = tx.account_transaction.effects
    assert effects.baker_configured is not None
    assert len(effects.baker_configured.events) == 1
    assert effects.baker_configured.events[0].baker_resumed == 83075


def test_tx_effect_baker_added(grpcclient: GRPCClient):
    block_hash = "c96749a809a1e47af9098cbffb0f8f549ff172c0b3b972067ba37c5e70d56bad"
    tx = tx_at_index_from(0, block_hash, grpcclient)

    assert tx.hash == "dc2be7e800a562942c65c2ef97afee1ace38fb6313dcc4b27d304bf868a3537a"
    # assert tx.type == {"type": "account_transaction", "contents": "baker_added"}
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "baker_added"

    effects = tx.account_transaction.effects
    assert effects.baker_added.stake == 82000000000
    assert effects.baker_added.restake_earnings is True
    assert effects.baker_added.keys_event.baker_id == 72723
    assert (
        effects.baker_added.keys_event.account
        == "3BFChzvx3783jGUKgHVCanFVxyDAn5xT3Y5NL5FKydVMuBa7Bm"
    )


def test_tx_effect_baker_removed(grpcclient: GRPCClient):
    block_hash = "e233e6e4b614ca3cf6bb5daea97e3228aee23f689affb44e6a4ae3ef6adaa080"
    tx = tx_at_index_from(0, block_hash, grpcclient)

    assert tx.hash == "f4c27e62ec3e91e0e85536cf9a4b93df60ec747c01fd396d0b83fa156689f3ec"
    # assert tx.type == {"type": "account_transaction", "contents": "baker_removed"}
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "baker_removed"

    effects = tx.account_transaction.effects
    assert effects.baker_removed == 77218


def test_tx_effect_baker_configured_baker_removed(grpcclient: GRPCClient):
    block_hash = "2ce512070bebf7fe495c2607a96edd4291273698a211fabd4b2a0caf378630b0"
    tx = tx_at_index_from(10, block_hash, grpcclient)

    assert tx.hash == "f8eea94badb0ef3ddfc60aed945fc05b80297885e88d42df2b9ef0e3b04f3923"
    # assert tx.type == {"type": "account_transaction", "contents": "baker_configured"}
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "baker_configured"

    effects = tx.account_transaction.effects
    assert effects.baker_configured.events[0].baker_removed == 89075


def test_tx_effect_baker_configured_baker_set_open_status(grpcclient: GRPCClient):
    block_hash = "e83c4ce45a3c9ef42375a19bfee33ae6bf21fb04b41ba762f7865a863f692f16"
    tx = tx_at_index_from(10, block_hash, grpcclient)

    assert tx.hash == "02a70a89b1d4c358b321df8a6d80cd9beffdfbd9eb5b2f5c5fa73a324142ad11"
    # assert tx.type == {"type": "account_transaction", "contents": "baker_configured"}
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "baker_configured"

    effects = tx.account_transaction.effects
    assert effects.baker_configured.events[0].baker_set_open_status.baker_id == 89075
    assert effects.baker_configured.events[0].baker_set_open_status.open_status == 0


def test_tx_effect_baker_configured_baker_empty_url(grpcclient: GRPCClient):
    block_hash = "c20bf8dbfff786fda6a6a03e626918a3ec7df1c54ef9b4b926e513ca8fda18c9"
    tx = tx_at_index_from(5, block_hash, grpcclient)

    assert tx.hash == "d154e56ba01118fe315b8a86108f3d1bfcb143da5b32d6532bea03691f6d76a1"
    # assert tx.type == {"type": "account_transaction", "contents": "baker_configured"}
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "baker_configured"

    effects = tx.account_transaction.effects
    assert effects.baker_configured.events[3].baker_set_metadata_url.url == ""


def test_tx_effect_baker_configured_stake_increased(grpcclient: GRPCClient):
    block_hash = "977e8f7339036362375364cbb6616a74f7b22f6832283dc2584951b42f9b7019"
    tx = tx_at_index_from(10, block_hash, grpcclient)

    assert tx.hash == "1cc0f65d6b44af5498cebe3fa5452a7616b8f1662ff4dee3f2a0a000267be5fd"
    # assert tx.type == {"type": "account_transaction", "contents": "baker_configured"}
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "baker_configured"

    effects = tx.account_transaction.effects
    assert effects.baker_configured.events[0].baker_stake_increased.baker_id == 72723
    assert effects.baker_configured.events[0].baker_stake_increased.new_stake == 1089780959436


def test_tx_effect_baker_configured_stake_decreased(grpcclient: GRPCClient):
    block_hash = "dc2feebd4d51ed7f3535237753f0907f32ad953d022f10d4b117420275f2e31c"
    tx = tx_at_index_from(12, block_hash, grpcclient)

    assert tx.hash == "3105aebfc92e6cb3b4cc9197046ba7e88b69f4eeb065d8ff5debe1de2f299f3e"
    # assert tx.type == {"type": "account_transaction", "contents": "baker_configured"}
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "baker_configured"

    effects = tx.account_transaction.effects
    assert effects.baker_configured.events[0].baker_stake_decreased.baker_id == 76670
    assert effects.baker_configured.events[0].baker_stake_decreased.new_stake == 195000000000000


def test_tx_effect_baker_configured_set_url(grpcclient: GRPCClient):
    block_hash = "94dc7ea9419569e16e633649c0cf27134295068ba969be56222a989835aded08"
    tx = tx_at_index_from(0, block_hash, grpcclient)

    assert tx.hash == "ef6d10fb47dee5941ae515ae1819c5e525970d92c1e1946d8832e8baa62cf412"
    # assert tx.type == {"type": "account_transaction", "contents": "baker_configured"}
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "baker_configured"

    effects = tx.account_transaction.effects
    assert effects.baker_configured.events[0].baker_set_metadata_url.baker_id == 72723
    assert (
        effects.baker_configured.events[0].baker_set_metadata_url.url
        == "https://concordium-explorer.nl/delegate-to-72723-for-free-access/"
    )


def test_tx_effect_delegation_configured_strange(grpcclient: GRPCClient):
    block_hash = "a6aace9b20ff3e6fcdbf5ae9a1211c57ae45a3944f2245ed4ab79ab03582c4fc"
    tx = tx_at_index_from(11, block_hash, grpcclient)

    print(tx)


def test_tx_effect_delegation_removed_empty(grpcclient: GRPCClient):
    block_hash = "c3c02ae8eeb4d75115e3bcd029e745684c43f261f969d854ee6adb71fb31c1bf"
    tx = tx_at_index_from(9, block_hash, grpcclient)

    print(tx)


def test_tx_effect_delegation_configured_staked_increased(grpcclient: GRPCClient):
    block_hash = "e0b0fe488659e1acc7dafb40b5df096eba9ff7821318d8563441d19a00faf9d1"
    tx = tx_at_index_from(6, block_hash, grpcclient)

    assert tx.hash == "86d93bda166fef2a0dce4f1da5bf176fb100bd94def1441e09c153eb52b79675"
    # assert tx.type == {
    #     "type": "account_transaction",
    #     "contents": "delegation_configured",
    # }
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "delegation_configured"

    effects = tx.account_transaction.effects
    assert (
        effects.delegation_configured.events[0].delegation_set_delegation_target.delegator_id
        == 72723
    )
    assert (
        effects.delegation_configured.events[
            0
        ].delegation_set_delegation_target.delegation_target.baker
        == 76670
    )
    assert effects.delegation_configured.events[1].delegation_stake_increased.delegator_id == 72723
    assert (
        effects.delegation_configured.events[1].delegation_stake_increased.new_stake == 48400000000
    )


def test_tx_effect_delegation_configured_new_delegator(grpcclient: GRPCClient):
    block_hash = "db0dee12df918eb700d082495cfd1cbeb75a60b76cb515493676abfb41d5f78a"
    tx = tx_at_index_from(5, block_hash, grpcclient)

    assert tx.hash == "93ef077f492aa177d75872eba6d6180a8fcec75535baa83d8d8c4173f1b8e254"
    # assert tx.type == {
    #     "type": "account_transaction",
    #     "contents": "delegation_configured",
    # }
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "delegation_configured"

    effects = tx.account_transaction.effects
    assert (
        effects.delegation_configured.events[1].delegation_set_delegation_target.delegator_id
        == 86087
    )
    assert (
        effects.delegation_configured.events[
            1
        ].delegation_set_delegation_target.delegation_target.baker
        == 72723
    )
    assert (
        effects.delegation_configured.events[2].delegation_set_restake_earnings.delegator_id
        == 86087
    )
    assert (
        effects.delegation_configured.events[2].delegation_set_restake_earnings.restake_earnings
        is True
    )
    assert effects.delegation_configured.events[3].delegation_stake_increased.delegator_id == 86087
    assert (
        effects.delegation_configured.events[3].delegation_stake_increased.new_stake == 468100000000
    )


def test_tx_effect_updateCredentials(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(239630)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    # assert tx.type == {"type": "account_transaction", "contents": "credentials_updated"}
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "credentials_updated"

    assert tx.account_transaction.effects.credentials_updated.new_threshold == 3
    assert (
        tx.account_transaction.effects.credentials_updated.new_cred_ids[0]
        == "83e2c387d081c7042f766d2b08c25c41802bbac91f9ddae98f27e6f324b9955a6f04e59d69770a777bdb64c75982fa4a"
    )


def test_tx_effect_transferToEncrypted(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(1424606)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    # assert tx.type == {
    #     "type": "account_transaction",
    #     "contents": "transferred_to_encrypted",
    # }
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "transferred_to_encrypted"

    assert (
        tx.account_transaction.effects.transferred_to_encrypted.account
        == "37MzsaqfLYDx8ZR2fnuwFETfVJayYH7NkVDwVf899BLP3nYh4C"
    )
    assert tx.account_transaction.effects.transferred_to_encrypted.amount == 50000000


def test_tx_effect_transferToPublic(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(481)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    # assert tx.type == {
    #     "type": "account_transaction",
    #     "contents": "transferred_to_public",
    # }
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "transferred_to_public"

    assert tx.account_transaction.effects.transferred_to_public.amount == 2000000
    assert (
        tx.account_transaction.effects.transferred_to_public.removed.account
        == "42Hc6eq8GaKpz7w9B7tGD1mNWLjbYFq3iSyK5NjNPNif7fgNVS"
    )
    assert (
        tx.account_transaction.effects.transferred_to_public.removed.new_amount
        == "a643d99bcb44ec1646ee21bb4b61d9cf972ef82298190a4ab4da589159970c3b83baceb4439aa166b7448ccc8cac5fc3af97818769c689166727e69f68df3b5fc86464e47acb6cd23f80f00f5bf4757822b7e4b1bdb3c2db4e8ab3bf53c8770fb4df09d24aef342dec1940804e69e3f15dc5d329907cde8e3694067f700babc29213f56dd5ec93ed4d16edec516054018d7f72e8fa382524ede966cb3ae5cda5f0ba261443decaa81decbbe976a14716a448db901fd69d4b915d8e3dcd79b1a5"
    )


def test_tx_effect_encryptedAmountTransfer(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(432)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    # assert tx.type == {
    #     "type": "account_transaction",
    #     "contents": "encrypted_amount_transferred",
    # }
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "encrypted_amount_transferred"

    assert tx.account_transaction.effects.encrypted_amount_transferred.memo == ""
    assert (
        tx.account_transaction.effects.encrypted_amount_transferred.removed.account
        == "2xKDpRzaqUeSEeo3Bcaq7HRKmRaKRoz2KSY7DSsp9qGZdKfiLM"
    )


def test_tx_effect_encryptedAmountTransferWithMemo(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(2093818)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    # assert tx.type == {
    #     "type": "account_transaction",
    #     "contents": "encrypted_amount_transferred",
    # }
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "encrypted_amount_transferred"

    assert tx.account_transaction.effects.encrypted_amount_transferred.memo == "626363"
    assert (
        tx.account_transaction.effects.encrypted_amount_transferred.removed.account
        == "3dzi5ZYZns6Ub2VbAue6KZkPM9C1RqbYERmmBN75ju4MhkZctq"
    )


def test_tx_effect_updateBakerRestakeEarnings(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(1860034)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    # assert tx.type == {
    #     "type": "account_transaction",
    #     "contents": "baker_restake_earnings_updated",
    # }
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "baker_restake_earnings_updated"

    assert tx.account_transaction.effects.baker_restake_earnings_updated.baker_id == 1051
    assert tx.account_transaction.effects.baker_restake_earnings_updated.restake_earnings is True


def test_tx_effect_updateBakerKeys(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(1849412)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    # assert tx.type == {"type": "account_transaction", "contents": "baker_keys_updated"}
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "baker_keys_updated"

    assert (
        tx.account_transaction.effects.baker_keys_updated.account
        == "3ctQKM4tB5YQV4RS7ojjLfnLRDT37NNs7eAfA6QG4J8sqNdKhj"
    )
    assert tx.account_transaction.effects.baker_keys_updated.baker_id == 700
    assert (
        tx.account_transaction.effects.baker_keys_updated.sign_key
        == "27c45d84ad4793c4ea68e91f61b7ea01161f3f86e4b71e0d4fcd0d4a1ebec258"
    )


def test_tx_effect_updateBakerStake(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(1686394)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    # assert tx.type == {"type": "account_transaction", "contents": "baker_stake_updated"}
    assert tx.type.type == "account_transaction"
    assert tx.type.contents == "baker_stake_updated"

    assert tx.account_transaction.effects.baker_stake_updated.update.baker_id == 44954
    assert tx.account_transaction.effects.baker_stake_updated.update.new_stake == 22000000000
    assert tx.account_transaction.effects.baker_stake_updated.update.increased is True


def test_tx_effect_update_issued(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(1976086)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "success"


def test_tx_effect_update_level1update(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(3217753)[0]
    tx = tx_at_index_from(6, block_hash, grpcclient)
    print(tx)
    assert tx.update is not None
    assert tx.update.effective_time == 1655828100
    assert (
        tx.update.payload.level_1_update.level_1_keys_update.keys[0]
        == "12f6025af2762f500162d9f6b5558b698bae0c126725d499262d41aabe7ec0b3"
    )
    assert tx.update.payload.level_1_update.level_1_keys_update.threshold == 7


def test_tx_effect_baker_configured_empty(grpcclient: GRPCClient):
    block_hash = grpcclient.get_finalized_block_at_height(5284048).hash
    tx = tx_at_index_from(5, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.effects.baker_configured.events[0].baker_removed == 0


def test_tx_effect_cis_mint(grpcclient: GRPCClient):
    block_hash = grpcclient.get_finalized_block_at_height(2700369, NET.TESTNET).hash
    tx = tx_at_index_from(0, block_hash, grpcclient, NET.TESTNET)
    # print(tx)
    ci = CIS(grpcclient, 0, 0, "", NET.TESTNET)
    for effect in tx.account_transaction.effects.contract_update_issued.effects:
        if effect:
            if effect.updated:
                for event in effect.updated.events:
                    result = ci.process_log_events(event)
                    print(result)

    # assert tx.account_transaction.effects.baker_configured.events[0].baker_removed == 0


def test_tx_effect_contract_upgraded(grpcclient: GRPCClient):
    block_hash = grpcclient.get_finalized_block_at_height(5949978, NET.MAINNET).hash
    tx = tx_at_index_from(6, block_hash, grpcclient, NET.MAINNET)
    print(tx)
    # for effect in tx.account_transaction.effects.contract_update_issued.effects:
    #     if effect:
    #         if effect.updated:
    #             for event in effect.updated.events:
    #                 result = ci.process_log_events(event)
    #                 print(result)


def test_tx_effect_cis_mint_16325(grpcclient: GRPCClient):
    block_hash = grpcclient.get_finalized_block_at_height(16325, NET.TESTNET).hash
    tx = tx_at_index_from(0, block_hash, grpcclient, NET.TESTNET)
    # print(tx)
    ci = CIS(grpcclient, 0, 0, "", NET.TESTNET)

    for event in tx.account_transaction.effects.contract_initialized.events:
        result = ci.process_log_events(event)
        print(result)


def test_tx_effect_cis_mint_7843(grpcclient: GRPCClient):
    block_hash = grpcclient.get_finalized_block_at_height(10003559, NET.TESTNET).hash
    tx = tx_at_index_from(0, block_hash, grpcclient, NET.TESTNET)
    # print(tx)
    ci = CIS(grpcclient, 7843, 0, "", NET.TESTNET)

    for effect in tx.account_transaction.effects.contract_update_issued.effects:
        if effect.updated:
            for event in effect.updated.events:
                result = ci.process_log_events(event)
                print(result)


def test_tx_effect_baker_set_baking_reward(grpcclient: GRPCClient):
    block_hash = grpcclient.get_finalized_block_at_height(7131699, net=NET.TESTNET).hash
    tx = tx_at_index_from(0, block_hash, grpcclient, net=NET.TESTNET)
    print(tx)
    assert (
        tx.account_transaction.effects.baker_configured.events[
            4
        ].baker_set_transaction_fee_commission.transaction_fee_commission
        == 0.54504
    )
    assert (
        tx.account_transaction.effects.baker_configured.events[
            5
        ].baker_set_baking_reward_commission.baking_reward_commission
        == 0.09975
    )
    assert (
        tx.account_transaction.effects.baker_configured.events[
            6
        ].baker_set_finalization_reward_commission.finalization_reward_commission
        == 1
    )


def test_tx_update_fin_parameters(grpcclient: GRPCClient):
    block_hash = grpcclient.get_finalized_block_at_height(9739535).hash
    tx = tx_at_index_from(0, block_hash, grpcclient)
    print(tx)
    # assert (
    #     tx.account_transaction.effects.baker_configured.events[
    #         4
    #     ].baker_set_transaction_fee_commission.transaction_fee_commission
    #     == 0.54504
    # )
    # assert (
    #     tx.account_transaction.effects.baker_configured.events[
    #         5
    #     ].baker_set_baking_reward_commission.baking_reward_commission
    #     == 0.09975
    # )
    # assert (
    #     tx.account_transaction.effects.baker_configured.events[
    #         6
    #     ].baker_set_finalization_reward_commission.finalization_reward_commission
    #     == 1
    # )


def test_tx_instance_migration(grpcclient: GRPCClient):
    block_hash = grpcclient.get_finalized_block_at_height(16094108, NET.MAINNET).hash
    tx = tx_at_index_from(6, block_hash, grpcclient, NET.MAINNET)
    # print(tx)
    assert (
        tx.account_transaction.effects.contract_update_issued.effects[2].upgraded.address.index
        == 9459
    )
    assert (
        tx.account_transaction.effects.contract_update_issued.effects[2].upgraded.from_module
        == "efc4ea2b19330518131b67b0b66ac6f628438bfd740d694fcf64a13412bf327b"
    )
    assert (
        tx.account_transaction.effects.contract_update_issued.effects[2].upgraded.to_module
        == "fc9e9fdb728c4657891390f58dcb91070f10b93d6b9928cae7cb18823466cd44"
    )


def test_tx_testnet_delegation_configured_p7(grpcclient: GRPCClient):
    block_hash = grpcclient.get_finalized_block_at_height(20325075, NET.TESTNET).hash
    tx = tx_at_index_from(0, block_hash, grpcclient, NET.TESTNET)
    print(tx)


def test_tx_testnet_delegation_configured_p6(grpcclient: GRPCClient):
    block_hash = grpcclient.get_finalized_block_at_height(17864381, NET.TESTNET).hash
    tx = tx_at_index_from(0, block_hash, grpcclient, NET.TESTNET)
    print(tx)


def test_tx_mainnet_delegation_configured(grpcclient: GRPCClient):
    block_hash = grpcclient.get_finalized_block_at_height(4736706, NET.MAINNET).hash
    tx = tx_at_index_from(5, block_hash, grpcclient, NET.MAINNET)
    print(tx)
