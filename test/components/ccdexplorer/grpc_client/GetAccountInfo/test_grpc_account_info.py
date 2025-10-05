# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportArgumentType=false

import datetime as dt

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import CoolDownStatus
from rich import print


def test_account_info_plt(grpcclient: GRPCClient):
    block_hash = "3b92fa7257d2d7d252586fa3e7ad68af8c9bcaf4660cff660247023480bafe43"
    # block_hash = "last_final"
    account = "3kBVb3GAVg3A5M3E2cExk3UkjEPwKZokNaSyoj1LKXvtzekRdS"
    ai = grpcclient.get_account_info(block_hash, account)
    print(ai)
    assert ai.tokens is not None
    assert len(ai.tokens) == 2
    assert ai.tokens[0].token_id == "EURR"
    assert ai.tokens[0].token_account_state.balance.value == "10000000"
    assert ai.tokens[0].token_account_state.balance.decimals == 6
    assert ai.tokens[0].token_account_state.module_state is not None
    assert ai.tokens[0].token_account_state.module_state.allow_list is None


def test_tx_account_info_schedule(grpcclient: GRPCClient):
    account = "4qL7HmpqdKpYufCV7EvL1WnHVowy1CrTQS7jfYgef5stzpftwS"
    block_hash = "9c89b7b3cd65d3f5a96b1b9112c66530a0cea4dc831ba9b6c9dfddbeafbd8813"
    ai = grpcclient.get_account_info(block_hash, account)
    print(ai)
    assert ai.address == account
    assert ai.schedule.schedules[0].amount == 16358781149999
    assert ai.schedule.schedules[0].timestamp == dt.datetime(
        2023, 2, 5, 21, 0, tzinfo=dt.timezone.utc
    )
    assert (
        ai.schedule.schedules[0].transactions[0]
        == "f33050060051c6b738549b60e99498fc7f59fb0b6c915ed9e90a11a7584f2d30"
    )


def test_tx_account_info_baker(grpcclient: GRPCClient):
    account = "3BFChzvx3783jGUKgHVCanFVxyDAn5xT3Y5NL5FKydVMuBa7Bm"
    account = "2xxQruBfpdRbHAzU3TdH2sHtthdbXdp1yMHS1ws88FgEX9KwHr"
    block_hash = "9c89b7b3cd65d3f5a96b1b9112c66530a0cea4dc831ba9b6c9dfddbeafbd8813"
    block_hash = "last_final"
    ai = grpcclient.get_account_info(block_hash, account)
    print(ai.model_dump(exclude_none=True))
    assert ai.address == account


def test_tx_account_info_encrypted_transfer(grpcclient: GRPCClient):
    account = "2xKDpRzaqUeSEeo3Bcaq7HRKmRaKRoz2KSY7DSsp9qGZdKfiLM"
    block_hash = "9c89b7b3cd65d3f5a96b1b9112c66530a0cea4dc831ba9b6c9dfddbeafbd8813"
    ai = grpcclient.get_account_info(block_hash, account)
    print(ai.dict(exclude_none=True))
    assert ai.address == account
    assert (
        ai.encryption_key
        == "b14cbfe44a02c6b1f78711176d5f437295367aa4f2a8c2551ee10d25a03adc69d61a332a058971919dad7312e1fc94c5a5a75e23c8e822586afeeeff2a3b8ddd4ff29acfb423dd7b23628304eca9827987cef2004a94c73db4abffa8aaadb0b3"
    )
    assert (
        ai.encrypted_balance.self_amount
        == "915cb47e8df909bc873ecd1368d89762830f2785102cadc8c7c2c7b2b1975c089e743f0bf6187366bc822acc1f971f5aadbf60a8927d5679d8b59823a331db3e79c38be22c1a787d2659da1db471ff20dbe54a4a012c2b3a4a911e3f8ebf995188612b72f9881bd46b96ab68a1d65a366c887b3c883d6d46c3563d575d10a68be6077f1d8111c240b3392e22c230ffda96ae65016fc632721296d6b33fe07d3fbb0eba6111ad2162a2fd0cb7c32388e938414db50a1366e8fe176905d2c394a1"
    )


def test_tx_account_info_1(grpcclient: GRPCClient):
    account = "3rfJwWcM5AcdpchhXtQ1Zo5aVFjjPpM1DqNT2ucJRLZgu95R7s"
    block_hash = "9c89b7b3cd65d3f5a96b1b9112c66530a0cea4dc831ba9b6c9dfddbeafbd8813"
    ai = grpcclient.get_account_info(block_hash, account)
    print(ai.model_dump(exclude_none=True))
    assert ai.address == account


def test_tx_account_info_revealed_attributes(grpcclient: GRPCClient):
    account = "3X6GrcepiHLbXttZLG2jk6sVY3ybu5vMiDASLy5DoVJMCrKrx9"
    block_hash = "9c89b7b3cd65d3f5a96b1b9112c66530a0cea4dc831ba9b6c9dfddbeafbd8813"
    ai = grpcclient.get_account_info(block_hash, account)
    print(ai)
    print(ai.model_dump(exclude_none=True))
    assert ai.address == account


def test_tx_account_info_2(grpcclient: GRPCClient):
    baker_id = 72723
    block_hash = "9c89b7b3cd65d3f5a96b1b9112c66530a0cea4dc831ba9b6c9dfddbeafbd8813"
    ai = grpcclient.get_account_info(block_hash, account_index=baker_id)
    print(ai.model_dump(exclude_none=True))
    assert ai.address == "3BFChzvx3783jGUKgHVCanFVxyDAn5xT3Y5NL5FKydVMuBa7Bm"

    pass


def test_tx_account_info_payday(grpcclient: GRPCClient):
    baker_id = 84252
    block_hash = "d2735387725c426c85e5242177f80b12a44c1ef6aaa8cc7453a7ed83f912dc10"
    ai = grpcclient.get_account_info(block_hash, account_index=baker_id)
    print(ai.model_dump(exclude_none=True))
    assert ai.address == "3FYJZJkxgMtby5tqkwhq8dKWztLkAJYdVKKR3EbgNkNPbuN9Bi"

    pass


def test_tx_account_info_higher_threshold(grpcclient: GRPCClient):
    account = "4CqVcmNi9F5V53YdJZ9U5sLaqaWt7Uxrf8VYk5WCLDYwbLL62Y"
    block_hash = "d2735387725c426c85e5242177f80b12a44c1ef6aaa8cc7453a7ed83f912dc10"
    ai = grpcclient.get_account_info(block_hash, hex_address=account)
    print(ai.model_dump(exclude_none=True))
    assert ai.address == "4CqVcmNi9F5V53YdJZ9U5sLaqaWt7Uxrf8VYk5WCLDYwbLL62Y"

    pass


def test_tx_account_info_initial(grpcclient: GRPCClient):
    account = "2wkEiDDDhWLset4GT4TPVutLArbvKfTsP7VYhhA776aTmvHcNB"
    block_hash = "last_final"
    ai = grpcclient.get_account_info(block_hash, hex_address=account)
    print(ai.credentials)
    assert ai.address == "2wkEiDDDhWLset4GT4TPVutLArbvKfTsP7VYhhA776aTmvHcNB"

    pass


def test_tx_account_info_normal(grpcclient: GRPCClient):
    account = "4D44RYigFqPkABrRAHXSBBQqG4VNhXEsyJrt2GH6V2H8tS1tN3"
    block_hash = "last_final"
    ai = grpcclient.get_account_info(block_hash, hex_address=account)
    print(ai.credentials)
    assert ai.address == "4D44RYigFqPkABrRAHXSBBQqG4VNhXEsyJrt2GH6V2H8tS1tN3"


def test_tx_account_info_weird(grpcclient: GRPCClient):
    account = "2wkEiDDDhWLset4GT4TPVutLArbvKfTsP7VYhhA776aTmvHcNB"
    block_hash = "last_final"
    ai = grpcclient.get_account_info(block_hash, hex_address=account)
    print(ai)
    assert ai.address == "2wkEiDDDhWLset4GT4TPVutLArbvKfTsP7VYhhA776aTmvHcNB"


def test_tx_account_info_credential_lei(grpcclient: GRPCClient):
    account = "38BkoGvD3EPiChHjWJ55Do6zSn8QugJFhGyhdB9ybowSqPi8Ta"
    block_hash = "last_final"
    ai = grpcclient.get_account_info(block_hash, hex_address=account)
    print(ai)
    assert ai.address == "38BkoGvD3EPiChHjWJ55Do6zSn8QugJFhGyhdB9ybowSqPi8Ta"


def test_tx_account_info_corporate_account(grpcclient: GRPCClient):
    account = "36bpBdY5PPqSMTBSSsbey6Xb9GpabrU86XtpyE5y8n8p8NzW8Z"
    block_hash = "last_final"
    ai = grpcclient.get_account_info(block_hash, hex_address=account, net=NET.TESTNET)
    print(ai)
    # assert ai.address == "38BkoGvD3EPiChHjWJ55Do6zSn8QugJFhGyhdB9ybowSqPi8Ta"


def test_tx_account_info_cooldown_P7(grpcclient: GRPCClient):
    account = "4Z2sLB2cfHV1H2VGVEMJ7t6ZLKqiXVE1CF8fua1f89fi3bc1nx"
    block_hash = "32f62592688b9c9b56de591d721f656e86450371453036d8b22cc1085ca85ccd"
    ai = grpcclient.get_account_info(block_hash, hex_address=account, net=NET.TESTNET)
    print(ai)
    assert ai.cooldowns[0].status == CoolDownStatus.PRE_PRE_COOLDOWN
    assert ai.cooldowns[0].amount == 12500000000
    # assert ai.address == "38BkoGvD3EPiChHjWJ55Do6zSn8QugJFhGyhdB9ybowSqPi8Ta"


def test_tx_account_info_cooldown_P7_3(grpcclient: GRPCClient):
    account = "3XJk92a2UXFMqiSDLKHM94QJhBTRfb8EQbHwsnCS5GfTVD47jT"
    block_hash = "02cef62160309949af7c6774d205706dacfdcd48b094bdf16d71fec109d2171e"
    ai = grpcclient.get_account_info(block_hash, hex_address=account, net=NET.MAINNET)
    print(ai)
    assert len(ai.cooldowns) == 1
    assert ai.cooldowns[0].status == CoolDownStatus.COOLDOWN
    assert ai.cooldowns[0].amount == 210720085700


def test_tx_account_info_cooldown_P7_2(grpcclient: GRPCClient):
    account = "4Z2sLB2cfHV1H2VGVEMJ7t6ZLKqiXVE1CF8fua1f89fi3bc1nx"
    block_hash = "last_final"
    ai = grpcclient.get_account_info(block_hash, hex_address=account, net=NET.TESTNET)
    print(ai)
    # assert ai.cooldowns[0].status == CoolDownStatus.PRE_PRE_COOLDOWN
    # assert ai.cooldowns[0].amount == 12500000000
    # assert ai.address == "38BkoGvD3EPiChHjWJ55Do6zSn8QugJFhGyhdB9ybowSqPi8Ta"
