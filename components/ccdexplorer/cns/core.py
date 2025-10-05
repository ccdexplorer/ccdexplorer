import io
import base58

from eth_hash.auto import keccak

from enum import Enum
from ccdexplorer.domain.generic import NET

from rich.console import Console
from pymongo import ReplaceOne
from ccdexplorer.grpc_client import GRPCClient

console = Console()


class CNSActions(Enum):
    bid = "bid"
    cancel = "cancel"
    finalize = "finalize"
    setAddress = "setAddress"
    setData = "setData"
    transfer = "transfer"
    register = "register"
    createSubdomain = "createSubdomain"
    getTokenExpiry = "getTokenExpiry"
    extend = "extend"


class CNSEvent(Enum):
    CancelEvent = 233
    AbortEvent = 242
    FinalizeEvent = 243
    BidEvent = 244
    AuctionEvent = 247
    BuyEvent = 248
    UnlistEvent = 249


class CNSDomain:
    def __init__(self):
        self.function_calls = {}  # event["receiveName"] : event["message"]
        self.cns_event: CNSEvent | None = None
        self.transfer_to = None
        self.amount = 0
        self.subdomain = None
        self.tokenId = None
        self.action: CNSActions | None = None
        self.action_message = None
        self.domain_name = None
        self.set_address = None
        self.set_data_key = None
        self.set_data_value = None
        self.duration_years = None
        self.register_address = None

    # Helper functions
    def __repl__(self):
        return f"{self.tokenId=} | {self.domain_name=}"

    def bytes_from_hex_tokenID(self, hex):
        the_list = list(bytes.fromhex(hex[2:]))
        the_list.insert(0, 32)
        return the_list

    def write_binary_to_file(self, hex):
        newFileBytes = self.bytes_from_hex_tokenID(hex)
        newFileByteArray = bytearray(newFileBytes)
        newFile = open("token.bin", "wb")
        newFile.write(newFileByteArray)

    def namehash_dome(self, name: str, encoding="utf-8"):
        """ENS "namehash()" convention mapping of strings to bytes(32) hashes.

        Recursive function variant. Performs slightly better than the
        generator-based variant, but can't handle names with infinite (or
        extremely large) number of labels.

        :param name: name to hash, labels separated by dots
        :type name: str
        :returns: bytes(32)"""

        name = (
            name.encode("latin_1")
            .decode("raw_unicode_escape")
            .encode("utf-16", "surrogatepass")
            .decode("utf-16")
            .encode("raw_unicode_escape")
            .decode("latin_1")
        )

        if name == "":
            return b"\x00" * 32
        else:
            label, _, remainder = name.partition(".")
            return keccak(self.namehash_dome(remainder) + keccak(bytes(label, encoding=encoding)))

    def namehash_hashlib(self, name: str, encoding="utf-8"):
        """ENS "namehash()" convention mapping of strings to bytes(32) hashes.

        Recursive function variant. Performs slightly better than the
        generator-based variant, but can't handle names with infinite (or
        extremely large) number of labels.

        :param name: name to hash, labels separated by dots
        :type name: str
        :returns: bytes(32)"""
        import hashlib

        if name == "":
            return b"\x00" * 32
        else:
            label, _, remainder = name.partition(".")
            second_part = hashlib.new("sha3_256")
            second_part.update(bytes(label, encoding=encoding))
            ss = second_part.digest()
            return_statement = hashlib.new("sha3_256")
            return_statement.update(self.namehash_hashlib(remainder) + ss)
            return return_statement.digest()

    # CNS utilities
    def read_domain_owner(self, bs):
        t = int.from_bytes(bs.read(1), byteorder="little")
        if t == 0:
            return (0, "Token doesn't exist.")
        elif t == 1:
            to_ = self.address(bs)
            return (1, to_)

    # CNS Defined terms
    def token(self, bs):
        contract_index, contract_subindex = self.contract_address(bs)
        tokenID = self.token_id(bs)
        return contract_index, contract_subindex, tokenID

    def account_address(self, bs):
        addr = bs.read(32)
        return base58.b58encode_check(b"\x01" + addr).decode()

    def contract_address(self, bs):
        return int.from_bytes(bs.read(8), byteorder="little"), int.from_bytes(
            bs.read(8), byteorder="little"
        )

    def royalty_length(self, bs):
        return int.from_bytes(bs.read(4), byteorder="little")

    def royalty(self, bs):
        beneficiary_ = self.account_address(bs)  # noqa: F841
        percentage = self.percentage(bs)  # noqa: F841

    def address(self, bs):
        t = int.from_bytes(bs.read(1), byteorder="little")
        if t == 0:
            return self.account_address(bs)
        elif t == 1:
            return self.contract_address(bs)
        else:
            return "x" * 50

    def translate_hex_to_bytes(self, parameter_hex: str) -> bytes:
        parameter_bytes = list(bytes.fromhex(parameter_hex))
        parameter_bytes.insert(0, 32)
        return bytes(parameter_bytes)

    def get_cns_domain_name_v2(self, grpcclient: GRPCClient, hex: str, net: str):
        try:
            block_hash = "last_final"
            instance_index = 7073
            instance_subindex = 0
            entrypoint = "BictoryCnsNft.getTokenInfo"
            parameter_bytes = self.translate_hex_to_bytes(hex)

            ii = grpcclient.invoke_instance(
                block_hash,
                instance_index,
                instance_subindex,
                entrypoint,
                parameter_bytes,
                NET(net),
            )
            if ii.success is not None:
                res = ii.success.return_value
                bb = list(bytes.fromhex(res.decode()))
                domain_name = bytes(bb[5:-8]).decode()
        except Exception as _:
            domain_name = "Unable to determine..."
        self.domain_name = domain_name  # type: ignore

    def get_domain_name_owner_v2(self, grpcclient: GRPCClient, mongodb, Collections, domain_name):
        console.log(f"get_domain_name_owner for {domain_name}")
        result = mongodb.mainnet[Collections.cns_domains].find_one({"domain_name": domain_name})
        hex = None
        if result:
            hex = result["_id"]

        if not hex:
            bb = self.namehash_dome(domain_name)
            hex = bytes.hex(bb)
            document_to_store = ReplaceOne(
                {
                    "_id": hex,
                },
                {
                    "_id": hex,
                    "domain_name": domain_name,
                },
                upsert=True,
            )
            result = mongodb.mainnet[Collections.cns_domains].bulk_write([document_to_store])

        block_hash = "last_final"
        instance_index = 7073
        instance_subindex = 0
        entrypoint = "BictoryCnsNft.getTokenExpiry"
        parameter_bytes = self.translate_hex_to_bytes(hex)

        ii = grpcclient.invoke_instance(
            block_hash,
            instance_index,
            instance_subindex,
            entrypoint,
            parameter_bytes,
        )

        if ii.success is not None:
            res = ii.success.return_value
            bb = list(bytes.fromhex(res.decode()))
            byt = io.BytesIO(bytes(bb))
            (status, to) = CNSDomain().read_domain_owner(byt)  # type: ignore
            return (status, to)
        else:
            return (-1, None)

    def string(self, bs):
        # bs = io.BytesIO(bytes.fromhex(hex))
        n = int.from_bytes(bs.read(4), byteorder="little")
        name = bs.read(n)
        return bytes.decode(name, "UTF-8")

    def data_value(self, bs):
        t = int.from_bytes(bs.read(1), byteorder="little")
        if t == 0:
            return ""
        elif t == 1:
            return self.address(bs)
        elif t == 2:
            return self.string(bs)
        elif t == 3:
            return bs
        elif t == 4:
            return self.string(bs)
        elif t == 5:
            return self.token()  # type: ignore
        else:
            raise Exception("invalid type")

    def decode_set_data_from(self, hex):
        bs = io.BytesIO(bytes.fromhex(hex))
        domain_ = self.string(bs)
        key_ = self.string(bs)
        data_value_ = self.data_value(bs)
        return domain_, key_, data_value_

    def decode_set_address_from(self, hex):
        bs = io.BytesIO(bytes.fromhex(hex))
        domain_ = self.string(bs)
        address_ = self.address(bs)
        return domain_, address_

    def decode_from_extend(self, hex):
        bs = io.BytesIO(bytes.fromhex(hex))
        name_ = self.string(bs)
        duration_ = int.from_bytes(bs.read(1), byteorder="little")
        return name_, duration_

    def decode_from_register(self, hex):
        bs = io.BytesIO(bytes.fromhex(hex))
        name_ = self.string(bs)
        address_ = self.address(bs)
        duration_ = int.from_bytes(bs.read(1), byteorder="little")
        return name_, address_, duration_

    def decode_domain_from(self, hex):
        bs = io.BytesIO(bytes.fromhex(hex))
        name_ = self.string(bs)
        return name_

    def decode_token_id_from(self, hex):
        bs = io.BytesIO(bytes.fromhex(hex))
        id_ = self.token_id(bs)
        return id_

    def decode_subdomain_from(self, hex):
        bs = io.BytesIO(bytes.fromhex(hex))
        n = int.from_bytes(bs.read(4), byteorder="little")
        name = bs.read(n)
        return bytes.decode(name, "UTF-8")

    def decode_transfer_to_from(self, hex):
        all_data = self.transfer_parameter(io.BytesIO(bytes.fromhex(hex)))
        if len(all_data) > 0:
            if len(all_data[0]) > 3:
                return all_data[0][0], all_data[0][3]  # tokenId, to_address
            else:
                return None
        else:
            return None

    def transfer_parameter(self, bs):
        n = int.from_bytes(bs.read(2), byteorder="little")
        return self.transfers(bs, n)

    def transfers(self, bs, n):
        return [self.transfer(bs) for _ in range(n)]

    def transfer(self, bs):
        id_ = self.token_id(bs)
        amount = self.token_amount(bs)
        from_ = self.address(bs)
        to = self.receiver(bs)
        data = self.additional_data(bs)
        return [id_, amount, from_, to, data]

    def token_id(self, bs):
        n = int.from_bytes(bs.read(1), byteorder="little")
        return bytes.hex(bs.read(n))

    def token_amount(self, bs):
        return int.from_bytes(bs.read(1), byteorder="little")

    def percentage(self, bs):
        return int.from_bytes(bs.read(8), byteorder="little")

    def receiver(self, bs):
        t = int.from_bytes(bs.read(1), byteorder="little")
        if t == 0:
            return self.account_address(bs)
        elif t == 1:
            return self.contract_address(bs), self.receive_hook_name(bs)
        else:
            return "x" * 50

    def receive_hook_name(self, bs):
        n = int.from_bytes(bs.read(2), byteorder="little")
        name = bs.read(n)
        return bytes.decode(name, "UTF-8")

    def additional_data(self, bs):
        n = int.from_bytes(bs.read(2), byteorder="little")
        data = bs.read(n)
        return data

    # CNS Event
    def buyEvent(self, hexParameter):
        bs = io.BytesIO(bytes.fromhex(hexParameter))

        tag_ = int.from_bytes(bs.read(1), byteorder="little")
        self.cns_event = CNSEvent(tag_)

        contract_index, contract_subindex, token_id_ = self.token(bs)
        seller_ = self.account_address(bs)
        buyer_ = self.account_address(bs)
        price_ = self.token_amount(bs)
        seller_share_ = self.token_amount(bs)
        royalty_len_ = int.from_bytes(bs.read(4), byteorder="little")
        royalties_ = []
        for _ in range(royalty_len_):
            royalties_.append(self.royalty(bs))

        return [
            tag_,
            contract_index,
            contract_subindex,
            token_id_,
            seller_,
            buyer_,
            price_,
            seller_share_,
            royalties_,
        ]

    def bidEvent(self, hexParameter):
        bs = io.BytesIO(bytes.fromhex(hexParameter))

        tag_ = int.from_bytes(bs.read(1), byteorder="little")
        self.cns_event = CNSEvent(tag_)

        contract_index, contract_subindex, token_id_ = self.token(bs)
        bidder_ = self.account_address(bs)
        amount_ = self.token_amount(bs)

        return [tag_, contract_index, contract_subindex, token_id_, bidder_, amount_]

    def cancelEvent(self, hexParameter):
        bs = io.BytesIO(bytes.fromhex(hexParameter))

        tag_ = int.from_bytes(bs.read(1), byteorder="little")
        self.cns_event = CNSEvent(tag_)

        contract_index, contract_subindex, token_id_ = self.token(bs)
        owner_ = self.account_address(bs)

        return [tag_, contract_index, contract_subindex, token_id_, owner_]

    def abortEvent(self, hexParameter):
        bs = io.BytesIO(bytes.fromhex(hexParameter))

        tag_ = int.from_bytes(bs.read(1), byteorder="little")
        self.cns_event = CNSEvent(tag_)

        contract_index, contract_subindex, token_id_ = self.token(bs)
        owner_ = self.account_address(bs)
        bidder_ = self.account_address(bs)
        amount_ = self.token_amount(bs)

        return [
            tag_,
            contract_index,
            contract_subindex,
            token_id_,
            owner_,
            bidder_,
            amount_,
        ]

    def finalizeEvent(self, hexParameter):
        bs = io.BytesIO(bytes.fromhex(hexParameter))

        tag_ = int.from_bytes(bs.read(1), byteorder="little")
        self.cns_event = CNSEvent(tag_)

        contract_index, contract_subindex, token_id_ = self.token(bs)
        seller_ = self.account_address(bs)
        winner_ = self.account_address(bs)
        price_ = self.token_amount(bs)
        seller_share = self.token_amount(bs)
        royalty_length_ = self.royalty_length(bs)
        royalties = [self.royalty(bs) for x in range(royalty_length_)]

        return [
            tag_,
            contract_index,
            contract_subindex,
            token_id_,
            seller_,
            winner_,
            price_,
            seller_share,
            royalty_length_,
            royalties,
        ]

    def finalize(self, hexParameter):
        bs = io.BytesIO(bytes.fromhex(hexParameter))
        tag_ = int.from_bytes(bs.read(1), byteorder="little")

        # only available for finalizeEvent
        seller_, winner_, price_, seller_share, royalty_length_, royalties = (
            None,
            None,
            None,
            None,
            None,
            None,
        )

        # only available for abortEvent, cancelEvent
        owner_, bidder_, amount_ = None, None, None

        # tag 161 is undocumented for now?
        (
            contract_index,
            contract_subindex,
            token_id_,
        ) = (
            None,
            None,
            None,
        )

        if tag_ == 243:
            (
                tag_,
                contract_index,
                contract_subindex,
                token_id_,
                seller_,
                winner_,
                price_,
                seller_share,
                royalty_length_,
                royalties,
            ) = self.finalizeEvent(hexParameter)

        if tag_ == 242:
            (
                tag_,
                contract_index,
                contract_subindex,
                token_id_,
                owner_,
                bidder_,
                amount_,
            ) = self.abortEvent(hexParameter)

        if tag_ == 233:
            (
                tag_,
                contract_index,
                contract_subindex,
                token_id_,
                owner_,
            ) = self.cancelEvent(hexParameter)

        return [
            tag_,
            contract_index,
            contract_subindex,
            token_id_,
            seller_,
            winner_,
            price_,
            seller_share,
            royalty_length_,
            royalties,
            owner_,
            bidder_,
            amount_,
        ]
