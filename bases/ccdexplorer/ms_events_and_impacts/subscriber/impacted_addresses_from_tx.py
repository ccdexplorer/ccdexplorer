# pyright: reportOptionalMemberAccess=false
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_BlockItemSummary,
    microCCD,
    CCD_TokenTransferEvent,
    CCD_TokenEvent,
    CCD_MetaEvent,
    CCD_LockCreateEvent,
    CCD_LockDestroyEvent,
)
from ccdexplorer.mongodb import Collections

from ccdexplorer.domain.mongo import (
    AccountStatementEntryType,
    AccountStatementTransferType,
    MongoImpactedAddress,
    PLTTransferType,
)

from .utils import Utils


########### Impacted Addresses
class ImpactedAddresses(Utils):
    def file_a_balance_movement(
        self,
        tx: CCD_BlockItemSummary,
        impacted_addresses_in_tx: dict[str, MongoImpactedAddress],
        impacted_address: str,
        balance_movement_to_add: AccountStatementEntryType | None,
        plt_token_id: str | None = None,
        lock_id: str | None = None,
    ):
        if impacted_addresses_in_tx.get(impacted_address):
            impacted_address_as_class: MongoImpactedAddress = impacted_addresses_in_tx[
                impacted_address
            ]
            if plt_token_id:
                impacted_address_as_class.plt_token_id = plt_token_id
            if lock_id:
                # a single tx can touch more than one lock for the same address (e.g.
                # destroy one lock, create another) - accumulate, don't overwrite
                if not impacted_address_as_class.lock_ids:
                    impacted_address_as_class.lock_ids = []
                if lock_id not in impacted_address_as_class.lock_ids:
                    impacted_address_as_class.lock_ids.append(lock_id)
            bm: AccountStatementEntryType | None = impacted_address_as_class.balance_movement
            if not bm:
                bm = AccountStatementEntryType()
            if balance_movement_to_add:
                field_set = list(balance_movement_to_add.model_fields_set)[0]

                if field_set in [
                    "transfer_in",
                    "transfer_out",
                    "plt_transfer_in",
                    "plt_transfer_out",
                    "amount_encrypted",
                    "amount_decrypted",
                ]:
                    impacted_address_as_class.included_in_flow = True

                if field_set == "transfer_in":
                    if not bm.transfer_in:
                        bm.transfer_in = []
                    bm.transfer_in.extend(balance_movement_to_add.transfer_in)
                elif field_set == "transfer_out":
                    if not bm.transfer_out:
                        bm.transfer_out = []
                    bm.transfer_out.extend(balance_movement_to_add.transfer_out)
                elif field_set == "plt_transfer_in":
                    if not bm.plt_transfer_in:
                        bm.plt_transfer_in = []
                    bm.plt_transfer_in.extend(balance_movement_to_add.plt_transfer_in)
                elif field_set == "plt_transfer_out":
                    if not bm.plt_transfer_out:
                        bm.plt_transfer_out = []
                    bm.plt_transfer_out.extend(balance_movement_to_add.plt_transfer_out)

                elif field_set == "amount_encrypted":
                    bm.amount_encrypted = balance_movement_to_add.amount_encrypted
                elif field_set == "amount_decrypted":
                    bm.amount_decrypted = balance_movement_to_add.amount_decrypted
                elif field_set == "baker_reward":
                    bm.baker_reward = balance_movement_to_add.baker_reward
                elif field_set == "finalization_reward":
                    bm.finalization_reward = balance_movement_to_add.finalization_reward
                elif field_set == "foundation_reward":
                    bm.foundation_reward = balance_movement_to_add.foundation_reward
                elif field_set == "transaction_fee_reward":
                    bm.transaction_fee_reward = balance_movement_to_add.transaction_fee_reward

            impacted_address_as_class.balance_movement = bm
        else:
            # new address
            included_in_flow = False
            if balance_movement_to_add:
                model_fields_set = list(balance_movement_to_add.model_fields_set)
                if len(model_fields_set) > 0:
                    field_set = model_fields_set[0]
                    if field_set in [
                        "transfer_in",
                        "transfer_out",
                        "plt_transfer_in",
                        "plt_transfer_out",
                        "amount_encrypted",
                        "amount_decrypted",
                    ]:
                        included_in_flow = True

            impacted_address_as_class = MongoImpactedAddress(
                **{
                    "_id": f"{tx.hash}-{impacted_address[:29]}",
                    "tx_hash": tx.hash,
                    "impacted_address": impacted_address,
                    "impacted_address_canonical": impacted_address[:29],
                    "effect_type": tx.type.contents,
                    "balance_movement": balance_movement_to_add,
                    "block_height": tx.block_info.height,
                    "included_in_flow": included_in_flow,
                    "plt_token_id": plt_token_id,
                    "lock_ids": [lock_id] if lock_id else None,
                    "date": f"{tx.block_info.slot_time:%Y-%m-%d}",
                }
            )
            impacted_addresses_in_tx[impacted_address] = impacted_address_as_class

    def file_balance_movements(
        self,
        tx: CCD_BlockItemSummary,
        impacted_addresses_in_tx: dict[str, MongoImpactedAddress],
        amount: microCCD | str,
        sender: str,
        receiver: str,
        plt_token_id: str | None = None,
    ):
        # first add to sender balance_movement
        if int(amount) > 0:
            balance_movement = AccountStatementEntryType(
                transfer_out=[
                    AccountStatementTransferType(
                        amount=amount,
                        counterparty=receiver[:29] if len(receiver) > 20 else receiver,
                    )
                ]
            )
        else:
            balance_movement = None

        self.file_a_balance_movement(
            tx, impacted_addresses_in_tx, sender, balance_movement, plt_token_id
        )

        # then to the receiver balance_movement
        if int(amount) > 0:
            balance_movement = AccountStatementEntryType(
                transfer_in=[
                    AccountStatementTransferType(
                        amount=amount,
                        counterparty=sender[:29] if len(sender) > 20 else sender,
                    )
                ]
            )
        else:
            balance_movement = None

        self.file_a_balance_movement(
            tx, impacted_addresses_in_tx, receiver, balance_movement, plt_token_id
        )

    def file_balance_movements_plt(
        self,
        tx: CCD_BlockItemSummary,
        impacted_addresses_in_tx: dict[str, MongoImpactedAddress],
        event: CCD_TokenTransferEvent,
        plt_token_id: str,
    ):
        # a transfer touching a lock carries from_lock (paid out of a lock) or
        # to_lock (paid into a lock) alongside the regular from_/to accounts
        lock = event.from_lock or event.to_lock
        lock_id = lock.to_str() if lock else None

        # first add to sender balance_movement
        if int(event.amount.value) > 0:
            balance_movement = AccountStatementEntryType(
                plt_transfer_out=[PLTTransferType(event=event, token_id=plt_token_id)]
            )
        else:
            balance_movement = None

        self.file_a_balance_movement(
            tx,
            impacted_addresses_in_tx,
            event.from_.account,
            balance_movement,
            plt_token_id,
            lock_id=lock_id,
        )

        # then to the receiver balance_movement
        if int(event.amount.value) > 0:
            balance_movement = AccountStatementEntryType(
                plt_transfer_in=[PLTTransferType(event=event, token_id=plt_token_id)]
            )
        else:
            balance_movement = None

        self.file_a_balance_movement(
            tx,
            impacted_addresses_in_tx,
            event.to.account,
            balance_movement,
            plt_token_id,
            lock_id=lock_id,
        )

    def extract_impacted_addresses_from_tx(self, tx: CCD_BlockItemSummary):
        self.queues: dict[Collections, list]
        impacted_addresses_in_tx: dict = {}
        if tx.account_creation:
            balance_movement = AccountStatementEntryType()
            self.file_a_balance_movement(
                tx,
                impacted_addresses_in_tx,
                tx.account_creation.address,
                balance_movement,
            )

        elif tx.token_creation:
            if len(tx.token_creation.events) > 0:
                self.process_plt_events(
                    tx,
                    tx.token_creation.events,
                    impacted_addresses_in_tx,
                )
            else:
                # for a token creation, there are no costs associated, so empty balance movement
                # we make the governance account the impacted address for token creation
                balance_movement = AccountStatementEntryType(
                    transaction_fee=0  # no tx cost
                )
                self.file_a_balance_movement(
                    tx,
                    impacted_addresses_in_tx,
                    tx.token_creation.create_plt.initialization_parameters.governance_account.account,
                    balance_movement,
                    plt_token_id=tx.token_creation.create_plt.token_id,
                )

        elif tx.account_transaction:
            # Always store the fee. Normally for the sender, but use sponsor if present
            if not tx.account_transaction.sponsor:
                balance_movement = AccountStatementEntryType(
                    transaction_fee=tx.account_transaction.cost
                )
                self.file_a_balance_movement(
                    tx,
                    impacted_addresses_in_tx,
                    tx.account_transaction.sender,
                    balance_movement,
                )
            else:
                balance_movement = AccountStatementEntryType(
                    sponsored_transaction_fee=tx.account_transaction.sponsor.cost
                )
                self.file_a_balance_movement(
                    tx,
                    impacted_addresses_in_tx,
                    tx.account_transaction.sponsor.sponsor,
                    balance_movement,
                )

            if tx.account_transaction.effects.contract_initialized:
                if tx.account_transaction.effects.contract_initialized.amount > 0:
                    self.file_balance_movements(
                        tx,
                        impacted_addresses_in_tx,
                        tx.account_transaction.effects.contract_initialized.amount,
                        tx.account_transaction.sender,
                        tx.account_transaction.effects.contract_initialized.address.to_str(),
                    )
                else:
                    balance_movement = None
                    self.file_a_balance_movement(
                        tx,
                        impacted_addresses_in_tx,
                        tx.account_transaction.effects.contract_initialized.address.to_str(),
                        balance_movement,
                    )

            elif tx.account_transaction.effects.contract_update_issued:
                for effect in tx.account_transaction.effects.contract_update_issued.effects:
                    if effect.updated:
                        instigator_str = self.address_to_str(effect.updated.instigator)
                        self.file_balance_movements(
                            tx,
                            impacted_addresses_in_tx,
                            effect.updated.amount,
                            instigator_str,
                            effect.updated.address.to_str(),
                        )

                    elif effect.transferred:
                        self.file_balance_movements(
                            tx,
                            impacted_addresses_in_tx,
                            effect.transferred.amount,
                            effect.transferred.sender.to_str(),
                            effect.transferred.receiver,
                        )

                    elif effect.interrupted:
                        balance_movement = None
                        self.file_a_balance_movement(
                            tx,
                            impacted_addresses_in_tx,
                            effect.interrupted.address.to_str(),
                            balance_movement,
                        )

                    elif effect.resumed:
                        balance_movement = None
                        self.file_a_balance_movement(
                            tx,
                            impacted_addresses_in_tx,
                            effect.resumed.address.to_str(),
                            balance_movement,
                        )

            elif tx.account_transaction.effects.account_transfer:
                self.file_balance_movements(
                    tx,
                    impacted_addresses_in_tx,
                    tx.account_transaction.effects.account_transfer.amount,
                    tx.account_transaction.sender,
                    tx.account_transaction.effects.account_transfer.receiver,
                )

            elif tx.account_transaction.effects.transferred_with_schedule:
                self.file_balance_movements(
                    tx,
                    impacted_addresses_in_tx,
                    self.get_sum_amount_from_scheduled_transfer(
                        tx.account_transaction.effects.transferred_with_schedule.amount
                    ),
                    tx.account_transaction.sender,
                    tx.account_transaction.effects.transferred_with_schedule.receiver,
                )

            elif tx.account_transaction.effects.transferred_to_encrypted:
                balance_movement = AccountStatementEntryType(
                    amount_encrypted=tx.account_transaction.effects.transferred_to_encrypted.amount
                )
                self.file_a_balance_movement(
                    tx,
                    impacted_addresses_in_tx,
                    tx.account_transaction.sender,
                    balance_movement,
                )

            elif tx.account_transaction.effects.transferred_to_public:
                balance_movement = AccountStatementEntryType(
                    amount_decrypted=tx.account_transaction.effects.transferred_to_public.amount
                )
                self.file_a_balance_movement(
                    tx,
                    impacted_addresses_in_tx,
                    tx.account_transaction.sender,
                    balance_movement,
                )

            elif tx.account_transaction.effects.token_update_effect:
                self.process_plt_events(
                    tx,
                    tx.account_transaction.effects.token_update_effect.events,
                    impacted_addresses_in_tx,
                )

            elif tx.account_transaction.effects.meta_update_effect:
                self.process_meta_events(
                    tx,
                    tx.account_transaction.effects.meta_update_effect.events,
                    impacted_addresses_in_tx,
                )

        return impacted_addresses_in_tx.values()

    def _process_token_like_event(
        self,
        tx: CCD_BlockItemSummary,
        event: CCD_TokenEvent | CCD_MetaEvent,
        impacted_addresses_in_tx: dict,
    ):
        """File impacted addresses for the module/transfer/mint/burn event variants shared by
        `CCD_TokenEvent` (token_update_effect) and `CCD_MetaEvent` (meta_update_effect).

        `CCD_MetaEvent` has no top-level `token_id` (unlike `CCD_TokenEvent`), so token_id is
        always read off the specific sub-event instead - that field is populated identically
        by the gRPC converter regardless of which wrapper the sub-event is nested under.
        """
        if event.module_event:
            # no impacted addresses other than governance address, which is captured by the fee in CCD
            # but need to update to add plt_token_id!
            # this branch is only reached via token_update_effect/meta_update_effect, both of
            # which are account_transaction effects - unlike transfer/mint/burn, which can also
            # come from tx.token_creation.events where account_transaction is None.
            # Routed through file_a_balance_movement (not direct dict indexing) since a
            # sponsored tx only files the sponsor via the fee movement, not the sender.
            assert tx.account_transaction is not None
            self.file_a_balance_movement(
                tx,
                impacted_addresses_in_tx,
                tx.account_transaction.sender,
                None,
                plt_token_id=event.module_event.token_id,
            )

        elif event.transfer_event:
            self.file_balance_movements_plt(
                tx,
                impacted_addresses_in_tx,
                event.transfer_event,
                plt_token_id=event.transfer_event.token_id,
            )
        elif event.mint_event:
            impacted_address = event.mint_event.target.account
            token_id = event.mint_event.token_id
            balance_movement = AccountStatementEntryType(
                plt_transfer_in=[PLTTransferType(event=event.mint_event, token_id=token_id)]
            )
            self.file_a_balance_movement(
                tx,
                impacted_addresses_in_tx,
                impacted_address,
                balance_movement,
                plt_token_id=token_id,
            )
        elif event.burn_event:
            impacted_address = event.burn_event.target.account
            token_id = event.burn_event.token_id
            balance_movement = AccountStatementEntryType(
                plt_transfer_out=[PLTTransferType(event=event.burn_event, token_id=token_id)]
            )
            self.file_a_balance_movement(
                tx,
                impacted_addresses_in_tx,
                impacted_address,
                balance_movement,
                plt_token_id=token_id,
            )

    def process_plt_events(
        self,
        tx: CCD_BlockItemSummary,
        events: list[CCD_TokenEvent],
        impacted_addresses_in_tx: dict,
    ):
        for event in events:
            self._process_token_like_event(tx, event, impacted_addresses_in_tx)

    def process_meta_events(
        self,
        tx: CCD_BlockItemSummary,
        events: list[CCD_MetaEvent],
        impacted_addresses_in_tx: dict,
    ):
        for event in events:
            if event.lock_create_event:
                self._process_lock_create_event(
                    tx, event.lock_create_event, impacted_addresses_in_tx
                )
            elif event.lock_destroy_event:
                self._process_lock_destroy_event(
                    tx, event.lock_destroy_event, impacted_addresses_in_tx
                )
            else:
                self._process_token_like_event(tx, event, impacted_addresses_in_tx)

    def _process_lock_create_event(
        self,
        tx: CCD_BlockItemSummary,
        event: CCD_LockCreateEvent,
        impacted_addresses_in_tx: dict,
    ):
        assert tx.account_transaction is not None
        self.grpc_client: GRPCClient
        lock_id = event.lock_id.to_str()
        config = self.grpc_client.decode_lock_config(event.lock_config)

        # creator + every controller + every explicit recipient are impacted by the lock
        # being created, not just the tx sender. No plt_token_id here: a lock can support
        # multiple tokens (config.controller.tokens), so there's no single correct value to
        # tag - these accounts already surface via lock_id, and a per-token tx list picking
        # one token arbitrarily would be misleading.
        accounts = {tx.account_transaction.sender}
        accounts.update(grant.account for grant in config.controller.grants)
        if isinstance(config.recipients, list):
            accounts.update(config.recipients)

        for account in accounts:
            self.file_a_balance_movement(
                tx,
                impacted_addresses_in_tx,
                account,
                None,
                lock_id=lock_id,
            )

    def _process_lock_destroy_event(
        self,
        tx: CCD_BlockItemSummary,
        event: CCD_LockDestroyEvent,
        impacted_addresses_in_tx: dict,
    ):
        # the tx sender is usually already filed via the unconditional fee balance movement,
        # but not for a sponsored tx (only the sponsor gets filed then) - route through
        # file_a_balance_movement rather than indexing the dict directly, so this still
        # creates the record if it's not there yet. No other account data is available on
        # a destroy event beyond the lock id.
        assert tx.account_transaction is not None
        self.file_a_balance_movement(
            tx,
            impacted_addresses_in_tx,
            tx.account_transaction.sender,
            None,
            lock_id=event.lock_id.to_str(),
        )

    ########## Impacted Addresses
