# ruff: noqa: F403, F405, E402, E501

from pydantic import BaseModel
from typing import Optional
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_ContractAddress,
    microCCD,
    CCD_AccountAddress,
    CCD_AccountIndex,
)
import datetime as dt
from enum import Enum


class Reward:
    """ """

    def __init__(self, r):
        self.r = r

    def account_reward(self):
        r = self.r
        if r.payday_account_reward:
            return r.payday_account_reward.account
        else:
            return None

    def pool_reward(self):
        r = self.r

        if r.payday_pool_reward:
            return r.payday_pool_reward.pool_owner
        else:
            return None


class NotificationServices(str, Enum):
    telegram = "telegram"
    email = "email"


class NotificationService(BaseModel):
    enabled: Optional[bool] = None
    limit: Optional[microCCD] = None
    exclude_exchange_to_exchange: Optional[bool] = None


class NotificationPreferences(BaseModel):
    telegram: Optional[NotificationService] = None
    email: Optional[NotificationService] = None


class ContractNotificationPreferences(BaseModel):
    # this is a list of methods that can be individually enabled
    contract_update_issued: Optional[dict[str, NotificationPreferences]] = None


class AccountNotificationPreferences(BaseModel):
    validator_commission_changed: Optional[NotificationPreferences] = None
    validator_primed_for_suspension: Optional[NotificationPreferences] = None
    validator_suspended: Optional[NotificationPreferences] = None
    delegation_configured: Optional[NotificationPreferences] = None
    payday_account_reward: Optional[NotificationPreferences] = None
    account_transfer: Optional[NotificationPreferences] = None
    transferred_with_schedule: Optional[NotificationPreferences] = None
    data_registered: Optional[NotificationPreferences] = None
    module_deployed: Optional[NotificationPreferences] = None
    contract_initialized: Optional[NotificationPreferences] = None
    encrypted_amount_transferred: Optional[NotificationPreferences] = None
    transferred_to_encrypted: Optional[NotificationPreferences] = None
    transferred_to_public: Optional[NotificationPreferences] = None

    credential_keys_updated: Optional[NotificationPreferences] = None
    credentials_updated: Optional[NotificationPreferences] = None
    token_update_effect: Optional[NotificationPreferences] = None
    # if the account is delegating, get notified if your validator configures

    token_event: Optional[NotificationPreferences] = None


class ValidatorNotificationPreferences(BaseModel):
    block_validated: Optional[NotificationPreferences] = None
    validator_configured: Optional[NotificationPreferences] = None
    payday_pool_reward: Optional[NotificationPreferences] = None
    delegation_configured: Optional[NotificationPreferences] = None
    validator_running_behind: Optional[NotificationPreferences] = None
    # baker_configured: Optional[NotificationPreferences] = None
    # block_baked_by_baker: Optional[NotificationPreferences] = None
    validator_primed_for_suspension: Optional[NotificationPreferences] = None
    validator_suspended: Optional[NotificationPreferences] = None
    validator_missed_rounds: Optional[NotificationPreferences] = None


class OtherNotificationPreferences(BaseModel):
    validator_commission_changed: Optional[NotificationPreferences] = None
    protocol_update: Optional[NotificationPreferences] = None
    add_anonymity_revoker_update: Optional[NotificationPreferences] = None
    add_identity_provider_update: Optional[NotificationPreferences] = None
    account_transfer: Optional[NotificationPreferences] = None
    transferred_with_schedule: Optional[NotificationPreferences] = None
    domain_name_minted: Optional[NotificationPreferences] = None
    module_deployed: Optional[NotificationPreferences] = None
    contract_initialized: Optional[NotificationPreferences] = None
    account_created: Optional[NotificationPreferences] = None
    # lowered_stake: Optional[NotificationPreferences] = None
    validator_lowered_stake: Optional[NotificationPreferences] = None
    validator_suspended: Optional[NotificationPreferences] = None
    validator_primed_for_suspension: Optional[NotificationPreferences] = None
    validator_node_version_upgrade: Optional[NotificationPreferences] = None
    token_creation: Optional[NotificationPreferences] = None


class AccountForUser(BaseModel):
    account_index: Optional[int] = None
    account_id: Optional[CCD_AccountAddress] = None
    # only set if targeting a validator (and not passive)
    delegation_target: Optional[CCD_AccountIndex] = None
    label: Optional[str] = None
    account_notification_preferences: Optional[AccountNotificationPreferences] = None
    validator_notification_preferences: Optional[ValidatorNotificationPreferences] = None


class ContractForUser(BaseModel):
    contract: Optional[CCD_ContractAddress] = None
    contract_name: Optional[str] = None
    label: Optional[str] = None
    contract_notification_preferences: Optional[ContractNotificationPreferences] = None


class SiteUser(BaseModel):
    telegram_chat_id: Optional[int] = None
    email_address: Optional[str] = None
    token: str
    username: Optional[str] = None
    first_name: Optional[str] = None
    language_code: Optional[str] = None
    accounts: Optional[dict] = {}
    contracts: Optional[dict] = {}
    other_notification_preferences: Optional[OtherNotificationPreferences] = None
    last_modified: Optional[dt.datetime] = None
    last_seen_on_site: Optional[dt.datetime] = None
