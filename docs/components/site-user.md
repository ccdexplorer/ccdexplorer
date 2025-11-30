# Site User Models (`components/ccdexplorer/site_user`)

`site_user` contains the Pydantic models shared between the Explorer site, API, and notification bot to describe user accounts,
linked Concordium accounts, and notification preferences.

## Model overview
- `NotificationService` / `NotificationPreferences` – enable/disable Telegram or email channels plus optional limits (minimum CCD amount, exclude exchange-to-exchange transfers, etc.).
- `AccountNotificationPreferences`, `ValidatorNotificationPreferences`, `OtherNotificationPreferences` – granular opt-in flags for the dozens of Concordium events the bot/site can monitor (validator suspension, payday rewards, module deployments, token creation, etc.).
- `ContractNotificationPreferences` – method-level toggles for smart contract receive functions so users can focus on the entrypoints they care about.
- `AccountForUser` – ties an Explorer user to a Concordium account index/address, including delegation targets and the configured notification preferences.

## Usage
```python
from ccdexplorer.site_user import AccountForUser

acct = AccountForUser(
    account_index=1234,
    account_id="4F4cGJ8nMM7AGdHhE74L521sgot2GjZ1Pu417wfgQCkZoPXXfE",
    account_notification_preferences={"account_transfer": {"telegram": {"enabled": True}}},
)
```

Storing and validating preferences through these models keeps the API, bot, and background workers in sync even as the notification catalogue grows.***
