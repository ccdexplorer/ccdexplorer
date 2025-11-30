# Welcome to CCDExplorer.io

## Introduction
This is the **DRAFT** documentation repository for all things CCDExplorer.io, including the API, notification bot and all (micro-) services. This is *very much* a work in progress.

The repo is organized as a monorepo, using [Polylith for Python](https://github.com/DavidVujic/python-polylith). 


Documentation follows the Polylith repo organization (components, bases, projects) and now also ships with an “Entities” section that describes the core Concordium objects (blocks, transactions, accounts, aliases, contracts, modules, tokens). Use the navigation or the quick links below to dive in.

### Projects
- User Facing
    - [CCDExplorer.io (site)](projects/site.md)
    - [CCDExplorer.io (API)](projects/api.md)
    - [CCDExplorer.io (Bot)](projects/bot.md)
- Background Services
    - Timed services
        - [Transactions by Type/Contents](projects/timed_services/transactions_by_type_contents.md)
        - [Statistics Daily Holders](projects/timed_services/statistics_daily_holders.md)
        - [Statistics Daily Limits](projects/timed_services/statistics_daily_limits.md)
        - [Transactions by Projects](projects/timed_services/transactions_by_projects.md)
        - [Unique Addresses](projects/timed_services/statistics_unique_addresses.md)
        - [Realised Prirces](projects/timed_services/statistics_realized_prices.md)
    - Every block
        - [Heartbeat](projects/every_block/heartbeat.md)
        - [Block Analyzer](projects/every_block/block_analyzer.md)
        - [New Account Address](projects/every_block/new_address.md)
        - [New/Upgraded Contract](projects/every_block/new_contract.md)
        - [New/Upgraded Module](projects/every_block/new_module.md)

### Entities
- [Block](entities/block.md)
- [Transaction](entities/transaction.md)
- [Account](entities/account.md)
- [Alias](entities/alias.md)
- [Contract](entities/contract.md)
- [Module](entities/module.md)
- [Token](entities/token.md)
