# .Github Repo
This is a special repo that hosts [projects.json](https://github.com/ccdexplorer/.github/blob/5de7935c90c08f2fefed167ea50f978b6dcc72cc/projects.json), a community owned json-file that contains information about recognized projects on the Concordium blockchain. 

Example content:
```
"djookyx": {
        "display_name": "DjookyX",
        "url": "https://djookyx.com/",
        "description": "",
        "mainnet": {
            "account_addresses": [ 
            {
                "account_index": 96613,
                "account_address": "4rU8pSVm3dCqxgbDQjWDboTE6EegyWo43dAbQeyCtQw5NQUqF5"
            },
            {
                "account_index": 93963,
                "account_address": "4ArYT1qkX2Ap5TvpmBevC9Ad17ULDff7QqBCiQGp1e8Bhi3Shk"
            },
            {
                "account_index": 96415,
                "account_address": "3dUr8c15HUhe2qUyAFghWt7g4EiRxdngDXKAzHxS3JbzWnr53F"
            },
            {
                "account_index": 97840,
                "account_address": "417C6DEiSjowwNMt7b4wUVSzpxp4QwmrNvtwye27UcmrWKgCdf"
            },
            {
                "account_index": 97842,
                "account_address": "3Q4Lj624JrZf8oyegYSntxkM5bUjE7D8Bs9kWpJikggD3QUE1E"
            }],
           
        "modules":[
            "80c18cbad7d4294a2f964a402341f09f27eb72ec2201f176f6cca52260b7f410",
            "ba5d3e587108208da7bca88fe0cb6a3d84bcc647cb844d781cbad4c1256e7fee"
        ]
        }
    },
```

## Add content
Issue a pull request to this repo, containing edits/additions to this file, in order to get a project recognized. 


## Goal
CCDExplorer.io enables the community to add `community labels` to accounts and contracts through the `projects.json` file in the github repo.

This page describes how and where labels are displayed. 


## Data used
Data for labels is stored in the `projects` collection. This has documents for `accounts`, `contracts` and `modules`.

## Examples
### Account
```
{
  "_id": "provenance-address-86568",
  "project_id": "provenance",
  "type": "account_address",
  "account_index": 86568,
  "account_address": "3suZfxcME62akyyss72hjNhkzXeZuyhoyQz1tvNSXY2yxvwo53"
}
```

### Contract
```
{
  "_id": "panenkafc-address-<9534,0>",
  "project_id": "panenkafc",
  "type": "contract_address",
  "contract_index": 9534,
  "contract_address": "<9534,0>"
}

```

### Module
```
{
  "_id": "gonana-module-9c41b5b9dbca53667839e041c9ede71ded569752196411e0bfa5648cad821c94",
  "project_id": "gonana",
  "type": "module",
  "module_ref": "9c41b5b9dbca53667839e041c9ede71ded569752196411e0bfa5648cad821c94"
}
```

This information is sourced from `projects.json` and saved in this collection using the "Update Projects" service. This service takes all entries in `projects.json` and saves, for each project, the account addresses and modules with the corresponding `project_id`. From the modules, this service also finds all contracts from these modules (at runtime) and labels these contract accordingly. 

!!! Note 1
    Finding contracts from modules is performed in this service only when the service runs, i.e. when `projects.json` is updated. Any new contracts initialized from existing modules are captured in the [Transactions By Projects](../services/end_of_day/transactions_by_projects.md) service.

## Specialized labeling
For projects that need specialized labeling conventions (such as 5Tars), we have the possbility to add bespoke `display_name` properties to the entries in the `projects` collection. Entries containing a `display_name` property, will not be overwritten.

    
```
    {
  "_id": "5tars-address-<9904,0>",
  "project_id": "5tars",
  "type": "contract_address",
  "contract_index": 9904,
  "contract_address": "<9904,0>",
  "display_name": "STARZ Wallets"
}
```
