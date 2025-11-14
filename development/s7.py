import marimo

__generated_with = "0.17.7"
app = marimo.App(width="full")


@app.cell
def _():
    from ccdexplorer.mongodb import MongoDB, Collections
    from ccdexplorer.cis import CIS
    from ccdexplorer.grpc_client.core import GRPCClient
    from ccdexplorer.domain.generic import NET
    from ccdexplorer.domain.cis import s7_InventoryCreateParams_ERC721_V2
    from ccdexplorer.grpc_client.CCD_Types import CCD_BlockItemSummary
    import marimo as mo
    import pandas as pd
    from optparse import OptionParser
    import requests
    import inspect
    mongodb = MongoDB(None)
    grpc_client = GRPCClient()
    cis = CIS(grpc_client)
    return (
        CCD_BlockItemSummary,
        Collections,
        NET,
        cis,
        mo,
        mongodb,
        pd,
        requests,
        s7_InventoryCreateParams_ERC721_V2,
    )


@app.cell
def _():
    # parser = OptionParser()
    # # inspect.getmembers(cis, predicate=inspect.ismethod)
    # cis.s7_inventory_create_erc721_v1
    # cis.s7_inventory_create_erc1155_v1
    return


@app.cell
def _():
    erc721_v1_contracts = [0,377,378]
    return (erc721_v1_contracts,)


@app.cell(hide_code=True)
def _():
    erc721_v2_contracts = [
        2, 3, 4,
        5, 6, 7,
        14, 15, 16,
        17, 18, 19,
        20, 21, 22,
        23, 24, 25,
        26, 27, 28,
        29, 30, 31,
        32, 33, 34,
        35, 36, 37,
        38, 39, 40,
        41, 42, 43,
        44, 45, 46,
        47, 48, 49,
        50, 51, 52,
        53, 54, 55,
        56, 57, 58,
        59, 60, 61,
        62, 63, 64,
        65, 66, 67,
        68, 69, 70,
        71, 72, 73,
        74, 75, 76,
        77, 78, 79,
        80, 81, 82,
        1879, 1880, 1881,
        1882, 1883, 1884,
        1885, 1886, 1887,
        1888, 1889, 1890,
        1891, 1892, 1893,
        1894, 1895, 1896,
        1897, 1898, 1899,
        1900, 1901, 1902,
        1903, 1904, 1905,
        1906, 1907, 1908,
        1909, 1910, 1911,
        1912, 1913, 1914,
        1915, 1916, 1917,
        1918, 1919, 1920,
        1921, 1922, 1923,
        1924, 1925, 1926,
        1927, 1928, 1929,
        1930, 1931, 1932,
        1933, 1934, 1935,
        1936, 1937, 1938,
        1939, 1940, 1941,
        1942, 1943, 1944,
        1945, 1946, 1947,
        1948, 1949, 1950,
        1951, 1952, 1953,
        1954, 1955, 1956,
        1957, 1958, 1959,
        1960, 1961, 1962,
        1963, 1964, 1965,
        1966, 1967, 1968,
        1969, 1970, 1971,
        1972, 1973, 1974,
        1975, 1976, 1977,
        1978, 1979, 1980,
        1981, 1982, 1983,
        1984, 1985, 1986,
        1987, 1988, 1989,
        1990, 1991, 1992,
        1993, 1994, 1995,
        1996, 1997, 1998
    ]
    return (erc721_v2_contracts,)


@app.cell
def _():
    erc_1155_v1_contracts_groups = (
        [[i, i + 1, i + 2] for i in range(8, 83, 3)] +
        [[i, i + 1, i + 2] for i in range(83, 1585, 3)] +
        [[i, i + 1, i + 2] for i in range(1879, 1999, 3)] +
        [[i, i + 1, i + 2] for i in range(1999, 3199, 3)]
    )
    erc_1155_v1_contracts = [x for group in erc_1155_v1_contracts_groups for x in group]
    return (erc_1155_v1_contracts,)


@app.cell
def _(erc_1155_v1_contracts):
    erc_1155_v1_contracts
    return


@app.cell(hide_code=True)
def _(NET, cis):
    entrypoint = "inventory.create"

    cis.instance_index = 3
    cis.instance_subindex = 0
    cis.entrypoint = entrypoint
    cis.net = NET.MAINNET
    hex="0800009ed89c8d03014feb8e5998fa692316d315a8f3c50b673b5ccb57f52180dc1abb512a117fb132000000000000000042000000697066733a2f2f6261666b726569656673696a74686e6c376d356d74796c36776163676836326f7a67377369357179737435716b7568786234716c37703773633734"
    parsed_result = cis.s7_inventory_create_erc721_v2(hex)
    parsed_result
    return


@app.cell(hide_code=True)
def _(CCD_BlockItemSummary, Collections, NET, cis, mongodb):
    def erc_721_v2(contract_index: int):
        erc_721_v2_txs = [x["tx_hash"] for x in mongodb.mainnet[Collections.impacted_addresses].find({"impacted_address_canonical":f"<{contract_index},0>"})]
        txs = [CCD_BlockItemSummary(**x) for x in mongodb.mainnet[Collections.transactions].find({"_id": {"$in": erc_721_v2_txs}})]
        results = {}
        for tx in txs:
            if not tx.account_transaction.effects.contract_update_issued:
                continue
            for effect in tx.account_transaction.effects.contract_update_issued.effects:
                if effect.updated:
                    if effect.updated.receive_name=="trader.create_and_sell":
                        entrypoint = "trader.create_and_sell"

                        cis.instance_index = contract_index
                        cis.instance_subindex = 0
                        cis.entrypoint = entrypoint
                        cis.net = NET.MAINNET
                        hex = effect.updated.parameter
                        parsed_result = cis.s7_trader_create_and_sell_erc721_v2(hex)
                        results[tx.hash] = parsed_result
        return results
    return


@app.cell
def _(
    CCD_BlockItemSummary,
    Collections,
    NET,
    cis,
    erc721_v1_contracts,
    erc721_v2_contracts,
    erc_1155_v1_contracts,
    mongodb,
    save_fake_cis2_token,
):
    def inventory_create(contract_index: int):
        imp_txs = [x["tx_hash"] for x in mongodb.mainnet[Collections.impacted_addresses].find({"impacted_address_canonical":f"<{contract_index},0>"})]
        txs = [CCD_BlockItemSummary(**x) for x in mongodb.mainnet[Collections.transactions].find({"_id": {"$in": imp_txs}})]
        results = {}
        for tx in txs:
            if not tx.account_transaction.effects.contract_update_issued:
                continue
            for effect in tx.account_transaction.effects.contract_update_issued.effects:
                if effect.updated:
                    print (effect.updated.receive_name)
                    if effect.updated.receive_name=="inventory.create":
                        cis.instance_index = contract_index
                        cis.instance_subindex = 0
                        cis.entrypoint = effect.updated.receive_name
                        cis.net = NET.MAINNET
                        hex = effect.updated.parameter
                        # print (tx.hash, hex)
                        parsed_result = None
                        if contract_index in erc721_v1_contracts:
                            version = "s7_erc721_v1"
                            parsed_result = cis.s7_inventory_create_erc721_v1(hex)
                            print (parsed_result)
                            # save_fake_cis2_token(contract_index, version, parsed_result, tx)
                        elif contract_index in erc721_v2_contracts:
                            version = "s7_erc721_v2"
                            parsed_result = cis.s7_inventory_create_erc721_v2_create_parameter(hex)
                            save_fake_cis2_token(contract_index, version, parsed_result, tx)
                        elif contract_index in erc_1155_v1_contracts:
                            version = "s7_erc1155_v1"
                            parsed_result = cis.s7_inventory_create_erc1155_v1_create_parameter(hex)
                            save_fake_cis2_token(contract_index, version, parsed_result, tx)
                        else:
                            pass
                        if parsed_result:
                            results[f"{contract_index}-{tx.hash}"] = {"contract_index":contract_index, "version": version, "tx_hash": tx.hash, "parsed_result": parsed_result.model_dump()}
        return results
    return (inventory_create,)


@app.cell
def _(inventory_create):
    inventory_create(378)
    return


@app.cell(hide_code=True)
def _(
    CCD_BlockItemSummary,
    Collections,
    NET,
    cis,
    erc721_v1_contracts,
    erc721_v2_contracts,
    erc_1155_v1_contracts,
    mongodb,
    save_fake_cis2_token,
):
    def trader_create_and_sell(contract_index: int):
        imp_txs = [x["tx_hash"] for x in mongodb.mainnet[Collections.impacted_addresses].find({"impacted_address_canonical":f"<{contract_index},0>"})]
        txs = [CCD_BlockItemSummary(**x) for x in mongodb.mainnet[Collections.transactions].find({"_id": {"$in": imp_txs}})]
        results = {}
        for tx in txs:
            if not tx.account_transaction.effects.contract_update_issued:
                continue
            for effect in tx.account_transaction.effects.contract_update_issued.effects:
                if effect.updated:
                    if effect.updated.receive_name=="inventory.create":
                        cis.instance_index = contract_index
                        cis.instance_subindex = 0
                        cis.entrypoint = effect.updated.receive_name
                        cis.net = NET.MAINNET
                        hex = effect.updated.parameter
                        print (tx.hash, hex)
                        parsed_result = None
                        if contract_index in erc721_v1_contracts:
                            version = "erc721_v1"
                            parsed_result = cis.s7_inventory_create_erc721_v1(hex)
                            # save_fake_cis2_token(version, parsed_result, tx)
                        elif contract_index in erc721_v2_contracts:
                            version = "erc721_v2"
                            parsed_result = cis.s7_inventory_create_erc721_v2(hex)
                            save_fake_cis2_token(version, parsed_result, tx)
                        elif contract_index in erc_1155_v1_contracts:
                            version = "erc1155_v1"
                            parsed_result = cis.s7_inventory_create_erc1155_v1(hex)
                            save_fake_cis2_token(version, parsed_result, tx)
                        else:
                            pass
                        if parsed_result:
                            results[f"{contract_index}-{tx.hash}"] = {"contract_index":contract_index, "version": version, "tx_hash": tx.hash, "parsed_result": parsed_result.model_dump()}
        return results
    return


@app.cell(hide_code=True)
def _(requests):
    def request_metadata(url: str):
        try:
            url = f"https://ipfs.io/ipfs/{url[7:]}"
            resp = requests.get(url, timeout=4)
            if resp.status_code == 200:
                t = resp.json()
                dd = {
                    "name": t["name"],
                    "unique": True,
                    "description": t["description"],
                    "thumbnail": {
                      "url": f"https://ipfs.io/ipfs/{t['image'][7:]}"
                    },
                    "display": {
                      "url": f"https://ipfs.io/ipfs/{t['image'][7:]}"
                    }
                  }
                # print (dd)
                return dd, url
            else:
                return None, url
        except Exception as e:
            print (e)
            return None, url
    return (request_metadata,)


@app.cell(hide_code=True)
def _(request_metadata):
    url = "ipfs://bafkreiefsijthnl7m5mtyl6wacgh62ozg7si5qyst5qkuhxb4ql7p7sc74"
    token_metadata = request_metadata(url)
    token_metadata
    return


@app.cell
def _(
    Collections,
    mongodb,
    request_metadata,
    s7_InventoryCreateParams_ERC721_V2,
):
    def save_fake_cis2_token(contract_index: int, version: str, result:s7_InventoryCreateParams_ERC721_V2, tx):
            url = result.url
            if len(url) < 10:
                    return None
            token_metadata, url = request_metadata(url)
            if not token_metadata:
                return None

            token_id = result.custom_token_id

            dd = {
                "_id": f"<{contract_index},0>-{token_id}",
                "contract": f"<{contract_index},0>",
                "token_id": str(token_id),
                "token_amount": 0,
                "metadata_url": url,
                "last_height_processed": tx.block_info.height,
                "special_type": version,
                "token_metadata": token_metadata,
                "mint_tx_hash": tx.hash
            }
            rr = mongodb.mainnet[Collections.tokens_token_addresses_v2].replace_one(
                {"_id": dd["_id"]},
                dd,
                upsert=True
            )
            # print (rr)
    return (save_fake_cis2_token,)


@app.cell
def _(Collections, mongodb):
    def add_contract_to_spaceseven_tag(contract_index:int):
        tag = "spaceseven"
        current_tag = mongodb.mainnet[Collections.tokens_tags].find_one({"_id": tag})
        if current_tag:
            current_contracts = set(current_tag["contracts"])
            current_contracts.add(f"<{contract_index},0>")
            current_tag["contracts"] = sorted(list(current_contracts))
            # print (current_tag["contracts"])
            rr = mongodb.mainnet[Collections.tokens_tags].replace_one(
                {"_id": current_tag["_id"]},
                current_tag,
                upsert=True
            )
        # print (current_tag)
    return (add_contract_to_spaceseven_tag,)


@app.cell
def _(add_contract_to_spaceseven_tag):
    add_contract_to_spaceseven_tag(2)
    return


@app.cell(disabled=True)
def _(
    add_contract_to_spaceseven_tag,
    erc721_v2_contracts,
    erc_1155_v1_contracts,
    inventory_create,
    mo,
):
    # all_contracts =  erc721_v1_contracts + erc721_v2_contracts  #erc_1155_v1_contracts #
    all_contracts =  erc_1155_v1_contracts
    all_contracts = erc721_v2_contracts
    # all_contracts = [0]
    results = {}
    for contract in mo.status.progress_bar(all_contracts):
        res = inventory_create(contract)
        results.update(res)
        add_contract_to_spaceseven_tag(contract)
    len(results)
    return (results,)


@app.cell
def _(pd, results):
    df = pd.json_normalize(results.values())
    df.to_csv("trader_create_and_sell_all_contracts_erc155_v1.csv", index=False)
    return (df,)


@app.cell
def _(df):
    f = df["parsed_result.url"].str.len() > 10
    len(df[f])
    return


@app.cell
def _(df):
    df.columns
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
