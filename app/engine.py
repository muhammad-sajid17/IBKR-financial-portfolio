import os
import time
import xml.etree.ElementTree as ET
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
import requests

CONFIG = [
    {
        "owner": "Maha",
        "token": "240508230866489854230236",
        "query_id": "1607540",
    },
    {
        "owner": "Ghalya",
        "token": "233528787537176523145924",
        "query_id": "1607580",
    },
    {
        "owner": "Ameena",
        "token": "198985636346164004513062",
        "query_id": "1607623",
    },
    {
        "owner": "Hamad",
        "token": "47844748042797202423808",
        "query_id": "1607632",
    },
]

TABLE_DEFINITIONS = {
    "AccountInformation": "AccountInformation",
    "ChangeInNAV": "ChangeInNAV",
    "OpenPositions": "OpenPosition",
    "Trades_Executions": "Trade",
    "Trades_Orders": "Order",
}


def fetch_ibkr_data(token, query_id, owner):
    send_url = f"https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest?t={token}&q={query_id}&v=3"
    response = requests.get(send_url)
    root = ET.fromstring(response.content)

    if root.find("Status").text != "Success":
        err = root.find("ErrorMessage").text
        raise Exception(f"IBKR Error for {owner}: {err}")

    ref_code = root.find("ReferenceCode").text
    time.sleep(15)  # Respect IBKR generation delay

    get_url = f"https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/GetStatement?q={ref_code}&t={token}&v=3"
    data_response = requests.get(get_url)
    return data_response.content


def parse_statement(xml_bytes, owner_name):
    root = ET.fromstring(xml_bytes)
    statements = root.findall(".//FlexStatement")
    records_by_table = {table_key: [] for table_key in TABLE_DEFINITIONS}

    for stmt in statements:
        stmt_acc_id = stmt.attrib.get("accountId", "")
        for elem in stmt.iter():
            for table_key, xml_tag in TABLE_DEFINITIONS.items():
                if elem.tag == xml_tag and elem.attrib:
                    row = dict(elem.attrib)
                    row["Owner"] = owner_name
                    if not row.get("accountId"):
                        row["accountId"] = stmt_acc_id
                    records_by_table[table_key].append(row)
    return records_by_table


def execute_sync(spreadsheet_url: str, credentials_path: str = "service_account.json"):
    aggregated_data = {table_key: [] for table_key in TABLE_DEFINITIONS}

    print("🚀 Starting IBKR Fetch Pipeline...")
    
    # --- PHASE 1: FETCH (All or Nothing) ---
    for item in CONFIG:
        try:
            print(f"Fetching data for {item['owner']}...")
            xml_bytes = fetch_ibkr_data(
                item["token"], item["query_id"], item["owner"]
            )
            
            if not xml_bytes:
                raise Exception("API returned an empty response.")
                
            parsed = parse_statement(xml_bytes, item["owner"])
            for table_key, rows in parsed.items():
                aggregated_data[table_key].extend(rows)
                
        except Exception as e:
            # 🛑 HALT EXECUTION: If any account fails, abort completely.
            error_msg = f"Sync aborted! Failed on {item['owner']}. Reason: {str(e)}"
            print(f"❌ {error_msg}")
            print("🛑 No changes were made to Google Sheets.")
            
            # Return the error back to FastAPI so Power Automate knows it failed
            return {"status": "error", "message": error_msg}
            
        time.sleep(15)  # Strict 15-second buffer to avoid the 1018 rate limit

    # --- PHASE 2: UPLOAD (Only runs if ALL accounts succeeded) ---
    print("\n🔗 All 4 accounts fetched successfully. Connecting to Google Sheets...")
    gc = gspread.service_account(filename=credentials_path)
    sh = gc.open_by_url(spreadsheet_url)

    for table_name, rows in aggregated_data.items():
        if not rows:
            continue
            
        df = pd.DataFrame(rows)
        lead_cols = [
            c
            for c in [
                "Owner",
                "accountId",
                "acctAlias",
                "symbol",
                "reportDate",
                "tradeDate",
                "transactionID",
            ]
            if c in df.columns
        ]
        other_cols = [c for c in df.columns if c not in lead_cols]
        df = df[lead_cols + other_cols]

        worksheet = sh.worksheet(table_name)

        if table_name in ["Trades_Executions", "Trades_Orders"]:
            existing_df = get_as_dataframe(worksheet).dropna(how="all")
            if not existing_df.empty:
                combined = pd.concat([existing_df, df], ignore_index=True)
                dedup_keys = (
                    ["transactionID"]
                    if "transactionID" in combined.columns
                    else ["Owner", "accountId", "tradeDate", "symbol"]
                )
                df = combined.drop_duplicates(subset=dedup_keys, keep="last")

        worksheet.clear()
        set_with_dataframe(worksheet, df)
        print(f"✅ Updated tab: {table_name}")

    return {"status": "success", "message": "All Google Sheets updated successfully."}