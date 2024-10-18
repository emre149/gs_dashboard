import pandas as pd
import numpy as np
import os
import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv

load_dotenv()

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = '/Users/emre/Downloads/omega-ether-438918-e8-55c40c86661e.json'
SPREADSHEET_ID = os.getenv('SHEET_ID')

def setup_webdriver():
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.binary_location = '/Applications/Brave Browser.app/Contents/MacOS/Brave Browser'
    service = Service('/Users/emre/Desktop/dev/Side Projects/gs_dashboard/chromedriver')
    return webdriver.Chrome(service=service, options=chrome_options)

def setup_google_sheets_service():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build('sheets', 'v4', credentials=creds)

def get_wallet_data(driver, wallet_address):
    if pd.isna(wallet_address):
        return None, None

    url = f"{os.getenv('ENDPOINT')}{wallet_address}/"
    driver.get(url)
    
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'pre')))
        json_content = driver.find_element(By.TAG_NAME, 'pre').text
        data = json.loads(json_content)
        
        if data.get('code') == 0:
            return data['data'].get('pnl_7d'), data['data'].get('winrate')
        else:
            print(f"Erreur pour {wallet_address}: {data.get('msg')}")
            return None, None
    except Exception as e:
        print(f"Erreur lors du traitement de {wallet_address}: {str(e)}")
        return None, None

def fetch_and_process_data(sheet_service):
    result = sheet_service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range='A1:F').execute()
    values = result.get('values', [])
    headers, data = values[0], values[1:]
    return pd.DataFrame(data, columns=headers)

def update_wallet_data(df, driver):
    df['Extracted Wallets'] = df['Wallets Solana'].str.replace(r'https://gmgn\.ai/sol/address/', '', regex=True)
    
    for idx, row in df.iterrows():
        pnl_7d, winrate = get_wallet_data(driver, row['Extracted Wallets'])
        if pnl_7d is not None:
            df.at[idx, '7D/PNL'] = f"{pnl_7d*100:.2f}%"
        if winrate is not None:
            df.at[idx, 'Winrate'] = f"{winrate*100:.2f}%"
    
    return df

def prepare_data_for_update(df):
    df = df.drop(['Number *', 'Score'], axis=1, errors='ignore')
    df = df.rename(columns={'Wallets': 'Wallets Solana'})
    df['7D/PNL_numeric'] = df['7D/PNL'].str.rstrip('%').astype('float') / 100
    df_sorted = df.sort_values(by='7D/PNL_numeric', ascending=False)
    df_sorted = df_sorted.drop(['7D/PNL_numeric', 'Extracted Wallets'], axis=1)
    new_column_order = ['Name of Wallet', 'Wallets Solana', 'Winrate', '7D/PNL']
    return df_sorted[new_column_order]

def update_google_sheet(sheet_service, df):
    update_values = [df.columns.tolist()] + df.values.tolist()
    result = sheet_service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID, 
        range='A1',
        valueInputOption='USER_ENTERED',
        body={'values': update_values}
    ).execute()
    print(f"{result.get('updatedCells')} cells updated.")

def main():
    driver = setup_webdriver()
    sheet_service = setup_google_sheets_service()
    
    try:
        df = fetch_and_process_data(sheet_service)
        df = update_wallet_data(df, driver)
        df = prepare_data_for_update(df)
        update_google_sheet(sheet_service, df)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()