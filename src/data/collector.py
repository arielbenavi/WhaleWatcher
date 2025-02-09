import pandas as pd
import time
from pathlib import Path
from datetime import datetime
import logging
import requests
import random
from bs4 import BeautifulSoup
from typing import List, Dict, Optional

class WhaleDataCollector:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def get_richlist_wallets(self, pages: int = 1) -> pd.DataFrame:
        """Fetch top Bitcoin wallet addresses from bitinfocharts"""
        df = pd.DataFrame({'btc_address': [], 'last_in': [], 'last_out': []})
        
        for page in range(1, pages + 1):
            self.logger.info(f'Extracting richlist wallets: page {page}')
            btc_richlist_url = f'https://bitinfocharts.com/top-100-richest-bitcoin-addresses-{page}.html'
            
            try:
                response = requests.get(btc_richlist_url, headers=self._get_headers())
                df = pd.concat([df, self._extract_richlist_wallets(response)])
                time.sleep(2)  # Rate limiting
            except Exception as e:
                self.logger.error(f"Error fetching page {page}: {str(e)}")
                
        # Delete old richlist files before saving new one
        richlist_dir = Path("data/raw/richlist")
        for old_file in richlist_dir.glob("richlist_*.csv"):
            old_file.unlink()
            self.logger.info(f"Deleted old richlist file: {old_file}")

        return df

    def get_wallet_transactions(self, address: str) -> pd.DataFrame:
        """Fetch complete transaction history for a wallet using blockchain.info API"""
        self.logger.info(f'Fetching transactions for {address}')
        
        transactions = []
        offset = 0
        limit = 50  # Maximum allowed by the API
        last_hash = None

        while True:
            try:
                self.logger.info(f'Fetching batch at offset {offset}')
                time.sleep(0.8)  # Rate limiting
                
                proxy = self._get_proxy()
                url = f"https://blockchain.info/rawaddr/{address}?offset={offset}&limit={limit}"
                response = requests.get(url, proxies=proxy)
                
                if response.status_code != 200:
                    self.logger.error(f"API error: {response.text}")
                    break
                
                data = response.json()
                txs = data.get("txs", [])
                
                # Check for duplicates
                if last_hash and any(tx["hash"] == last_hash for tx in txs):
                    self.logger.info("Reached duplicate transactions, stopping")
                    transactions_dict = {tx["hash"]: tx for tx in transactions + txs}
                    transactions = list(transactions_dict.values())
                    break

                transactions.extend(txs)
                if txs:
                    last_hash = txs[-1]["hash"]

                if len(txs) < limit:
                    self.logger.info("All transactions received")
                    break
                
                offset += limit

            except Exception as e:
                self.logger.error(f"Error fetching transactions: {str(e)}")
                break

        return pd.json_normalize(transactions)

    # def _get_proxy(self) -> Dict[str, str]:
    #     """Get proxy configuration"""
    #     session_number = random.randint(1, 9999)
    #     proxy = f"http://oc-71c84685df30a37ce15a54edb5bc6d50967b7cc0554c18637b06868a74118326-country-FR-session-{session_number}:sp51985direv@proxy.oculus-proxy.com:31111"
    #     return {"http": proxy, "https": proxy}

    def _get_proxy(self) -> Dict[str, str]:
        """Get proxy configuration using Oxylabs proxy credentials"""
        from src.config.credentials import OXYLABS_USERNAME, OXYLABS_PASSWORD
        
        proxy = f"http://{OXYLABS_USERNAME}:{OXYLABS_PASSWORD}@pr.oxylabs.io:7777"
        
        return {"http": proxy, "https": proxy}

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers"""
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        ]
        return {"User-Agent": random.choice(user_agents)}


    def _extract_richlist_wallets(self, response) -> pd.DataFrame:
        """Parse richlist page and extract wallet information"""
        df = pd.DataFrame({'rank': [], 'btc_address': [], 'last_in': [], 'last_out': []})
        soup = BeautifulSoup(response.text, 'lxml')
        
        # Find and parse both tables
        table_1 = soup.find('table', class_='table table-striped abtb')
        table_1 = table_1.find('tbody') if table_1 else None
        table_2 = soup.find('table', class_='table table-striped bb')
        
        wallets_soup = []
        if table_1:
            wallets_soup.extend(table_1.find_all('tr'))
        if table_2:
            wallets_soup.extend(table_2.find_all('tr'))
            
        for wallet in wallets_soup:
            try:
                # Skip exchange wallets
                is_exchange = wallet.find('small')
                if is_exchange and is_exchange.find('a') and 'wallet' in is_exchange.find('a').text:
                    continue
                    
                # Extract address and timestamps
                address = wallet.find('a', href=True)
                address = address['href'].replace('https://bitinfocharts.com/bitcoin/address/', '')
                
                timestamps = wallet.find_all('td', class_='utc hidden-tablet')
                last_in = timestamps[1].text if timestamps[1].text else None
                last_out = timestamps[-1].text if timestamps[-1].text else None
                
                df.loc[len(df)] = [len(df) + 1, address, last_in, last_out]
                
            except Exception as e:
                self.logger.error(f"Error parsing wallet entry: {str(e)}")
                continue
                
        return df

    def cleanup(self):
        """Close browser and clean up resources"""
        if self.browser:
            self.browser.quit()
