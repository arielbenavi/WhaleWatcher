from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
import logging

class DataProcessor:
    def __init__(self, base_dir: str = "data"):
        self.base_dir = Path(base_dir)
        self.logger = logging.getLogger(__name__)
        self.price_data = self._load_price_data()
    
    def _load_price_data(self) -> pd.DataFrame:
        """Load BTC-USD price data"""
        price_file = self.base_dir / "raw" / "price" / "BTC_USD_Bitfinex_Investing_com.csv"
        try:
            df = pd.read_csv(price_file)
            df['Date'] = pd.to_datetime(df['Date'])
            # Convert price to numeric, removing any commas
            df['Price'] = pd.to_numeric(df['Price'].astype(str).str.replace(',', ''), errors='coerce')
            return df.set_index('Date')
        except FileNotFoundError:
            self.logger.error(f"Price data file not found at {price_file}")
            raise
            
    def process_wallet_transactions(self, wallet_address: str) -> pd.DataFrame:
        """Process raw transaction data for a wallet"""
        raw_file = self.base_dir / "raw" / "transactions" / f"{wallet_address}.csv"
        if not raw_file.exists():
            self.logger.warning(f"No data found for wallet {wallet_address}")
            return pd.DataFrame()
        
        df = pd.read_csv(raw_file)
        
        # Process into standardized format with last_updated timestamp
        processed = pd.DataFrame({
            'wallet_address': wallet_address,
            'timestamp': pd.to_datetime(df['time'], unit='s'),
            'hash': df['hash'],
            'amount_btc': df['result'] / 100000000,
            'balance_btc': df['balance'] / 100000000,
            'fee': df['fee'] / 100000000 if 'fee' in df.columns else None,
            'block_height': df['block_height'],
            'last_updated': datetime.now()  # Add this column
        })
        
        # Match with price data and ensure numeric type
        processed['price_usd'] = None  
        for idx, row in processed.iterrows():
            date = row['timestamp'].strftime('%Y-%m-%d')
            if date in self.price_data.index:
                processed.at[idx, 'price_usd'] = float(self.price_data.loc[date, 'Price'])
        
        # Calculate additional metrics with error handling
        processed['transaction_value_usd'] = processed.apply(
            lambda row: float(row['amount_btc']) * float(row['price_usd']) 
            if pd.notnull(row['amount_btc']) and pd.notnull(row['price_usd']) 
            else None, axis=1
        )
        
        processed['transaction_type'] = processed['amount_btc'].apply(
            lambda x: 'buy' if x > 0 else 'sell' if x < 0 else 'transfer'
        )
        
        # Calculate portfolio percentage with error handling
        processed['portfolio_pct'] = processed.apply(
            lambda row: (abs(float(row['amount_btc'])) / float(row['balance_btc'])) * 100 
            if pd.notnull(row['amount_btc']) and pd.notnull(row['balance_btc']) and row['balance_btc'] != 0
            else None, axis=1
        )
        
        # Sort by timestamp
        processed = processed.sort_values('timestamp')
        
        # Log basic info
        self.logger.info(f"Processed {len(processed)} transactions for {wallet_address}")
        
        # Save processed data
        output_file = self.base_dir / "processed" / "transactions" / f"{wallet_address}.csv"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        processed.to_csv(output_file, index=False)
        
        return processed

    def process_all_wallets(self):
        """Process all wallets in raw data"""
        raw_dir = self.base_dir / "raw" / "transactions"
        for file in raw_dir.glob("*.csv"):
            wallet_address = file.stem
            self.logger.info(f"\nProcessing wallet: {wallet_address}")
            try:
                self.process_wallet_transactions(wallet_address)
            except Exception as e:
                self.logger.error(f"Error processing wallet {wallet_address}: {e}")
                self.logger.exception("Full error trace:")
                continue