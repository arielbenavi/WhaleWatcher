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
        self.richlist = self._load_richlist()

    def _load_richlist(self) -> pd.DataFrame:
        """Load latest richlist data"""
        richlist_dir = self.base_dir / "raw" / "richlist"
        
        # First check if directory exists
        if not richlist_dir.exists():
            self.logger.warning(f"Richlist directory not found: {richlist_dir}")
            return pd.DataFrame()
        
        # Get list of files and log what we found
        files = list(richlist_dir.glob("richlist_*.csv"))
        self.logger.info(f"Found richlist files: {[f.name for f in files]}")
        
        if not files:
            self.logger.warning("No richlist files found")
            return pd.DataFrame()
            
        # Get latest file by comparing date in filename
        latest_file = max(files, key=lambda x: x.stem.split('_')[1])
        self.logger.info(f"Using latest richlist file: {latest_file}")
        
        try:
            df = pd.read_csv(latest_file)
            self.logger.info(f"Loaded richlist with {len(df)} rows")
            return df
        except Exception as e:
            self.logger.error(f"Error loading richlist file: {str(e)}")
            return pd.DataFrame()
    
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
        
        if not self.richlist.empty:
            matching_rows = self.richlist[self.richlist['btc_address'] == wallet_address]
            # self.logger.info(f"Found {len(matching_rows)} matching rows in richlist for {wallet_address}")
            if not matching_rows.empty:
                wallet_rank = matching_rows['rank'].iloc[0]
                # self.logger.info(f"Found rank {wallet_rank} for wallet {wallet_address}")
            else:
                self.logger.warning(f"No rank found for wallet {wallet_address} in richlist")
        else: self.logger.warning("Richlist is empty")

        # Process into standardized format with last_updated timestamp
        processed = pd.DataFrame({
            'wallet_address': wallet_address,
            'timestamp': pd.to_datetime(df['time'], unit='s'),
            'hash': df['hash'],
            'amount_btc': df['result'] / 100000000,
            'balance_btc': df['balance'] / 100000000,
            'fee': df['fee'] / 100000000 if 'fee' in df.columns else None,
            'block_height': df['block_height'],
            'last_updated': datetime.now()
        })

        # Add wallet ranking according to richlist, Make more efficient
        if not self.richlist.empty:
            matching_rows = self.richlist[self.richlist['btc_address'] == wallet_address]
            if not matching_rows.empty:
                processed['rank'] = matching_rows['rank'].iloc[0]
            else:
                processed['rank'] = 999  # Former richlist wallet, now >100
                self.logger.warning(f"Wallet {wallet_address} not in current richlist, assigning rank 999")
        else:
            processed['rank'] = 999
            self.logger.warning("Richlist is empty, assigning default rank 999")
        
        # Match with price data and ensure numeric type
        processed['price_usd'] = None  
        for idx, row in processed.iterrows():
            date = row['timestamp'].strftime('%Y-%m-%d')
            if date in self.price_data.index:
                price_values = self.price_data.loc[date, 'Price']
                # Handle multiple entries for same date - take the first one
                if isinstance(price_values, pd.Series):
                    processed.at[idx, 'price_usd'] = float(price_values.iloc[0])
                else:
                    processed.at[idx, 'price_usd'] = float(price_values)
        
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
        
        # Add date-based aggregation
        processed['date'] = processed['timestamp'].dt.date

        # Aggregate transactions by date
        # July 16th 2025 - removed , 'rank' due to keyerror (float?)
        # processed['rank'] = processed['rank'].fillna(0)
        daily_aggregated = processed.groupby(['wallet_address', 'date', 'rank']).agg({
            'timestamp': ['first', 'last'],
            'hash': lambda x: ','.join(x),
            'amount_btc': 'sum',
            'balance_btc': 'last',
            'fee': 'sum',
            'block_height': ['first', 'last'],
            'last_updated': 'max',
            'price_usd': 'mean',
            'transaction_value_usd': 'sum'
        }).reset_index()

        # Flatten multi-level columns
        daily_aggregated.columns = [
            'wallet_address', 'date', 'rank',
            'first_timestamp', 'last_timestamp',
            'transactions', 'amount_btc', 'balance_btc',
            'total_fee', 'first_block', 'last_block',
            'last_updated', 'price_usd', 'transaction_value_usd'
        ]
        
        # Recalculate transaction type based on net daily movement
        daily_aggregated['transaction_type'] = daily_aggregated['amount_btc'].apply(
            lambda x: 'buy' if x > 0 else 'sell' if x < 0 else 'transfer'
        )
        
        # Recalculate portfolio percentage based on net daily movement
        daily_aggregated['portfolio_pct'] = daily_aggregated.apply(
            lambda row: (abs(float(row['amount_btc'])) / float(row['balance_btc'])) * 100 
            if pd.notnull(row['amount_btc']) and pd.notnull(row['balance_btc']) and row['balance_btc'] != 0
            else None, axis=1
        )
        
        # Convert date back to timestamp for consistency
        daily_aggregated['timestamp'] = pd.to_datetime(daily_aggregated['date'])
        daily_aggregated = daily_aggregated.drop('date', axis=1)
        
        # Sort by timestamp
        daily_aggregated = daily_aggregated.sort_values('timestamp')
        
        # Add log for aggregation results
        self.logger.info(f"Consolidated into {len(daily_aggregated)} daily records for {wallet_address}")
        
        # Save processed data
        output_file = self.base_dir / "processed" / "transactions" / f"{wallet_address}.csv"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        daily_aggregated.to_csv(output_file, index=False)
        
        return daily_aggregated

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