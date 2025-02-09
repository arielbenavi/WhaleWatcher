from pathlib import Path
import pandas as pd
import requests
from datetime import datetime, timedelta
import logging
import time

class BTCPriceCollector:
    def __init__(self, base_dir: str = "data"):
        self.base_dir = Path(base_dir)
        self.logger = logging.getLogger(__name__)
        self.price_file = self.base_dir / "raw" / "price" / "BTC_USD_Bitfinex_Investing_com.csv"
        
    def update_price_data(self):
        """Update BTC-USD price data using CoinGecko API"""
        try:
            self.logger.info("Starting price data update process")
            
            # Load existing data if available
            if self.price_file.exists():
                self.logger.debug(f"Loading existing price file: {self.price_file}")
                existing_df = pd.read_csv(self.price_file)
                existing_df['Date'] = pd.to_datetime(existing_df['Date'])
                # Convert Price to numeric, removing any commas
                existing_df['Price'] = pd.to_numeric(existing_df['Price'].astype(str).str.replace(',', ''), errors='coerce')
                last_date = existing_df['Date'].max()
                self.logger.info(f"Found existing data with last date: {last_date}")
            else:
                self.logger.info("No existing price file found - starting fresh")
                existing_df = pd.DataFrame()
                last_date = datetime.now() - timedelta(days=30)
            
            # Calculate days to fetch
            days_to_fetch = (datetime.now() - last_date).days
            self.logger.info(f"Fetching {days_to_fetch} days of price data")
            
            if days_to_fetch <= 0:
                self.logger.info("Price data is already up to date")
                return
            
            # Fetch new data from CoinGecko
            url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
            params = {
                'vs_currency': 'usd',
                'days': str(days_to_fetch + 1),
                'interval': 'daily'
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            # Process new data
            data = response.json()
            prices = data['prices']
            
            new_data = []
            for timestamp_ms, price in prices:
                date = datetime.fromtimestamp(timestamp_ms / 1000)
                new_data.append({
                    'Date': date.strftime('%Y-%m-%d'),
                    'Price': float(price),  # Ensure price is float
                    'Open': float(price),
                    'High': float(price),
                    'Low': float(price),
                    'Vol.': '0',
                    'Change %': '0'
                })
            
            new_df = pd.DataFrame(new_data)
            new_df['Date'] = pd.to_datetime(new_df['Date'])
            
            # Combine with existing data
            if not existing_df.empty:
                new_df = new_df[new_df['Date'] > last_date]
                final_df = pd.concat([existing_df, new_df])
            else:
                final_df = new_df
            
            # Ensure Price is numeric before calculation
            final_df['Price'] = pd.to_numeric(final_df['Price'], errors='coerce')
            
            # Calculate Change %
            final_df['Change %'] = final_df['Price'].pct_change() * 100
            
            # Sort by date
            final_df = final_df.sort_values('Date')
            
            # Ensure directory exists
            self.price_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Save to CSV
            final_df.to_csv(self.price_file, index=False)
            self.logger.info(f"Successfully saved updated price data to {self.price_file}")
            
            # Sleep to respect rate limits
            time.sleep(1)
            
        except Exception as e:
            self.logger.error(f"Error updating price data: {str(e)}", exc_info=True)
            raise