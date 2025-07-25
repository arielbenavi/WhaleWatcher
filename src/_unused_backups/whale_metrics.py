from pathlib import Path
import pandas as pd
import logging
from datetime import datetime, timedelta


class WhaleMetricsCalculator:
    def __init__(self, base_dir: str = "data"):
        self.base_dir = Path(base_dir)
        self.logger = logging.getLogger(__name__)
        
        # Create metrics directory
        self.metrics_dir = self.base_dir / "processed" / "wallet_metrics"
        self.metrics_dir.mkdir(parents=True, exist_ok=True)
        
        # Load current BTC price
        self.current_btc_price = self._load_current_btc_price()
        
    def _load_current_btc_price(self) -> float:  # Move this inside the class
        """Load current BTC price (last entry from price file)"""
        price_file = self.base_dir / "raw" / "price" / "BTC_USD_Bitfinex_Investing_com.csv"
        try:
            df = pd.read_csv(price_file)
            last_price = df['Price'].iloc[-1]
            return float(last_price)
        except (FileNotFoundError, IndexError, ValueError) as e:
            self.logger.error(f"Error loading current BTC price: {str(e)}")
            return 0
        
    def calculate_wallet_metrics(self, wallet_address: str) -> dict:
        """Calculate performance metrics for a single wallet"""
        # Load processed transactions
        transactions_file = self.base_dir / "processed" / "transactions" / f"{wallet_address}.csv"
        if not transactions_file.exists():
            self.logger.warning(f"No transaction data found for {wallet_address}")
            return {}
            
        df = pd.read_csv(transactions_file)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Calculate basic metrics
        metrics = {
            'wallet_address': wallet_address,
            'total_trades': len(df),
            'total_volume_btc': df['amount_btc'].abs().sum(),
            'avg_position_size': df['portfolio_pct'].mean(),
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Calculate ROI for different timeframes
        metrics.update(self._calculate_roi_metrics(df))
        
        # Additional trading patterns
        metrics.update(self._calculate_trading_patterns(df))
        
        return metrics
    
    def _calculate_roi_metrics(self, df: pd.DataFrame) -> dict:
        """Calculate ROI using net investment method"""
        print("🚨 DEBUG: Using whale_metrics.py - WhaleMetricsCalculator")
        self.logger.info("🚨 DEBUG: whale_metrics.py is processing ROI")

        metrics = {}
        
        # Get all buys and sells
        buys = df[df['transaction_type'] == 'buy']
        sells = df[df['transaction_type'] == 'sell']
        
        if not buys.empty:
            # Calculate total money put in (USD)
            total_money_in = (buys['amount_btc'] * buys['price_usd']).sum() if 'price_usd' in df.columns else 0
            
            # Calculate total money taken out (USD)
            total_money_out = (sells['amount_btc'].abs() * sells['price_usd']).sum() if not sells.empty and 'price_usd' in df.columns else 0
            
            # Net investment (what they actually have at risk)
            net_investment = total_money_in - total_money_out
            
            # Calculate current portfolio value
            current_balance = df.iloc[-1]['balance_btc']
            current_value = current_balance * self.current_btc_price
            
            # Calculate ROI based on net investment
            if net_investment > 0:
                roi = ((current_value - net_investment) / net_investment) * 100
                metrics['roi_overall'] = roi
            else:
                # If net investment is 0 or negative, they've taken out more than they put in
                metrics['roi_overall'] = 0  # Or could be infinity, but 0 is safer
                
            # Store additional metrics for debugging
            metrics['total_money_in'] = total_money_in
            metrics['total_money_out'] = total_money_out
            metrics['net_investment'] = net_investment
            metrics['current_value'] = current_value
            
            # DEBUG: Print the calculations
            print(f"Total money in: ${total_money_in:,.2f}")
            print(f"Total money out: ${total_money_out:,.2f}")
            print(f"Net investment: ${net_investment:,.2f}")
            print(f"Current balance: {current_balance:.8f} BTC")
            print(f"Current BTC price: ${self.current_btc_price:,.2f}")
            print(f"Current value: ${current_value:,.2f}")

        else:
            metrics['roi_overall'] = 0
            
        return metrics
    
    def _calculate_trading_patterns(self, df: pd.DataFrame) -> dict:
        """Calculate various trading pattern metrics"""
        metrics = {}
        
        if not df.empty:
            # Win rate calculation
            trades = df[df['transaction_type'].isin(['buy', 'sell'])]
            if not trades.empty:
                profitable_trades = len(trades[trades['transaction_value_usd'] > 0])
                metrics['win_rate'] = (profitable_trades / len(trades)) * 100
            
            # Calculate average hold time
            buy_trades = df[df['transaction_type'] == 'buy']
            sell_trades = df[df['transaction_type'] == 'sell']
            
            if not buy_trades.empty and not sell_trades.empty:
                avg_hold_time = (sell_trades['timestamp'].mean() - buy_trades['timestamp'].mean()).days
                metrics['avg_hold_time_days'] = avg_hold_time
        
        return metrics

    def update_all_wallets(self):
        """Update metrics for all wallets"""
        # Get all transaction files
        transaction_dir = self.base_dir / "processed" / "transactions"
        wallet_files = transaction_dir.glob("*.csv")
        
        # Calculate metrics for each wallet
        all_metrics = []
        for wallet_file in wallet_files:
            wallet_address = wallet_file.stem
            metrics = self.calculate_wallet_metrics(wallet_address)
            if metrics:
                all_metrics.append(metrics)
        
        # Save to CSV
        if all_metrics:
            metrics_df = pd.DataFrame(all_metrics)
            output_file = self.metrics_dir / "wallet_stats.csv"
            metrics_df.to_csv(output_file, index=False)
            self.logger.info(f"Updated metrics for {len(all_metrics)} wallets")
