import sys; print("DEBUG: METRICS.PY - File loaded", file=sys.stderr)

from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

class WalletMetricsCalculator:
    def __init__(self, base_dir: str = "data"):
        self.base_dir = Path(base_dir)
        self.logger = logging.getLogger(__name__)
        
        # Define thresholds
        self.SIGNIFICANT_SELL_THRESHOLD = 0.01  # 1% of current balance
        self.TRADE_FREQUENCY_THRESHOLD = 10  # trades per month for active trader
        self.HOLDING_PERIOD_THRESHOLD = 30  # days to consider holding

        # Load current BTC price
        self.current_btc_price = self._load_current_btc_price()
    
    def _load_current_btc_price(self) -> float:
        """Load current BTC price (last entry from price file)"""
        price_file = self.base_dir / "raw" / "price" / "BTC_USD_Bitfinex_Investing_com.csv"
        try:
            df = pd.read_csv(price_file)
            # Handle price column that might have commas
            df['Price'] = pd.to_numeric(df['Price'].astype(str).str.replace(',', ''), errors='coerce')
            last_price = df['Price'].iloc[-1]
            return float(last_price)
        except (FileNotFoundError, IndexError, ValueError) as e:
            self.logger.error(f"Error loading current BTC price: {str(e)}")
            return 0

    def calculate_wallet_metrics(self, wallet_address: str) -> dict:
        """Calculate comprehensive metrics for a wallet"""
        transactions_file = self.base_dir / "processed" / "transactions" / f"{wallet_address}.csv"
        if not transactions_file.exists():
            self.logger.warning(f"No transaction data found for {wallet_address}")
            return {}
            
        df = pd.read_csv(transactions_file)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Basic metrics
        metrics = {
            'wallet_address': wallet_address,
            'rank': df['rank'].iloc[0] if 'rank' in df.columns else None,
            'first_transaction': df['timestamp'].min().strftime('%Y-%m-%d'),
            'last_transaction': df['timestamp'].max().strftime('%Y-%m-%d'),
            'total_transactions': len(df),
            'active_days': (df['timestamp'].max() - df['timestamp'].min()).days,
            'current_balance_btc': df.iloc[-1]['balance_btc'],
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Trading pattern metrics
        pattern_metrics = self._analyze_trading_pattern(df)
        metrics.update(pattern_metrics)
        
        # Performance metrics
        performance_metrics = self._calculate_performance_metrics(df)
        metrics.update(performance_metrics)
        
        # Save metrics
        self._save_wallet_metrics(wallet_address, metrics)
        
        return metrics
    
    def _analyze_trading_pattern(self, df: pd.DataFrame) -> dict:
        """Analyze trading patterns and categorize wallet behavior"""
        trades = df[df['transaction_type'].isin(['buy', 'sell'])]
        buys = trades[trades['transaction_type'] == 'buy']
        sells = trades[trades['transaction_type'] == 'sell']
        
        # Calculate trade counts and volumes
        metrics = {
            'buy_count': len(buys),
            'sell_count': len(sells),
            'total_buy_volume': buys['amount_btc'].abs().sum(),
            'total_sell_volume': sells['amount_btc'].abs().sum(),
            'avg_buy_size': buys['amount_btc'].abs().mean() if not buys.empty else 0,
            'avg_sell_size': sells['amount_btc'].abs().mean() if not sells.empty else 0,
        }
        
        # Calculate trading frequency (trades per month)
        date_range = (df['timestamp'].max() - df['timestamp'].min()).days / 30
        trades_per_month = len(trades) / date_range if date_range > 0 else 0
        metrics['trades_per_month'] = trades_per_month
        
        # Analyze significant sells
        significant_sells = sells[sells['portfolio_pct'] >= self.SIGNIFICANT_SELL_THRESHOLD]
        metrics['significant_sells_count'] = len(significant_sells)
        
        # Determine trader type
        metrics['trader_type'] = self._determine_trader_type(
            trades_per_month,
            metrics['significant_sells_count'],
            df['timestamp'].max() - df['timestamp'].min()
        )
        
        return metrics
    
    def _calculate_performance_metrics(self, df: pd.DataFrame) -> dict:
        """Calculate detailed performance metrics"""
        metrics = {}
        
        # Get all buys and sells
        buys = df[df['transaction_type'] == 'buy']
        sells = df[df['transaction_type'] == 'sell']
        
        if not buys.empty and 'price_usd' in df.columns:
            # Calculate net investment (money in - money out)
            total_money_in = (buys['amount_btc'] * buys['price_usd']).sum()
            total_money_out = (sells['amount_btc'].abs() * sells['price_usd']).sum() if not sells.empty else 0
            net_investment = total_money_in - total_money_out
            
            # Current portfolio value
            current_balance = df.iloc[-1]['balance_btc']
            current_value = current_balance * self.current_btc_price
            wallet_address = df.iloc[0]['wallet_address'] if 'wallet_address' in df.columns else "unknown"
            print(f"🔢 ROI DEBUG for {wallet_address}:", file=sys.stderr)
            # DEBUG: Print the calculations
            print(f"Total money in: ${total_money_in:,.2f}")
            print(f"Total money out: ${total_money_out:,.2f}")
            print(f"Net investment: ${net_investment:,.2f}")
            print(f"Current balance: {current_balance:.8f} BTC")
            print(f"Current BTC price: ${self.current_btc_price:,.2f}")
            print(f"Current value: ${current_value:,.2f}")

            # ROI calculation
            if net_investment > 0:
                roi = ((current_value - net_investment) / net_investment) * 100
                metrics['roi_overall'] = roi
            else:
                metrics['roi_overall'] = 0
        else:
            metrics['roi_overall'] = 0

        # Calculate realized PnL from trades
        trades = df[df['transaction_type'].isin(['buy', 'sell'])]
        if not trades.empty:
            realized_pnl = trades.apply(
                lambda x: x['amount_btc'] * x['price_usd'] if pd.notnull(x['price_usd']) else 0, 
                axis=1
            ).sum()
            metrics['realized_pnl_usd'] = realized_pnl
            
        # Calculate volatility of portfolio value
        if 'price_usd' in df.columns:
            portfolio_values = df['balance_btc'] * df['price_usd']
            if len(portfolio_values) > 1 and portfolio_values.mean() > 0:
                metrics['portfolio_value_volatility'] = portfolio_values.std() / portfolio_values.mean() * 100
            
        # Calculate max drawdown
        if 'price_usd' in df.columns:
            portfolio_values = df['balance_btc'] * df['price_usd']
            if len(portfolio_values) > 1:
                rolling_max = portfolio_values.expanding().max()
                drawdowns = (portfolio_values - rolling_max) / rolling_max * 100
                metrics['max_drawdown_pct'] = abs(drawdowns.min())
        
        return metrics
    
    def _determine_trader_type(self, trades_per_month: float, significant_sells: int, 
                             activity_period: timedelta) -> str:
        """Determine the type of trader based on activity patterns"""
        if activity_period.days < self.HOLDING_PERIOD_THRESHOLD:
            return 'New Wallet'
            
        if trades_per_month >= self.TRADE_FREQUENCY_THRESHOLD and significant_sells >= 3:
            return 'Active Trader'
        elif significant_sells >= 1:
            return 'Occasional Trader'
        else:
            return 'Holder'
    
    def _save_wallet_metrics(self, wallet_address: str, metrics: dict):
        """Save metrics to a CSV file"""
        output_dir = self.base_dir / "processed" / "wallet_metrics"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save individual wallet metrics
        wallet_file = output_dir / f"{wallet_address}.csv"
        pd.DataFrame([metrics]).to_csv(wallet_file, index=False)
        
        # Update or append to summary file
        summary_file = output_dir / "all_wallets_summary.csv"
        if summary_file.exists():
            summary_df = pd.read_csv(summary_file)
            summary_df = summary_df[summary_df['wallet_address'] != wallet_address]
            summary_df = pd.concat([summary_df, pd.DataFrame([metrics])], ignore_index=True)
        else:
            summary_df = pd.DataFrame([metrics])
            
        summary_df.to_csv(summary_file, index=False)
        
    def process_all_wallets(self):
        """Process metrics for all wallets with transaction data"""
        transaction_dir = self.base_dir / "processed" / "transactions"
        for file in transaction_dir.glob("*.csv"):
            wallet_address = file.stem
            try:
                self.calculate_wallet_metrics(wallet_address)
                self.logger.info(f"Processed metrics for {wallet_address}")
            except Exception as e:
                self.logger.error(f"Error processing metrics for {wallet_address}: {str(e)}")