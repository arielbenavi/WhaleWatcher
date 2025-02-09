from pathlib import Path
import pandas as pd
import logging
from datetime import datetime, timedelta

class WhaleMetricsCalculator:
    def __init__(self, base_dir: str = "data"):
        self.base_dir = Path(base_dir)
        self.logger = logging.getLogger(__name__)
        
        # Ensure metrics directory exists
        self.metrics_dir = self.base_dir / "processed" / "wallet_metrics"
        self.metrics_dir.mkdir(parents=True, exist_ok=True)
        
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
        """Calculate ROI using cost basis method"""
        metrics = {}
        
        # Get all buys and sells
        buys = df[df['transaction_type'] == 'buy']
        sells = df[df['transaction_type'] == 'sell']
        
        if not buys.empty:
            # Calculate total cost basis (sum of all buy amounts)
            total_invested = (buys['amount_btc'] * buys['price_usd']).sum() if 'price_usd' in df else buys['amount_btc'].sum()
            
            # Calculate current value
            current_balance = df.iloc[-1]['balance_btc']
            current_price = df.iloc[-1]['price_usd'] if 'price_usd' in df else 1  # Use 1 if no price data
            current_value = current_balance * current_price
            
            # Calculate realized gains from sells
            realized_gains = (sells['amount_btc'].abs() * sells['price_usd']).sum() if 'price_usd' in df else sells['amount_btc'].abs().sum()
            
            # Total return includes both realized gains and current holdings
            total_return = realized_gains + current_value
            
            # Calculate ROI
            if total_invested > 0:
                roi = ((total_return - total_invested) / total_invested) * 100
                metrics['roi_overall'] = roi
                metrics['total_invested'] = total_invested
                metrics['current_value'] = current_value
                metrics['realized_gains'] = realized_gains
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
