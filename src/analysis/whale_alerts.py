import pandas as pd
from pathlib import Path
import requests
from datetime import datetime, timedelta
import logging
import socket
from src.config.credentials import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID


class WhaleAlertMonitor:
    def __init__(self, base_dir="data", alert_enabled=True):
        self.base_dir = Path(base_dir)
        self.alert_enabled = alert_enabled
        self.token_id = TELEGRAM_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.logger = logging.getLogger(__name__)
        
        # Load wallet stats for context
        self.wallet_stats = self._load_wallet_stats()
    
    def telegram_noti(self, text: str):
        """Your existing telegram notification function"""
        if not self.alert_enabled:
            return
            
        msg = "Automatic message from host: \n" \
              "<b>MESSAGE: </b>\n<pre> {text} </pre>".format(text=text)
        
        payload = {
            'chat_id': self.chat_id,
            'text': msg,
            'parse_mode': 'HTML'
        }
        requests.post(f"https://api.telegram.org/bot{self.token_id}/sendMessage", data=payload)
    
    def _load_wallet_stats(self):
        """Load wallet performance stats"""
        stats_file = self.base_dir / "processed" / "wallet_metrics" / "all_wallets_summary.csv"
        self.logger.info(f"Looking for wallet stats at: {stats_file}")
        
        if stats_file.exists():
            df = pd.read_csv(stats_file)
            self.logger.info(f"Found stats file with {len(df)} wallets")
            return df
        
        self.logger.warning(f"Stats file not found at {stats_file}")
        return pd.DataFrame()
    
    def is_notable_transaction(self, transaction, wallet_stats=None):
        """Determine if transaction needs alert"""
        portfolio_pct = transaction['portfolio_pct']
        
        if portfolio_pct >= 10:
            return "🚨 URGENT"
        elif portfolio_pct >= 5:
            return "⚠️ HIGH"
        elif portfolio_pct >= 0.2:
            return "ℹ️ INFO"
        return None

    def check_timeframe(self, hours=24):
        """Alert on transactions in last X hours"""
        now = datetime.now()
        cutoff = now - timedelta(hours=hours)
        
        # Get all processed transaction files
        trans_dir = self.base_dir / "processed" / "transactions"
        self.logger.info(f"Checking transactions in: {trans_dir}")
        
        for trans_file in trans_dir.glob("*.csv"):
            try:
                df = pd.read_csv(trans_file)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                
                # Filter recent transactions
                recent_txs = df[df['timestamp'] >= cutoff]
                
                if not recent_txs.empty:
                    self.logger.info(f"Found {len(recent_txs)} recent transactions in {trans_file.name}")
                
                for _, tx in recent_txs.iterrows():
                    alert_level = self.is_notable_transaction(tx)
                    if alert_level:
                        # Get wallet stats if available
                        wallet_stats = None
                        if not self.wallet_stats.empty:
                            matching_stats = self.wallet_stats[
                                self.wallet_stats['wallet_address'] == tx['wallet_address']
                            ]
                            if not matching_stats.empty:
                                wallet_stats = matching_stats.iloc[0]
                                self.logger.info(f"Found stats for wallet {tx['wallet_address']}")
                            else:
                                self.logger.warning(f"No stats found for wallet {tx['wallet_address']}")
                        
                        # Format message
                        message = self.format_alert(tx, wallet_stats, alert_level)
                        self.telegram_noti(message)
                        
            except Exception as e:
                self.logger.error(f"Error processing file {trans_file.name}: {str(e)}")
                continue
    
    def format_alert(self, transaction, wallet_stats, alert_level):
        """Format alert message with wallet context"""
        action = "Bought" if transaction['amount_btc'] > 0 else "Sold"
        amount = abs(transaction['amount_btc'])
        
        # Base alert message
        msg = (
            f"{alert_level}\n"
            f"Wallet: {transaction['wallet_address']}\n"
            f"Action: {action} {amount:.2f} BTC\n"
            f"Portfolio: {transaction['portfolio_pct']:.1f}%\n"
        )
        
        # Add wallet context if stats are available
        if wallet_stats is not None:
            msg += "\nWallet Profile:"
            
            # Add rank if available
            if pd.notnull(wallet_stats.get('rank')):
                msg += f"\n• Rank: #{int(wallet_stats['rank'])}"
            
            # Trading behavior
            msg += f"\n• Type: {wallet_stats['trader_type']}"
            msg += f"\n• Active Days: {wallet_stats['active_days']}"
            
            # Performance metrics
            if pd.notnull(wallet_stats.get('roi_overall')):
                msg += f"\n• Overall ROI: {wallet_stats['roi_overall']:.1f}%"
            
            # Trading frequency
            trades_per_month = wallet_stats.get('trades_per_month')
            if pd.notnull(trades_per_month):
                msg += f"\n• Activity: {trades_per_month:.1f} trades/month"
            
            # PnL if available
            if pd.notnull(wallet_stats.get('realized_pnl_usd')):
                pnl = wallet_stats['realized_pnl_usd']
                msg += f"\n• Realized PnL: ${pnl:,.2f}"
            
            # Current balance for context
            if pd.notnull(wallet_stats.get('current_balance_btc')):
                msg += f"\n• Current Balance: {wallet_stats['current_balance_btc']:.2f} BTC"
        
        return msg