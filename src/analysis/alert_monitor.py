import pandas as pd
from pathlib import Path
import requests
from datetime import datetime, timedelta
import logging
import socket

class WhaleAlertMonitor:
    def __init__(self, base_dir="data", alert_enabled=True):
        self.base_dir = Path(base_dir)
        self.alert_enabled = alert_enabled
        self.token_id = '5339164296:AAHe8_P50HQa_SCybmMeFC51ggIWoQDuQhI'
        self.chat_id = '1546739282'
        self.logger = logging.getLogger(__name__)
        self.wallet_stats = self._load_wallet_stats()
        
    def check_transactions(self, mode='last', hours=None):
        """
        Check transactions based on mode:
        - 'last': Only transactions since last update
        - 'hours': Transactions in last X hours
        """
        now = datetime.now()
        trans_dir = self.base_dir / "processed" / "transactions"
        
        for trans_file in trans_dir.glob("*.csv"):
            wallet_address = trans_file.stem
            df = pd.read_csv(trans_file)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['last_updated'] = pd.to_datetime(df['last_updated'])
            
            # Filter transactions based on mode
            if mode == 'hours' and hours:
                cutoff = now - timedelta(hours=hours)
                transactions_to_check = df[df['timestamp'] >= cutoff]
            else:  # 'last' mode
                last_update = df['last_updated'].max()
                transactions_to_check = df[df['last_updated'] == last_update]
            
            if not transactions_to_check.empty:
                self.logger.info(f"Checking {len(transactions_to_check)} transactions for {wallet_address}")
                self._process_transactions(transactions_to_check, wallet_address)
    
    def _process_transactions(self, transactions, wallet_address):
        """Process and alert on transactions"""
        for _, tx in transactions.iterrows():
            alert_level = self.is_notable_transaction(tx)
            if alert_level:
                wallet_stats = self.get_wallet_stats(wallet_address)
                message = self.format_alert(tx, wallet_stats, alert_level)
                self.telegram_noti(message)
    
    def format_alert(self, transaction, wallet_stats, alert_level):
        """Format alert message with context"""
        action = "Bought" if transaction['amount_btc'] > 0 else "Sold"
        amount = abs(transaction['amount_btc'])
        
        msg = (
            f"{alert_level}\n"
            f"Wallet: {transaction['wallet_address']}\n"
            f"Action: {action} {amount:.2f} BTC\n"
            f"Portfolio: {transaction['portfolio_pct']:.1f}%\n"
        )
        
        if wallet_stats is not None:
            msg += f"Wallet ROI: {wallet_stats['roi_overall']:.1f}%\n"
            
        return msg
