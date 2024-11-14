from pathlib import Path
import pandas as pd
from datetime import datetime
import logging

class DataStorage:
    def __init__(self, base_dir: str = "data"):
        self.base_dir = Path(base_dir)
        self.setup_storage()
        self.logger = logging.getLogger(__name__)

    def setup_storage(self):
        """Create necessary directories"""
        # Raw data directories
        (self.base_dir / "raw" / "transactions").mkdir(parents=True, exist_ok=True)
        (self.base_dir / "raw" / "richlist").mkdir(parents=True, exist_ok=True)
        
        # Processed data directories
        (self.base_dir / "processed" / "transactions").mkdir(parents=True, exist_ok=True)
        (self.base_dir / "processed" / "metrics").mkdir(parents=True, exist_ok=True)

    def save_richlist(self, data: pd.DataFrame):
        """Save richlist data"""
        timestamp = datetime.now().strftime('%Y%m%d')
        filename = f"richlist_{timestamp}.csv"
        filepath = self.base_dir / "raw" / "richlist" / filename
        
        data.to_csv(filepath, index=False)
        self.logger.info(f"Saved richlist to {filepath}")

    def save_wallet_transactions(self, address: str, data: pd.DataFrame):
        """Save wallet transactions"""
        if data.empty:
            self.logger.warning(f"No transactions to save for {address}")
            return
            
        filename = f"{address}.csv"
        filepath = self.base_dir / "raw" / "transactions" / filename
        
        data.to_csv(filepath, index=False)
        self.logger.info(f"Saved {len(data)} transactions for {address} to {filepath}")

    def get_wallet_transactions(self, address: str) -> pd.DataFrame:
        """Load wallet transactions"""
        filepath = self.base_dir / "raw" / "transactions" / f"{address}.csv"
        
        if not filepath.exists():
            return pd.DataFrame()
            
        return pd.read_csv(filepath)

    def get_latest_richlist(self) -> pd.DataFrame:
        """Get most recent richlist data"""
        richlist_dir = self.base_dir / "raw" / "richlist"
        files = list(richlist_dir.glob("richlist_*.csv"))
        
        if not files:
            return pd.DataFrame()
            
        latest_file = max(files, key=lambda x: x.stem.split('_')[1])
        return pd.read_csv(latest_file)