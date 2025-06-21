import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data.collector import WhaleDataCollector
from src.data.BTC_price_collector import BTCPriceCollector
from src.data.storage import DataStorage
import logging


def setup_logging():

    # Create logs directory if it doesn't exist
    logs_dir = Path('logs')
    logs_dir.mkdir(exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/whale_tracker.log'),
            logging.StreamHandler()
        ]
    )

def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Update BTC USD price data appending on bitfenix file
        logger.info("Updating BTC price data")
        price_collector = BTCPriceCollector()
        price_collector.update_price_data()
    
        collector = WhaleDataCollector()
        storage = DataStorage()
        
        # Collect and store richlist
        richlist_df = collector.get_richlist_wallets(pages=1)
        storage.save_richlist(richlist_df)
        
        # Collect transaction data for each wallet
        for address in richlist_df['btc_address']:
            try:
                transactions_df = collector.get_wallet_transactions(address)
                if not transactions_df.empty:
                    storage.save_wallet_transactions(address, transactions_df)
            except Exception as e:
                logger.error(f"Error processing wallet {address}: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Error in data collection: {str(e)}")

if __name__ == "__main__":
    main()