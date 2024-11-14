from src.data.collector import WhaleDataCollector
from src.data.storage import DataStorage
import logging
import argparse
import pandas as pd

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/sample_run.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def run_sample(addresses):
    logger = setup_logging()
    
    collector = WhaleDataCollector()
    storage = DataStorage()
    
    logger.info(f"Starting sample run with wallets: {addresses}")
    
    for address in addresses:
        try:
            logger.info(f"\nProcessing wallet: {address}")
            transactions_df = collector.get_wallet_transactions(address)
            
            if not transactions_df.empty:
                logger.info(f"Found {len(transactions_df)} transactions")
                logger.info(f"Data columns: {transactions_df.columns.tolist()}")
                logger.info("\nFirst transaction preview:")
                logger.info(transactions_df.iloc[0][['hash', 'time', 'result', 'balance']].to_dict())
                
                storage.save_wallet_transactions(address, transactions_df)
                logger.info(f"Saved to data/raw/transactions/{address}.parquet")
            else:
                logger.warning(f"No transactions found for {address}")
                
        except Exception as e:
            logger.error(f"Error processing wallet {address}: {e}")
            continue

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Collect Bitcoin wallet transactions')
    parser.add_argument('addresses', nargs='*', default=[
        "1LGAVQYU52QemGhbV3SQmy8oLmDwAs7QJu"  # Default test wallet
    ], help='Bitcoin addresses to collect transactions for')
    
    args = parser.parse_args()
    run_sample(args.addresses)