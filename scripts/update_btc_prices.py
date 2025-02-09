#!/usr/bin/env python3
import logging
from src.data.BTC_price_collector import BTCPriceCollector

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/BTC_price_collection.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def main():
    logger = setup_logging()
    
    try:
        logger.info("Starting BTC price data update")
        collector = BTCPriceCollector()
        collector.update_price_data()
        logger.info("Price data update completed successfully")
        
    except Exception as e:
        logger.error(f"Error during price data update: {str(e)}")
        raise

if __name__ == "__main__":
    main()