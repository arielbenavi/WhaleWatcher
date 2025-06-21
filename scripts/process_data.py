from src.data.processor import DataProcessor
from src.data.metrics import WalletMetricsCalculator
import logging
from pathlib import Path


def setup_logging():
    
    # Create logs directory if it doesn't exist
    logs_dir = Path('logs')
    logs_dir.mkdir(exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/processing.log'),
            logging.StreamHandler()
        ]
    )

def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        processor = DataProcessor()
        processor.process_all_wallets()
        logger.info("Processing complete")

        # Calculate metrics
        logger.info("Starting wallets metrics calculation...")
        metrics_calculator = WalletMetricsCalculator()
        metrics_calculator.process_all_wallets()
        logger.info("Wallet metrics calculation complete")
        
    except Exception as e:
        logger.error(f"Error in data processing: {str(e)}")

if __name__ == "__main__":
    main()
