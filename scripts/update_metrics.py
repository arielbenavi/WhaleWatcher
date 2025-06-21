from src.analysis.whale_metrics import WhaleMetricsCalculator
import logging
from pathlib import Path

def setup_logging():

    logs_dir = Path('logs')
    logs_dir.mkdir(exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def main():
    setup_logging()
    calculator = WhaleMetricsCalculator()
    calculator.update_all_wallets()

if __name__ == "__main__":
    main()
