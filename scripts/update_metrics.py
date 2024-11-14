from src.analysis.whale_metrics import WhaleMetricsCalculator
import logging

def setup_logging():
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
