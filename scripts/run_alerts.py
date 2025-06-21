#!/usr/bin/env python3
import argparse
import logging
from src.analysis.whale_alerts import WhaleAlertMonitor
from pathlib import Path

def setup_logging():
    """Setup logging configuration"""

    # Create logs directory if it doesn't exist
    logs_dir = Path('logs')
    logs_dir.mkdir(exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/alerts.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Run whale alert monitoring system")
    parser.add_argument('--hours', type=int, default=24,
                      help='Number of hours to look back for alerts (default: 24)')
    parser.add_argument('--no-alert', action='store_true',
                      help='Run without sending actual alerts')
    
    args = parser.parse_args()
    logger = setup_logging()
    
    logger.info(f"Starting alert monitoring for past {args.hours} hours")
    
    try:
        # Initialize monitor with alert toggle
        monitor = WhaleAlertMonitor(alert_enabled=not args.no_alert)
        
        # Check transactions in specified timeframe
        monitor.check_timeframe(hours=args.hours)
        
        logger.info("Alert monitoring completed successfully")
        
    except Exception as e:
        logger.error(f"Error during alert monitoring: {str(e)}")
        raise

if __name__ == "__main__":
    main()