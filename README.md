# Bitcoin Whale Transaction Monitor

A Python-based system that monitors and analyzes Bitcoin whale wallet activities, providing real-time Telegram alerts for significant trading movements. The system analyzes transaction patterns, portfolio changes, and performance metrics to identify and categorize different types of whale behavior.

## Features

- **Data Collection**: Automated collection of whale wallet transactions using blockchain APIs
- **Transaction Analysis**: 
  - Portfolio percentage impact calculation
  - Wallet categorization (New, Recent, Established)
  - Trading frequency analysis
  - Performance metrics calculation
- **Alert System**:
  - Configurable alert thresholds based on portfolio impact
  - Different alert levels (INFO, HIGH, URGENT)
  - Telegram notifications with detailed context
  - Wallet type classification with historical performance

## Proxy
you can get free 1 week trial at oculus-proxy. i personally favor oxylab's pay as you go 8$ per gb for low frequancy requests volume.


## TODO:
1. update btc/usd price data to existing csv somehow
2. add to assumptions when you run the script midday you will get transaction data

## Installation

```bash
# Clone repository
git clone <repository-url>

# Install requirements
pip install -r requirements.txt

# Set up configuration
- Add Telegram bot token
- Configure alert thresholds
```

## Usage

```bash
# Collect whale transaction data
python scripts/run_collection.py

# Process and analyze transactions
python scripts/process_data.py

# Run alerts (24h timeframe)
python scripts/run_alerts.py --hours 24
```

## Project Structure
```
bitcoin-whale-analysis/
├── src/
│   ├── data/            # Data collection and processing
│   └── analysis/        # Analysis and alert system
├── scripts/             # Execution scripts
├── data/
│   ├── raw/            # Raw transaction data
│   └── processed/      # Processed data and metrics
```



# Assumptions
BTC_USD_Bitfinex_Investing_com contains price data from 2012 until end of 2024 and from then it is being collected (appended) via coingecko's free API with each run_collection.py job.

## Data Collection
- Blockchain.info API is available and maintains its current endpoint structure
- API rate limits allow for requests every 0.8s (at least)
- Transaction amounts from API are in satoshis (needs division by 100000000 for BTC)
- Exchange wallets are identified by "wallet" text in rich list

## Data Processing
- Price data exists at "data/raw/price/BTC_USD_Bitfinex_Investing_com.csv"
- Transaction types can be determined by amount direction:
  - Positive amount = buy
  - Negative amount = sell
- All timestamps are in UTC
- Portfolio percentage uses end-of-transaction balance

## Metrics Calculation
- Trading pattern thresholds:
  - Significant sell = 1% of current balance
  - Active trader = 10+ trades per month
  - Holding pattern = 30+ days
  - Pattern recognition requires 3+ significant sells
- First transaction represents entry point for ROI
- Portfolio value calculated using daily close prices
- Raw transaction data is properly processed before metrics calculation

## Alert System
- Alert thresholds:
  - Urgent: Portfolio changes ≥ 10%
  - High priority: Portfolio changes ≥ 5%
  - Info: Portfolio changes ≥ 0.2%
- Bot token and chat ID are valid
- Message formatting (HTML) is supported by Telegram

## Error Handling
- Failed API calls should be retried
- Failed processing of one wallet shouldn't stop entire process
- Network interruptions are temporary

## Testing
- Test wallets remain active and accessible
- Test data represents real-world scenarios
- Test wallet addresses remain valid
- Logging can be suppressed during tests



LAST THING CHANGED: gitignore to include oxylabs proxy creds. before that successfull e2e run.