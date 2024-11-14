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
python scripts/run_alerts.py --mode hours --hours 24
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
