import unittest
from src.data.collector import WhaleDataCollector
from src.data.storage import DataStorage
import pandas as pd
import logging

class TestRichlist(unittest.TestCase):
    """Tests for richlist collection functionality"""
    
    def setUp(self):
        self.collector = WhaleDataCollector()
        self.storage = DataStorage()
        logging.getLogger().setLevel(logging.ERROR)  # Reduce noise during tests
    
    def test_richlist_format(self):
        """Test if richlist data has correct format"""
        richlist_df = self.collector.get_richlist_wallets(pages=1)
        
        # Check structure
        self.assertIsInstance(richlist_df, pd.DataFrame)
        expected_columns = ['btc_address', 'last_in', 'last_out']
        self.assertTrue(all(col in richlist_df.columns for col in expected_columns))
        
        # Check content
        self.assertFalse(richlist_df.empty)
        self.assertTrue(len(richlist_df) > 0)
        
        # Check if addresses look valid (basic check)
        first_address = richlist_df['btc_address'].iloc[0]
        self.assertTrue(first_address.startswith(('1', '3', 'bc1')))

class TestWalletActivity(unittest.TestCase):
    """Tests for wallet transaction collection functionality"""
    
    def setUp(self):
        self.collector = WhaleDataCollector()
        self.storage = DataStorage()
        logging.getLogger().setLevel(logging.ERROR)
        
        # Known test wallet addresses
        self.test_addresses = {
            'active_whale': "1LGAVQYU52QemGhbV3SQmy8oLmDwAs7QJu",
            'large_holder': "1JtAupan5MSPXxSsWFiwA79bY9LD2Ga1je",
            'exchange_cold': "34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo"
        }
    
    def test_transaction_format(self):
        """Test if transaction data has correct format"""
        # Test with one known active address
        transactions_df = self.collector.get_wallet_transactions(self.test_addresses['active_whale'])
        
        # Check structure
        self.assertIsInstance(transactions_df, pd.DataFrame)
        expected_columns = ['hash', 'time', 'result', 'balance']  # Add more expected columns
        self.assertTrue(all(col in transactions_df.columns for col in expected_columns))
        
        # Check content
        self.assertFalse(transactions_df.empty)
    
    def test_data_storage(self):
        """Test if data is correctly saved and loaded"""
        address = self.test_addresses['active_whale']
        
        # Get and save data
        transactions_df = self.collector.get_wallet_transactions(address)
        self.storage.save_wallet_transactions(address, transactions_df)
        
        # Load and verify
        loaded_df = self.storage.get_wallet_transactions(address)
        self.assertEqual(len(transactions_df), len(loaded_df))
        
    def test_api_handling(self):
        """Test API edge cases"""
        # Test with invalid address
        invalid_address = "invalid_address_123"
        with self.assertRaises(Exception):
            self.collector.get_wallet_transactions(invalid_address)

def run_quick_test():
    """Run a quick test with one wallet"""
    collector = WhaleDataCollector()
    storage = DataStorage()
    
    # Test wallet
    test_address = "bc1qm34lsc65zpw79lxes69zkqmk6ee3ewf0j77s3h"
    
    print(f"Testing with wallet: {test_address}")
    transactions_df = collector.get_wallet_transactions(test_address)
    print(f"Found {len(transactions_df)} transactions")
    print("\nSample of data columns:", transactions_df.columns.tolist()[:5])
    print("\nFirst transaction:", transactions_df.iloc[0].to_dict())
    
    # Save data
    storage.save_wallet_transactions(test_address, transactions_df)
    print(f"\nData saved to: data/raw/transactions/{test_address}.parquet")

if __name__ == '__main__':
    # Option 1: Run all tests
    # unittest.main()
    
    # Option 2: Run quick test
    run_quick_test()


'''

Organized Test Classes:

TestRichlist for richlist functionality
TestWalletActivity for transaction collection


Different Types of Tests:

Format validation
Content validation
Storage functionality
API error handling


Quick Test Option:

run_quick_test() for quick manual testing
Shows sample of the data



You can use it in different ways:

Run all tests:

bashCopypython -m unittest tests/test_collector.py

Run quick test:

bashCopypython tests/test_collector.py

Run specific test class:

bashCopypython -m unittest tests/test_collector.py -k TestRichlist 
'''