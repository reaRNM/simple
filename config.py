class Config:
    def __init__(self):
        # Default configuration values
        self.settings = {
            'texas_sales_tax': 0.0825,  # 8.25%
            'auction_buyer_premium': 0.15,  # 15%
            'ship_to_me_cost': 0.0,
            'ebay_seller_fee': 0.40,
            'ebay_listing_fee': 0.13,  # 13%
            'promote_listing_fee': 0.02,  # 2%
            'lowest_profit_margin': 0.35  # 35%
        }
        
        # Database configuration
        self.db_config = {
            'database_file': 'auction_data.db'
        }
        
        # Scraping configuration
        self.scraping_config = {
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'request_timeout': 30,
            'max_retries': 3
        }
    
    def get(self, key):
        """Get a configuration value"""
        return self.settings.get(key)
    
    def set(self, key, value):
        """Set a configuration value"""
        if key in self.settings:
            self.settings[key] = value
            return True
        return False
    
    def get_db_config(self):
        """Get database configuration"""
        return self.db_config
    
    def get_scraping_config(self):
        """Get scraping configuration"""
        return self.scraping_config 