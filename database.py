import sqlite3
import csv
from datetime import datetime
from typing import Dict, Optional, List, Union, Any
import logging
from calculator import ProfitCalculator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, config=None):
        self.config = config
        self.db_file = config.get_db_config()['database_file'] if config else 'auction_data.db'
        self.conn = None
        self.cursor = None
        self._init_db()
    
    def _init_db(self):
        """Initialize the database and create necessary tables"""
        self.conn = sqlite3.connect(self.db_file)
        self.cursor = self.conn.cursor()
        
        # Create products table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                upc TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                brand TEXT,
                model TEXT,
                last_scraped_condition TEXT,
                last_scraped_functionality TEXT,
                last_scraped_damage TEXT,
                last_scraped_missing_items TEXT,
                last_scraped_damage_desc TEXT,
                last_scraped_missing_items_desc TEXT,
                last_scraped_notes TEXT,
                ebay_lowest_sold REAL,
                ebay_average_sold REAL,
                ebay_highest_sold REAL,
                ebay_lowest_listed REAL,
                ebay_average_listed REAL,
                ebay_highest_listed REAL,
                ebay_average_shipping REAL,
                ebay_active_listings_count INTEGER,
                amazon_price REAL,
                amazon_discount TEXT,
                amazon_star_rating REAL,
                amazon_reviews_count INTEGER,
                amazon_category_rating TEXT,
                amazon_subcategory_rating TEXT,
                amazon_sold_per_month INTEGER,
                amazon_frequently_returned INTEGER,
                grand_average_price REAL,
                recommended_highest_bid REAL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create auctions table (for tracking auction history)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS auctions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                title TEXT,
                auction_date TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create auction_items table (for tracking items in auctions)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS auction_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                auction_id INTEGER,
                upc TEXT,
                current_bid REAL,
                condition TEXT,
                functionality TEXT,
                damage TEXT,
                missing_items TEXT,
                damage_description TEXT,
                missing_items_description TEXT,
                notes TEXT,
                FOREIGN KEY (auction_id) REFERENCES auctions (id),
                FOREIGN KEY (upc) REFERENCES products (upc)
            )
        ''')
        
        self.conn.commit()
    
    def add_or_update_product(self, product_data: Dict[str, Any]) -> bool:
        """
        Add a new product or update existing based on UPC.
        Merges HiBid scraped data with existing research data.
        
        Args:
            product_data: Dictionary containing product information
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Prepare the SQL statement
            columns = []
            values = []
            placeholders = []
            
            for key, value in product_data.items():
                if value is not None:  # Skip None values
                    columns.append(key)
                    values.append(value)
                    placeholders.append('?')
            
            # Check if product exists
            self.cursor.execute('SELECT upc FROM products WHERE upc = ?', (product_data.get('upc'),))
            exists = self.cursor.fetchone() is not None
            
            if exists:
                # Update existing product
                update_sql = f'''
                    UPDATE products 
                    SET {', '.join(f'{col} = ?' for col in columns)}
                    WHERE upc = ?
                '''
                self.cursor.execute(update_sql, values + [product_data.get('upc')])
            else:
                # Insert new product
                insert_sql = f'''
                    INSERT INTO products ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                '''
                self.cursor.execute(insert_sql, values)
            
            self.conn.commit()
            return True
            
        except sqlite3.Error as e:
            logger.error(f"Error adding/updating product: {str(e)}")
            return False
    
    def get_product_by_upc(self, upc: str) -> Optional[Dict[str, Any]]:
        """Get product by UPC"""
        try:
            self.cursor.execute('SELECT * FROM products WHERE upc = ?', (upc,))
            row = self.cursor.fetchone()
            if row:
                columns = [description[0] for description in self.cursor.description]
                return dict(zip(columns, row))
            return None
        except sqlite3.Error as e:
            logger.error(f"Error getting product by UPC: {str(e)}")
            return None
    
    def get_product_by_name(self, name: str) -> List[Dict[str, Any]]:
        """Get products by name (partial match)"""
        try:
            self.cursor.execute('SELECT * FROM products WHERE name LIKE ?', (f'%{name}%',))
            rows = self.cursor.fetchall()
            columns = [description[0] for description in self.cursor.description]
            return [dict(zip(columns, row)) for row in rows]
        except sqlite3.Error as e:
            logger.error(f"Error getting products by name: {str(e)}")
            return []
    
    def get_product_by_brand_model(self, brand: str, model: str) -> List[Dict[str, Any]]:
        """Get products by brand and model"""
        try:
            self.cursor.execute(
                'SELECT * FROM products WHERE brand = ? AND model = ?',
                (brand, model)
            )
            rows = self.cursor.fetchall()
            columns = [description[0] for description in self.cursor.description]
            return [dict(zip(columns, row)) for row in rows]
        except sqlite3.Error as e:
            logger.error(f"Error getting products by brand/model: {str(e)}")
            return []
    
    def needs_research(self, upc: str) -> bool:
        """
        Check if a product needs research (missing key eBay/Amazon data)
        
        Returns:
            bool: True if research is needed, False otherwise
        """
        try:
            self.cursor.execute('''
                SELECT 
                    ebay_lowest_sold,
                    ebay_average_sold,
                    ebay_highest_sold,
                    amazon_price,
                    amazon_star_rating
                FROM products 
                WHERE upc = ?
            ''', (upc,))
            
            row = self.cursor.fetchone()
            if not row:
                return True  # Product not found
            
            # Check if any key research data is missing
            return any(value is None for value in row)
            
        except sqlite3.Error as e:
            logger.error(f"Error checking research needs: {str(e)}")
            return True
    
    def update_research_data(self, upc: str, research_data: Dict[str, Any]) -> bool:
        """
        Update research data for a product
        
        Args:
            upc: Product UPC
            research_data: Dictionary containing research data
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Prepare update statement
            columns = []
            values = []
            
            for key, value in research_data.items():
                if value is not None:
                    columns.append(key)
                    values.append(value)
            
            if not columns:
                return False
            
            update_sql = f'''
                UPDATE products 
                SET {', '.join(f'{col} = ?' for col in columns)},
                    last_updated = CURRENT_TIMESTAMP
                WHERE upc = ?
            '''
            
            self.cursor.execute(update_sql, values + [upc])
            self.conn.commit()
            return True
            
        except sqlite3.Error as e:
            logger.error(f"Error updating research data: {str(e)}")
            return False
    
    def export_to_csv(self, auction_data: List[Dict[str, Any]], filename: str) -> bool:
        """
        Export auction data to CSV with research and calculation results.
        
        Args:
            auction_data: List of dictionaries containing current auction data
            filename: Path to output CSV file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Define CSV columns
            fieldnames = [
                "Lot Number",
                "Current Bid",
                "Name",
                "Brand",
                "Model",
                "UPC",
                "Condition",
                "Functionality",
                "Damage?",
                "Missing Items?",
                "Damage Description",
                "Missing Item Description",
                "Notes",
                "Grand Average Price",
                "Average Shipping Price",
                "Recommended Highest Bid Amount",
                "Current Profit Margin",
                "Flagged"
            ]
            
            # Open CSV file for writing
            with open(filename, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                # Process each item
                for item in auction_data:
                    # Get full product data from database
                    product = self.get_product_by_upc(item.get('upc'))
                    if not product:
                        logger.warning(f"Product not found in database for UPC: {item.get('upc')}")
                        continue
                    
                    # Calculate current profit margin
                    calculator = ProfitCalculator(self.config)
                    current_margin = calculator.calculate_current_profit_margin(
                        current_bid=item.get('current_bid', 0),
                        grand_average_price=product.get('grand_average_price'),
                        config=self.config
                    )
                    
                    # Prepare row data
                    row = {
                        "Lot Number": item.get('lot_number', ''),
                        "Current Bid": f"${item.get('current_bid', 0):.2f}",
                        "Name": item.get('name', ''),
                        "Brand": item.get('brand', ''),
                        "Model": item.get('model', ''),
                        "UPC": item.get('upc', ''),
                        "Condition": item.get('condition', ''),
                        "Functionality": item.get('functionality', ''),
                        "Damage?": "Yes" if item.get('damage', False) else "No",
                        "Missing Items?": "Yes" if item.get('missing_items', False) else "No",
                        "Damage Description": item.get('damage_description', ''),
                        "Missing Item Description": item.get('missing_item_description', ''),
                        "Notes": item.get('notes', ''),
                        "Grand Average Price": f"${product.get('grand_average_price', 0):.2f}" if product.get('grand_average_price') else '',
                        "Average Shipping Price": f"${product.get('ebay_average_shipping', 0):.2f}" if product.get('ebay_average_shipping') else '',
                        "Recommended Highest Bid Amount": f"${product.get('recommended_highest_bid', 0):.2f}" if product.get('recommended_highest_bid') else '',
                        "Current Profit Margin": f"{current_margin:.1f}%" if current_margin is not None else '',
                        "Flagged": "Yes" if product.get('amazon_frequently_returned', 0) else "No"
                    }
                    
                    writer.writerow(row)
            
            logger.info(f"Successfully exported data to {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting to CSV: {str(e)}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
    
    def get_products_needing_research(self) -> List[Dict[str, Any]]:
        """
        Get all products that need research (missing key eBay/Amazon data or old data)
        
        Returns:
            List of dictionaries containing product data
        """
        try:
            self.cursor.execute('''
                SELECT * FROM products 
                WHERE 
                    ebay_lowest_sold IS NULL OR
                    ebay_average_sold IS NULL OR
                    ebay_highest_sold IS NULL OR
                    amazon_price IS NULL OR
                    amazon_star_rating IS NULL OR
                    last_updated < datetime('now', '-7 days')
            ''')
            
            columns = [description[0] for description in self.cursor.description]
            products = []
            
            for row in self.cursor.fetchall():
                product = dict(zip(columns, row))
                products.append(product)
            
            return products
            
        except sqlite3.Error as e:
            logger.error(f"Error getting products needing research: {str(e)}")
            return [] 