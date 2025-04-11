import sqlite3
import csv
from datetime import datetime
from typing import Dict, Optional, List, Union, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, config=None):
        self.config = config
        self.conn = None
        self.cursor = None
        self._init_db()
    
    def _init_db(self):
        """Initialize the database and create tables if they don't exist"""
        try:
            self.conn = sqlite3.connect('auction_data.db')
            self.cursor = self.conn.cursor()
            
            # Create products table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS products (
                    upc TEXT PRIMARY KEY,
                    name TEXT,
                    brand TEXT,
                    model TEXT,
                    last_scraped_condition TEXT,
                    last_scraped_functionality TEXT,
                    last_scraped_damage BOOLEAN,
                    last_scraped_missing_items BOOLEAN,
                    last_scraped_damage_desc TEXT,
                    last_scraped_missing_items_desc TEXT,
                    last_scraped_notes TEXT,
                    ebay_lowest_sold REAL,
                    ebay_average_sold REAL,
                    ebay_highest_sold REAL,
                    ebay_average_listed REAL,
                    ebay_average_shipping REAL,
                    amazon_price REAL,
                    amazon_star_rating REAL,
                    amazon_reviews_count INTEGER,
                    amazon_frequently_returned BOOLEAN,
                    grand_average_price REAL,
                    recommended_highest_bid REAL,
                    current_profit_margin REAL,
                    last_updated TIMESTAMP
                )
            ''')
            
            # Create auctions table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS auctions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE,
                    title TEXT,
                    date TEXT
                )
            ''')
            
            # Create auction_items table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS auction_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    auction_id INTEGER,
                    lot_number TEXT,
                    current_bid REAL,
                    upc TEXT,
                    FOREIGN KEY (auction_id) REFERENCES auctions (id),
                    FOREIGN KEY (upc) REFERENCES products (upc)
                )
            ''')
            
            self.conn.commit()
            logger.info("Database initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing database: {str(e)}")
            raise
    
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
                    try:
                        # Get full product data from database
                        product = self.get_product_by_upc(item.get('upc'))
                        
                        # Prepare row data with auction item data
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
                            "Missing Item Description": item.get('missing_items_description', ''),
                            "Notes": item.get('notes', ''),
                            "Grand Average Price": '',
                            "Average Shipping Price": '',
                            "Recommended Highest Bid Amount": '',
                            "Current Profit Margin": '',
                            "Flagged": "No"
                        }
                        
                        # Add product data if available
                        if product:
                            row.update({
                                "Grand Average Price": f"${product.get('grand_average_price', 0):.2f}" if product.get('grand_average_price') else '',
                                "Average Shipping Price": f"${product.get('ebay_average_shipping', 0):.2f}" if product.get('ebay_average_shipping') else '',
                                "Recommended Highest Bid Amount": f"${product.get('recommended_highest_bid', 0):.2f}" if product.get('recommended_highest_bid') else '',
                                "Current Profit Margin": f"{product.get('current_profit_margin', 0):.1f}%" if product.get('current_profit_margin') is not None else '',
                                "Flagged": "Yes" if product.get('amazon_frequently_returned', 0) else "No"
                            })
                        
                        # Write the row to CSV
                        writer.writerow(row)
                        logger.debug(f"Added item {item.get('lot_number')} to CSV")
                        
                    except Exception as e:
                        logger.error(f"Error processing item {item.get('lot_number')}: {str(e)}")
                        continue
            
            logger.info(f"Successfully exported {len(auction_data)} items to {filename}")
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

    def save_auction(self, url: str, title: str, date: str) -> Optional[int]:
        """
        Save auction information to database and return the auction ID.
        
        Args:
            url: Auction URL
            title: Auction title
            date: Auction date
            
        Returns:
            int: Auction ID if successful, None otherwise
        """
        try:
            # Check if auction already exists
            self.cursor.execute('SELECT id FROM auctions WHERE url = ?', (url,))
            result = self.cursor.fetchone()
            
            if result:
                return result[0]  # Return existing auction ID
            
            # Insert new auction
            self.cursor.execute('''
                INSERT INTO auctions (url, title, date)
                VALUES (?, ?, ?)
            ''', (url, title, date))
            
            self.conn.commit()
            return self.cursor.lastrowid
            
        except sqlite3.Error as e:
            logger.error(f"Error saving auction: {str(e)}")
            return None

    def save_auction_item(self, auction_id: int, lot_number: str, current_bid: float, upc: str) -> bool:
        """
        Save auction item information to database.
        
        Args:
            auction_id: ID of the auction
            lot_number: Lot number of the item
            current_bid: Current bid amount
            upc: UPC of the item
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.cursor.execute('''
                INSERT INTO auction_items (auction_id, lot_number, current_bid, upc)
                VALUES (?, ?, ?, ?)
            ''', (auction_id, lot_number, current_bid, upc))
            
            self.conn.commit()
            return True
            
        except sqlite3.Error as e:
            logger.error(f"Error saving auction item: {str(e)}")
            return False 