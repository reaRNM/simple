import sqlite3
import csv
from datetime import datetime
from typing import Dict, Optional, List, Union, Any
import logging
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, config=None):
        self.config = config
        self.db_path = config.get_database_path()
        self.conn = None
        self.cursor = None
        self._init_db()
    
    def _init_db(self):
        """Initialize the database and create tables if they don't exist"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            
            # Create products table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS products (
                    upc TEXT PRIMARY KEY,
                    name TEXT,
                    brand TEXT,
                    model TEXT,
                    condition TEXT,
                    functionality TEXT,
                    damage BOOLEAN,
                    missing_items BOOLEAN,
                    damage_desc TEXT,
                    missing_items_desc TEXT,
                    notes TEXT,
                    ebay_lowest_sold REAL,
                    ebay_average_sold REAL,
                    ebay_highest_sold REAL,
                    ebay_average_listed REAL,
                    ebay_average_shipping REAL,
                    ebay_active_count INTEGER,
                    amazon_price REAL,
                    amazon_star_rating REAL,
                    amazon_reviews_count INTEGER,
                    amazon_frequently_returned BOOLEAN,
                    grand_average_price REAL,
                    recommended_highest_bid REAL,
                    current_profit_margin REAL,
                    last_updated TIMESTAMP,
                    category TEXT,
                    auction_url TEXT,
                    auction_price REAL,
                    auction_date TEXT,
                    competitor_count INTEGER,
                    market_health REAL,
                    price_trend TEXT,
                    demand_trend TEXT,
                    amazon_discount TEXT,
                    amazon_category_rating INTEGER,
                    amazon_subcategory_rating INTEGER,
                    amazon_sold_per_month INTEGER,
                    last_research_date TEXT,
                    last_calculation_date TEXT
                )
            ''')
            
            # Create auctions table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS auctions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE,
                    title TEXT,
                    date TEXT,
                    location TEXT
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
                    price REAL,
                    FOREIGN KEY (auction_id) REFERENCES auctions (id),
                    FOREIGN KEY (upc) REFERENCES products (upc),
                    PRIMARY KEY (auction_id, upc)
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
            # Separate product data from auction-specific data
            product_fields = {
                'upc', 'name', 'brand', 'model', 'condition', 'functionality',
                'damage', 'missing_items', 'damage_desc', 'missing_items_desc', 'notes',
                'ebay_lowest_sold', 'ebay_average_sold', 'ebay_highest_sold',
                'ebay_average_listed', 'ebay_average_shipping', 'ebay_active_count',
                'amazon_price', 'amazon_star_rating', 'amazon_reviews_count',
                'amazon_frequently_returned', 'grand_average_price',
                'recommended_highest_bid', 'current_profit_margin'
            }
            
            # Extract product data
            product_data_filtered = {
                k: v for k, v in product_data.items()
                if k in product_fields and v is not None
            }
            
            # Prepare the SQL statement for product data
            columns = list(product_data_filtered.keys())
            values = list(product_data_filtered.values())
            placeholders = ['?'] * len(values)
            
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
            
            # Handle auction-specific data if present
            if 'lot_number' in product_data and product_data.get('lot_number'):
                auction_id = self.save_auction(
                    product_data.get('auction_url', ''),
                    product_data.get('auction_title', ''),
                    product_data.get('auction_date', '')
                )
                
                if auction_id:
                    self.save_auction_item(
                        auction_id,
                        product_data.get('lot_number'),
                        product_data.get('current_bid', 0),
                        product_data.get('upc')
                    )
            
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
    
    def export_to_csv(self, items: List[Dict[str, Any]], output_file: str) -> bool:
        """Export auction items to CSV file with research and calculation data."""
        try:
            with open(output_file, 'w', newline='') as csvfile:
                fieldnames = [
                    'lot_number', 'name', 'brand', 'model', 'upc',
                    'current_bid', 'next_bid', 'buy_now_price',
                    'condition', 'functionality', 'damage', 'missing_items',
                    'damage_desc', 'missing_items_desc', 'notes',
                    'ebay_lowest_sold', 'ebay_average_sold', 'ebay_highest_sold',
                    'ebay_average_shipping', 'ebay_active_count',
                    'amazon_price', 'amazon_star_rating', 'amazon_reviews_count',
                    'amazon_frequently_returned',
                    'grand_average_price', 'recommended_highest_bid',
                    'current_profit_margin', 'last_updated'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                item_count = 0
                for item in items:
                    try:
                        # Get product data from database
                        product = self.get_product_by_upc(item.get('upc'))
                        
                        # Combine auction and product data
                        row_data = {
                            'lot_number': item.get('lot_number', ''),
                            'name': item.get('name', ''),
                            'brand': item.get('brand', ''),
                            'model': item.get('model', ''),
                            'upc': item.get('upc', ''),
                            'current_bid': item.get('current_bid', 0),
                            'next_bid': item.get('next_bid', 0),
                            'buy_now_price': item.get('buy_now_price', 0),
                            'condition': item.get('last_scraped_condition', ''),
                            'functionality': item.get('last_scraped_functionality', ''),
                            'damage': 'Yes' if item.get('last_scraped_damage') else 'No',
                            'missing_items': 'Yes' if item.get('last_scraped_missing_items') else 'No',
                            'damage_desc': item.get('last_scraped_damage_desc', ''),
                            'missing_items_desc': item.get('last_scraped_missing_items_desc', ''),
                            'notes': item.get('last_scraped_notes', '')
                        }
                        
                        # Add research data if available
                        if product:
                            row_data.update({
                                'ebay_lowest_sold': product.get('ebay_lowest_sold', 0),
                                'ebay_average_sold': product.get('ebay_average_sold', 0),
                                'ebay_highest_sold': product.get('ebay_highest_sold', 0),
                                'ebay_average_shipping': product.get('ebay_average_shipping', 0),
                                'ebay_active_count': product.get('ebay_active_count', 0),
                                'amazon_price': product.get('amazon_price', 0),
                                'amazon_star_rating': product.get('amazon_star_rating', ''),
                                'amazon_reviews_count': product.get('amazon_reviews_count', ''),
                                'amazon_frequently_returned': 'Yes' if product.get('amazon_frequently_returned') else 'No',
                                'grand_average_price': product.get('grand_average_price', 0),
                                'recommended_highest_bid': product.get('recommended_highest_bid', 0),
                                'current_profit_margin': product.get('current_profit_margin', 0),
                                'last_updated': product.get('last_updated', '')
                            })
                        
                        writer.writerow(row_data)
                        item_count += 1
                        
                    except Exception as e:
                        logger.error(f"Error writing item to CSV: {str(e)}")
                        logger.debug(traceback.format_exc())
                        continue
                
                logger.info(f"Successfully exported {item_count} items to {output_file}")
                return True
                
        except Exception as e:
            logger.error(f"Error exporting to CSV: {str(e)}")
            logger.debug(traceback.format_exc())
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

    def list_all_products(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """
        List all products in the database with pagination.
        
        Args:
            limit: Maximum number of products to return
            offset: Number of products to skip (for pagination)
            
        Returns:
            List of dictionaries containing product information
        """
        try:
            self.cursor.execute('''
                SELECT 
                    upc, name, brand, model, condition,
                    auction_price, ebay_average_sold, amazon_price,
                    grand_average_price, recommended_highest_bid,
                    current_profit_margin, last_research_date
                FROM products
                ORDER BY last_research_date DESC
                LIMIT ? OFFSET ?
            ''', (limit, offset))
            
            products = []
            for row in self.cursor.fetchall():
                products.append({
                    'upc': row[0],
                    'name': row[1],
                    'brand': row[2],
                    'model': row[3],
                    'condition': row[4],
                    'auction_price': row[5],
                    'ebay_average_sold': row[6],
                    'amazon_price': row[7],
                    'grand_average_price': row[8],
                    'recommended_highest_bid': row[9],
                    'current_profit_margin': row[10],
                    'last_research_date': row[11]
                })
            
            return products
            
        except sqlite3.Error as e:
            logger.error(f"Error listing products: {str(e)}")
            return []
    
    def get_total_products_count(self) -> int:
        """
        Get the total number of products in the database.
        
        Returns:
            Total count of products
        """
        try:
            self.cursor.execute('SELECT COUNT(*) FROM products')
            return self.cursor.fetchone()[0]
        except sqlite3.Error as e:
            logger.error(f"Error getting product count: {str(e)}")
            return 0 