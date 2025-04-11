#!/usr/bin/env python3

import argparse
import logging
from typing import Dict, Optional, List, Any
from scraper import HiBidScraper
from database import Database
from research import PriceResearch
from calculator import Calculator
from config import Config
import time
from datetime import datetime
import sys
import os
import traceback

# Configure logging
def setup_logging():
    """Configure logging to both file and console"""
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_file = os.path.join(log_dir, f'auction_scraper_{datetime.now().strftime("%Y%m%d")}.log')
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(levelname)s: %(message)s'
    )
    
    # Configure file handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    
    # Configure console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return root_logger

logger = setup_logging()

def process_auction_item(item_data: dict, db: Database, researcher: PriceResearch) -> None:
    """
    Process a single auction item: scrape, research, and update database.
    
    Args:
        item_data: Dictionary containing scraped item data
        db: Database instance
        researcher: PriceResearch instance
        
    Raises:
        Exception: If any error occurs during processing
    """
    try:
        # Extract key identifiers
        search_terms = {
            'name': item_data.get('name'),
            'brand': item_data.get('brand'),
            'model': item_data.get('model'),
            'upc': item_data.get('upc')
        }
        
        # Save/update HiBid data in database
        if not db.add_or_update_product(item_data):
            logger.error(f"Failed to save item data for UPC: {item_data.get('upc')}")
            return
        
        # Check if research is needed
        if db.needs_research(item_data.get('upc')):
            logger.info(f"Researching product: {search_terms}")
            
            try:
                # Research eBay prices
                ebay_data = researcher.research_ebay(search_terms)
                time.sleep(2)  # Rate limiting
                
                # Research Amazon prices
                amazon_data = researcher.research_amazon(search_terms)
                time.sleep(2)  # Rate limiting
                
                # Combine all data
                update_data = {
                    **item_data,
                    **ebay_data,
                    **amazon_data,
                    'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                # Update database with research results
                if not db.add_or_update_product(update_data):
                    logger.error(f"Failed to update research data for UPC: {item_data.get('upc')}")
                
                logger.info(f"Completed research for product: {search_terms}")
            except Exception as e:
                logger.error(f"Error during research for UPC {item_data.get('upc')}: {str(e)}")
                logger.debug(traceback.format_exc())
        else:
            logger.info(f"Skipping research for product: {search_terms}")
            
    except Exception as e:
        logger.error(f"Error processing item: {str(e)}")
        logger.debug(traceback.format_exc())
        raise

def print_product_details(product: Dict[str, Any]) -> None:
    """
    Print product details in a readable format.
    
    Args:
        product: Dictionary containing product data
    """
    if not product:
        print("Product not found")
        return
    
    try:
        print("\nProduct Details:")
        print(f"Name: {product.get('name', 'N/A')}")
        print(f"Brand: {product.get('brand', 'N/A')}")
        print(f"Model: {product.get('model', 'N/A')}")
        print(f"UPC: {product.get('upc', 'N/A')}")
        
        print("\nAuction Data:")
        print(f"Condition: {product.get('last_scraped_condition', 'N/A')}")
        print(f"Functionality: {product.get('last_scraped_functionality', 'N/A')}")
        print(f"Damage: {'Yes' if product.get('last_scraped_damage') else 'No'}")
        print(f"Missing Items: {'Yes' if product.get('last_scraped_missing_items') else 'No'}")
        
        print("\nResearch Data:")
        print(f"eBay Lowest Sold: ${product.get('ebay_lowest_sold', 0):.2f}")
        print(f"eBay Average Sold: ${product.get('ebay_average_sold', 0):.2f}")
        print(f"eBay Highest Sold: ${product.get('ebay_highest_sold', 0):.2f}")
        print(f"eBay Average Shipping: ${product.get('ebay_average_shipping', 0):.2f}")
        print(f"Amazon Price: ${product.get('amazon_price', 0):.2f}")
        print(f"Amazon Star Rating: {product.get('amazon_star_rating', 'N/A')}")
        print(f"Amazon Reviews Count: {product.get('amazon_reviews_count', 'N/A')}")
        print(f"Frequently Returned: {'Yes' if product.get('amazon_frequently_returned') else 'No'}")
        
        print("\nCalculated Data:")
        print(f"Grand Average Price: ${product.get('grand_average_price', 0):.2f}")
        print(f"Recommended Highest Bid: ${product.get('recommended_highest_bid', 0):.2f}")
        print(f"Last Updated: {product.get('last_updated', 'N/A')}")
    except Exception as e:
        logger.error(f"Error printing product details: {str(e)}")
        logger.debug(traceback.format_exc())

def prompt_for_product_data() -> Dict[str, Any]:
    """
    Prompt user for product data.
    
    Returns:
        Dictionary containing product data
        
    Raises:
        Exception: If input validation fails
    """
    try:
        print("\nEnter product details:")
        data = {
            'name': input("Name: ").strip(),
            'brand': input("Brand: ").strip(),
            'model': input("Model: ").strip(),
            'upc': input("UPC: ").strip(),
            'last_scraped_condition': input("Condition: ").strip(),
            'last_scraped_functionality': input("Functionality: ").strip(),
            'last_scraped_damage': input("Damage? (y/n): ").strip().lower() == 'y',
            'last_scraped_missing_items': input("Missing Items? (y/n): ").strip().lower() == 'y',
            'last_scraped_damage_desc': input("Damage Description: ").strip(),
            'last_scraped_missing_items_desc': input("Missing Items Description: ").strip(),
            'last_scraped_notes': input("Notes: ").strip()
        }
        
        # Validate required fields
        if not data['name'] or not data['upc']:
            raise ValueError("Name and UPC are required fields")
            
        return data
    except Exception as e:
        logger.error(f"Error getting product data: {str(e)}")
        logger.debug(traceback.format_exc())
        raise

def process_auction_url(url: str, db: Database) -> None:
    """
    Process an auction URL: scrape items and update database.
    
    Args:
        url: URL of the auction to scrape
        db: Database instance
    """
    try:
        # Initialize scraper and researcher
        config = Config()
        scraper = HiBidScraper(config)
        researcher = PriceResearch(config)
        
        # Scrape auction items
        items = scraper.scrape_auction(url)
        if not items:
            logger.error(f"No items found in auction: {url}")
            return
            
        # Process each item
        for item in items:
            process_auction_item(item, db, researcher)
            
        logger.info(f"Successfully processed {len(items)} items from auction: {url}")
        
    except Exception as e:
        logger.error(f"Error processing auction URL {url}: {str(e)}")
        logger.debug(traceback.format_exc())

def process_research_request(search_terms: Dict[str, str], db: Database) -> None:
    """
    Process a research request for a product.
    
    Args:
        search_terms: Dictionary containing search criteria (upc, name, brand, model)
        db: Database instance
    """
    try:
        config = Config()
        researcher = PriceResearch(config)
        
        # Get product from database
        product = db.get_product_by_upc(search_terms.get('upc'))
        if not product:
            logger.error(f"Product not found for search terms: {search_terms}")
            return
            
        # Research prices
        ebay_data = researcher.research_ebay(search_terms)
        time.sleep(2)  # Rate limiting
        amazon_data = researcher.research_amazon(search_terms)
        
        # Update product data
        update_data = {
            **product,
            **ebay_data,
            **amazon_data,
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        if not db.add_or_update_product(update_data):
            logger.error(f"Failed to update research data for product: {search_terms}")
        else:
            logger.info(f"Successfully updated research data for product: {search_terms}")
            
    except Exception as e:
        logger.error(f"Error processing research request: {str(e)}")
        logger.debug(traceback.format_exc())

def main():
    """Main entry point for the application"""
    try:
        # Parse command line arguments
        parser = argparse.ArgumentParser(description='Auction Data Scraper and Profit Calculator')
        parser.add_argument('--url', help='HiBid auction URL to scrape')
        parser.add_argument('--upc', help='UPC to research')
        parser.add_argument('--name', help='Product name to research')
        parser.add_argument('--brand', help='Product brand to research')
        parser.add_argument('--model', help='Product model to research')
        parser.add_argument('--list', action='store_true', help='List all products in database')
        parser.add_argument('--page', type=int, default=1, help='Page number when listing products')
        args = parser.parse_args()
        
        # Initialize configuration and database
        config = Config()
        db = Database(config)
        
        if args.list:
            # List all products
            page_size = 50
            offset = (args.page - 1) * page_size
            products = db.list_all_products(limit=page_size, offset=offset)
            total_products = db.get_total_products_count()
            
            print(f"\nProducts (Page {args.page}, Showing {len(products)} of {total_products}):")
            print("-" * 120)
            print(f"{'UPC':<15} {'Name':<30} {'Brand':<15} {'Model':<15} {'Condition':<10} {'Auction Price':<12} {'eBay Avg':<10} {'Profit %':<10}")
            print("-" * 120)
            
            for product in products:
                profit_margin = product.get('current_profit_margin', 0)
                profit_color = '\033[92m' if profit_margin > 20 else '\033[91m' if profit_margin < 0 else '\033[0m'
                
                print(f"{product['upc']:<15} {product['name'][:30]:<30} {product['brand'][:15]:<15} {product['model'][:15]:<15} "
                      f"{product['condition'][:10]:<10} ${product['auction_price']:<11.2f} ${product['ebay_average_sold']:<9.2f} "
                      f"{profit_color}{profit_margin:>9.1f}%\033[0m")
            
            print("-" * 120)
            if total_products > page_size:
                total_pages = (total_products + page_size - 1) // page_size
                print(f"\nPage {args.page} of {total_pages}. Use --page <number> to view other pages.")
            return
        
        # Process auction URL if provided
        if args.url:
            process_auction_url(args.url, db)
        
        # Process research request if provided
        if args.upc or args.name or args.brand or args.model:
            search_terms = {
                'upc': args.upc,
                'name': args.name,
                'brand': args.brand,
                'model': args.model
            }
            process_research_request(search_terms, db)
        
        db.close()
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        logger.debug(traceback.format_exc())
        sys.exit(1)

if __name__ == '__main__':
    main() 