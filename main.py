#!/usr/bin/env python3

import argparse
from config import Config
from scraper import HiBidScraper
from database import Database
from research import PriceResearch
from calculator import ProfitCalculator
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
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

def main():
    """Main entry point for the application"""
    parser = argparse.ArgumentParser(description='Auction Data Scraper and Profit Calculator')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Scrape command
    scrape_parser = subparsers.add_parser('scrape', help='Scrape auction data from HiBid')
    scrape_parser.add_argument('--url', required=True, help='HiBid auction URL to scrape')
    scrape_parser.add_argument('--output', default='auction_results.csv', help='Output CSV file path')
    
    # Lookup command
    lookup_parser = subparsers.add_parser('lookup', help='Lookup product in database')
    lookup_group = lookup_parser.add_mutually_exclusive_group(required=True)
    lookup_group.add_argument('--upc', help='UPC to lookup')
    lookup_group.add_argument('--name', help='Name to lookup')
    lookup_group.add_argument('--brand', help='Brand to lookup (requires --model)')
    lookup_parser.add_argument('--model', help='Model to lookup (requires --brand)')
    
    # Add command
    add_parser = subparsers.add_parser('add', help='Add a new product to database')
    
    # Edit command
    edit_parser = subparsers.add_parser('edit', help='Edit an existing product')
    edit_parser.add_argument('--upc', required=True, help='UPC of product to edit')
    
    # Config command
    config_parser = subparsers.add_parser('config', help='View or modify configuration')
    config_group = config_parser.add_mutually_exclusive_group(required=True)
    config_group.add_argument('--view', action='store_true', help='View current configuration')
    config_group.add_argument('--set', nargs=2, metavar=('KEY', 'VALUE'), help='Set configuration value')
    
    args = parser.parse_args()
    
    # Initialize components
    try:
        config = Config()
        db = Database(config)
        
        if args.command == 'scrape':
            scraper = HiBidScraper(config)
            researcher = PriceResearch(config)
            
            # Scrape auction page to get item URLs
            items = scraper.scrape_auction(args.url)
            if not items:
                logger.error("No items found in auction")
                return
            
            # Process each item
            for item in items:
                process_auction_item(item, db, researcher)
            
            # Export results
            if not db.export_to_csv(items, args.output):
                logger.error(f"Failed to export results to {args.output}")
            
        elif args.command == 'lookup':
            if args.upc:
                product = db.get_product_by_upc(args.upc)
                print_product_details(product)
            elif args.name:
                products = db.get_product_by_name(args.name)
                for product in products:
                    print_product_details(product)
            elif args.brand and args.model:
                products = db.get_product_by_brand_model(args.brand, args.model)
                for product in products:
                    print_product_details(product)
            
        elif args.command == 'add':
            try:
                product_data = prompt_for_product_data()
                if db.add_or_update_product(product_data):
                    print("Product added successfully")
                else:
                    print("Error adding product")
            except Exception as e:
                logger.error(f"Error adding product: {str(e)}")
                logger.debug(traceback.format_exc())
            
        elif args.command == 'edit':
            product = db.get_product_by_upc(args.upc)
            if not product:
                print(f"Product with UPC {args.upc} not found")
                return
            
            try:
                print("Current product details:")
                print_product_details(product)
                
                print("\nEnter new values (press Enter to keep current value):")
                new_data = prompt_for_product_data()
                
                # Merge new data with existing, keeping current values if not specified
                for key, value in new_data.items():
                    if value:  # Only update if new value provided
                        product[key] = value
                
                if db.add_or_update_product(product):
                    print("Product updated successfully")
                else:
                    print("Error updating product")
            except Exception as e:
                logger.error(f"Error editing product: {str(e)}")
                logger.debug(traceback.format_exc())
            
        elif args.command == 'config':
            if args.view:
                print("\nCurrent Configuration:")
                for key, value in config.settings.items():
                    print(f"{key}: {value}")
            elif args.set:
                key, value = args.set
                try:
                    # Convert value to appropriate type
                    if '.' in value:
                        value = float(value)
                    else:
                        value = int(value)
                    
                    if config.set(key, value):
                        print(f"Configuration updated: {key} = {value}")
                    else:
                        print(f"Invalid configuration key: {key}")
                except ValueError:
                    print("Invalid value. Must be a number.")
                except Exception as e:
                    logger.error(f"Error updating configuration: {str(e)}")
                    logger.debug(traceback.format_exc())
        
        else:
            parser.print_help()
            
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        logger.debug(traceback.format_exc())
        sys.exit(1)
    finally:
        try:
            db.close()
        except Exception as e:
            logger.error(f"Error closing database: {str(e)}")
            logger.debug(traceback.format_exc())

if __name__ == '__main__':
    main() 