import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
from typing import Dict, Optional, Union, Any
import logging
from database import Database

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HiBidScraper:
    def __init__(self, config):
        self.config = config
        self.scraping_config = config.get_scraping_config()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.scraping_config['user_agent']
        })
    
    def scrape_auction(self, url: str) -> bool:
        """Scrape auction data from HiBid URL"""
        try:
            response = self.session.get(url, timeout=self.scraping_config['request_timeout'])
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract auction information
            auction_title = self._extract_auction_title(soup)
            auction_date = self._extract_auction_date(soup)
            
            # Save auction data
            db = Database(self.config)
            auction_id = db.save_auction(url, auction_title, auction_date)
            
            # Extract and save items
            items = self._extract_items(soup)
            for item in items:
                db.save_item(
                    auction_id=auction_id,
                    item_number=item['lot_number'],
                    title=item['name'],
                    description=item['notes'] or '',
                    current_bid=item['current_bid'],
                    estimated_value=0.0  # Will be updated during research phase
                )
            
            db.close()
            return True
            
        except requests.RequestException as e:
            logger.error(f"Error scraping auction {url}: {str(e)}")
            return False
    
    def scrape_item(self, url: str) -> Optional[Dict[str, Union[str, float, bool]]]:
        """
        Scrape detailed information for a single HiBid item.
        
        Args:
            url: The URL of the HiBid item page
            
        Returns:
            Dictionary containing item details or None if scraping fails
        """
        try:
            response = self.session.get(url, timeout=self.scraping_config['request_timeout'])
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract item details
            item_data = {
                'current_bid': self._extract_current_bid(soup),
                'lot_number': self._extract_lot_number(soup),
                'name': self._extract_item_name(soup),
                'brand': self._extract_brand(soup),
                'model': self._extract_model(soup),
                'upc': self._extract_upc(soup),
                'condition': self._extract_condition(soup),
                'functionality': self._extract_functionality(soup),
                'damage': self._extract_damage(soup),
                'missing_items': self._extract_missing_items(soup),
                'damage_description': self._extract_damage_description(soup),
                'missing_item_description': self._extract_missing_item_description(soup),
                'notes': self._extract_notes(soup)
            }
            
            return item_data
            
        except requests.RequestException as e:
            logger.error(f"Error scraping item {url}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error scraping item {url}: {str(e)}")
            return None
    
    def _extract_current_bid(self, soup: BeautifulSoup) -> float:
        """Extract current bid from item page"""
        # Look for bid amount in various possible locations
        bid_selectors = [
            'span.current-bid-amount',  # Common selector
            'div.bid-amount',           # Alternative selector
            'span[data-bid-amount]'     # Data attribute selector
        ]
        
        for selector in bid_selectors:
            element = soup.select_one(selector)
            if element:
                try:
                    # Remove currency symbol and commas, convert to float
                    return float(element.text.strip().replace('$', '').replace(',', ''))
                except ValueError:
                    continue
        
        return 0.0
    
    def _extract_lot_number(self, soup: BeautifulSoup) -> str:
        """Extract lot number from item page"""
        # Look for lot number in various possible locations
        lot_selectors = [
            'span.lot-number',          # Common selector
            'div.lot-info span',        # Alternative selector
            'span[data-lot-number]'     # Data attribute selector
        ]
        
        for selector in lot_selectors:
            element = soup.select_one(selector)
            if element:
                return element.text.strip()
        
        return "Unknown"
    
    def _extract_item_name(self, soup: BeautifulSoup) -> str:
        """Extract item name from item page"""
        # Look for item name in various possible locations
        name_selectors = [
            'h1.item-title',            # Common selector
            'div.item-details h1',      # Alternative selector
            'span[data-item-name]'      # Data attribute selector
        ]
        
        for selector in name_selectors:
            element = soup.select_one(selector)
            if element:
                return element.text.strip()
        
        return "Unknown"
    
    def _extract_brand(self, soup: BeautifulSoup) -> str:
        """Extract brand from item page"""
        # Look for brand in various possible locations
        brand_selectors = [
            'span.brand-name',          # Common selector
            'div.item-details span',    # Alternative selector
            'span[data-brand]'          # Data attribute selector
        ]
        
        for selector in brand_selectors:
            element = soup.select_one(selector)
            if element:
                return element.text.strip()
        
        return ""
    
    def _extract_model(self, soup: BeautifulSoup) -> str:
        """Extract model from item page"""
        # Look for model in various possible locations
        model_selectors = [
            'span.model-number',        # Common selector
            'div.item-details span',    # Alternative selector
            'span[data-model]'          # Data attribute selector
        ]
        
        for selector in model_selectors:
            element = soup.select_one(selector)
            if element:
                return element.text.strip()
        
        return ""
    
    def _extract_upc(self, soup: BeautifulSoup) -> str:
        """Extract UPC from item page"""
        # Look for UPC in various possible locations
        upc_selectors = [
            'span.upc-code',            # Common selector
            'div.item-details span',    # Alternative selector
            'span[data-upc]'            # Data attribute selector
        ]
        
        for selector in upc_selectors:
            element = soup.select_one(selector)
            if element:
                return element.text.strip()
        
        return ""
    
    def _extract_condition(self, soup: BeautifulSoup) -> str:
        """Extract condition from item page"""
        # Look for condition in various possible locations
        condition_selectors = [
            'span.item-condition',      # Common selector
            'div.item-details span',    # Alternative selector
            'span[data-condition]'      # Data attribute selector
        ]
        
        for selector in condition_selectors:
            element = soup.select_one(selector)
            if element:
                return element.text.strip()
        
        return ""
    
    def _extract_functionality(self, soup: BeautifulSoup) -> str:
        """Extract functionality from item page"""
        # Look for functionality in various possible locations
        func_selectors = [
            'span.functionality',       # Common selector
            'div.item-details span',    # Alternative selector
            'span[data-functionality]'  # Data attribute selector
        ]
        
        for selector in func_selectors:
            element = soup.select_one(selector)
            if element:
                return element.text.strip()
        
        return ""
    
    def _extract_damage(self, soup: BeautifulSoup) -> bool:
        """Extract damage status from item page"""
        # Look for damage indicator in various possible locations
        damage_selectors = [
            'span.damage-indicator',    # Common selector
            'div.item-details span',    # Alternative selector
            'span[data-damage]'         # Data attribute selector
        ]
        
        for selector in damage_selectors:
            element = soup.select_one(selector)
            if element:
                text = element.text.strip().lower()
                return text == 'yes' or text == 'true' or text == 'damaged'
        
        return False
    
    def _extract_missing_items(self, soup: BeautifulSoup) -> bool:
        """Extract missing items status from item page"""
        # Look for missing items indicator in various possible locations
        missing_selectors = [
            'span.missing-items',       # Common selector
            'div.item-details span',    # Alternative selector
            'span[data-missing-items]'  # Data attribute selector
        ]
        
        for selector in missing_selectors:
            element = soup.select_one(selector)
            if element:
                text = element.text.strip().lower()
                return text == 'yes' or text == 'true' or text == 'missing items'
        
        return False
    
    def _extract_damage_description(self, soup: BeautifulSoup) -> str:
        """Extract damage description from item page"""
        # Look for damage description in various possible locations
        damage_desc_selectors = [
            'div.damage-description',   # Common selector
            'div.item-details div',     # Alternative selector
            'div[data-damage-desc]'     # Data attribute selector
        ]
        
        for selector in damage_desc_selectors:
            element = soup.select_one(selector)
            if element:
                return element.text.strip()
        
        return ""
    
    def _extract_missing_item_description(self, soup: BeautifulSoup) -> str:
        """Extract missing items description from item page"""
        # Look for missing items description in various possible locations
        missing_desc_selectors = [
            'div.missing-items-desc',   # Common selector
            'div.item-details div',     # Alternative selector
            'div[data-missing-desc]'    # Data attribute selector
        ]
        
        for selector in missing_desc_selectors:
            element = soup.select_one(selector)
            if element:
                return element.text.strip()
        
        return ""
    
    def _extract_notes(self, soup: BeautifulSoup) -> str:
        """Extract general notes from item page"""
        # Look for notes in various possible locations
        notes_selectors = [
            'div.item-notes',           # Common selector
            'div.item-details div',     # Alternative selector
            'div[data-notes]'           # Data attribute selector
        ]
        
        for selector in notes_selectors:
            element = soup.select_one(selector)
            if element:
                return element.text.strip()
        
        return ""
    
    # Keep existing methods for auction scraping
    def _extract_auction_title(self, soup: BeautifulSoup) -> str:
        """Extract auction title from page"""
        title_element = soup.find('h1', class_='auction-title')
        return title_element.text.strip() if title_element else "Unknown Auction"
    
    def _extract_auction_date(self, soup: BeautifulSoup) -> str:
        """Extract auction date from page"""
        date_element = soup.find('div', class_='auction-date')
        if date_element:
            try:
                date_str = date_element.text.strip()
                return datetime.strptime(date_str, '%B %d, %Y').strftime('%Y-%m-%d')
            except ValueError:
                pass
        return datetime.now().strftime('%Y-%m-%d')
    
    def _extract_items(self, soup: BeautifulSoup) -> list:
        """Extract items from auction page"""
        items = []
        item_elements = soup.find_all('div', class_='item-lot')
        
        for element in item_elements:
            try:
                item = {
                    'lot_number': self._extract_lot_number(element),
                    'name': self._extract_item_name(element),
                    'current_bid': self._extract_current_bid(element),
                    'notes': self._extract_notes(element)
                }
                items.append(item)
            except Exception as e:
                logger.error(f"Error extracting item: {str(e)}")
                continue
        
        return items 