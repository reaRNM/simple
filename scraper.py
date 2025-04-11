import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
from typing import Dict, Optional, Union, Any, List
import logging
from database import Database
import traceback
import json

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
        self.db = Database(self.config)
        
        # GraphQL configuration
        self.graphql_url = "https://hibid.com/graphql"
        self.headers = {
            'authority': 'hibid.com',
            'method': 'POST',
            'path': '/graphql',
            'scheme': 'https',
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br, zstd',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'origin': 'https://hibid.com',
            'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Chrome OS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'site_subdomain': 'hibid.com',
            'user-agent': 'Mozilla/5.0 (X11; CrOS x86_64 14541.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36'
        }
        
        # Load cookies from config
        self.cookies = self.scraping_config.get('cookies', {})
    
    def scrape_auction(self, url: str) -> List[Dict[str, Any]]:
        """
        Scrape auction page to get item URLs and details using GraphQL API.
        
        Args:
            url: URL of the auction page
            
        Returns:
            List of dictionaries containing item data
        """
        try:
            # Extract auction ID from URL
            auction_id = self._extract_auction_id(url)
            if not auction_id:
                logger.error("Could not extract auction ID from URL")
                return []
            
            # Update headers with current URL
            self.headers['referer'] = url
            
            # GraphQL query payload
            payload = {
                "operationName": "LotSearch",
                "query": """
                    query LotSearch($auctionId: Int = null, $pageNumber: Int!, $pageLength: Int!, $category: CategoryId = null, $searchText: String = null, $zip: String = null, $miles: Int = null, $shippingOffered: Boolean = false, $countryName: String = null, $status: AuctionLotStatus = null, $sortOrder: EventItemSortOrder = null, $filter: AuctionLotFilter = null, $isArchive: Boolean = false, $dateStart: DateTime, $dateEnd: DateTime, $countAsView: Boolean = true, $hideGoogle: Boolean = false) {
                      lotSearch(
                        input: {auctionId: $auctionId, category: $category, searchText: $searchText, zip: $zip, miles: $miles, shippingOffered: $shippingOffered, countryName: $countryName, status: $status, sortOrder: $sortOrder, filter: $filter, isArchive: $isArchive, dateStart: $dateStart, dateEnd: $dateEnd, countAsView: $countAsView, hideGoogle: $hideGoogle}
                        pageNumber: $pageNumber
                        pageLength: $pageLength
                        sortDirection: DESC
                      ) {
                        pagedResults {
                          results {
                            lotNumber
                            description
                            lotState {
                              highBid
                              minBid
                            }
                          }
                        }
                      }
                    }
                """,
                "variables": {
                    "auctionId": auction_id,
                    "pageNumber": 1,
                    "pageLength": 9000,
                    "category": None,
                    "searchText": None,
                    "zip": "",
                    "miles": 50,
                    "shippingOffered": False,
                    "countryName": "",
                    "status": "ALL",
                    "sortOrder": "LOT_NUMBER",
                    "filter": "ALL",
                    "isArchive": False,
                    "dateStart": None,
                    "dateEnd": None,
                    "countAsView": True,
                    "hideGoogle": False
                }
            }
            
            # Make GraphQL request
            response = self.session.post(
                self.graphql_url,
                headers=self.headers,
                json=payload,
                cookies=self.cookies
            )
            
            if response.status_code != 200:
                logger.error(f"GraphQL request failed with status code: {response.status_code}")
                return []
            
            # Parse response
            data = response.json()
            items = data['data']['lotSearch']['pagedResults']['results']
            
            # Process items
            processed_items = []
            for item in items:
                try:
                    # Extract basic information
                    lot_number = item['lotNumber']
                    current_bid = item['lotState'].get('highBid', item['lotState'].get('minBid', 0))
                    
                    # Parse description
                    description = item['description']
                    item_data = self._parse_description(description)
                    
                    # Add lot number and current bid
                    item_data.update({
                        'lot_number': lot_number,
                        'current_bid': current_bid
                    })
                    
                    # Save to database
                    if item_data.get('upc'):
                        if not self.db.save_auction_item(auction_id, lot_number, current_bid, item_data['upc']):
                            logger.error(f"Failed to save item {lot_number}")
                            continue
                    
                    processed_items.append(item_data)
                    
                except Exception as e:
                    logger.error(f"Error processing item {lot_number}: {str(e)}")
                    continue
            
            return processed_items
            
        except Exception as e:
            logger.error(f"Error scraping auction: {str(e)}")
            logger.debug(traceback.format_exc())
            return []
    
    def _extract_auction_id(self, url: str) -> Optional[int]:
        """Extract auction ID from URL"""
        try:
            # URL format: https://hibid.com/catalog/{auction_id}/{auction_name}
            parts = url.split('/')
            for part in parts:
                if part.isdigit():
                    return int(part)
            return None
        except Exception:
            return None
    
    def _parse_description(self, description: str) -> Dict[str, Any]:
        """Parse item description text into structured data"""
        data = {
            'name': None,
            'brand': None,
            'model': None,
            'upc': None,
            'condition': None,
            'functionality': None,
            'damage': False,
            'missing_items': False,
            'damage_description': None,
            'missing_items_description': None,
            'notes': None
        }
        
        lines = description.splitlines()
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            if line.startswith("Title:"):
                data['name'] = line.replace("Title:", "").strip()
            elif line.startswith("Brand:"):
                data['brand'] = line.replace("Brand:", "").strip()
            elif line.startswith("Model:"):
                data['model'] = line.replace("Model:", "").strip()
            elif line.startswith("UPC:"):
                data['upc'] = line.replace("UPC:", "").strip()
            elif line.startswith("Condition:"):
                data['condition'] = line.replace("Condition:", "").strip()
            elif line.startswith("Functional?:"):
                data['functionality'] = line.replace("Functional?:", "").strip()
            elif line.startswith("Damaged?:"):
                data['damage'] = line.replace("Damaged?:", "").strip().lower() == 'yes'
            elif line.startswith("Missing Parts?:"):
                data['missing_items'] = line.replace("Missing Parts?:", "").strip().lower() == 'yes'
            elif line.startswith("Damage Description:"):
                data['damage_description'] = line.replace("Damage Description:", "").strip()
            elif line.startswith("Missing Parts Description:"):
                data['missing_items_description'] = line.replace("Missing Parts Description:", "").strip()
            elif line.startswith("Notes:"):
                data['notes'] = line.replace("Notes:", "").strip()
        
        return data
    
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