import requests
from bs4 import BeautifulSoup
import time
import re
from typing import Dict, List, Optional, Tuple, Union
import logging
from statistics import mean
from database import Database
from urllib.parse import quote_plus

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PriceResearch:
    def __init__(self, config):
        self.config = config
        self.scraping_config = config.get_scraping_config()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.scraping_config['user_agent']
        })
    
    def research(self, item_id=None):
        """Research prices for items"""
        db = Database(self.config)
        
        if item_id:
            # Research specific item
            self._research_item(db, item_id)
        else:
            # Research all items without research data
            self._research_all_items(db)
        
        db.close()
    
    def _research_all_items(self, db):
        """Research prices for all items without research data"""
        db.cursor.execute('''
            SELECT id, title FROM items
            WHERE id NOT IN (SELECT item_id FROM research_data)
        ''')
        items = db.cursor.fetchall()
        
        for item_id, title in items:
            self._research_item(db, item_id)
            time.sleep(2)  # Be nice to the servers
    
    def _research_item(self, db, item_id):
        """Research prices for a specific item"""
        # Get item details
        db.cursor.execute('SELECT title FROM items WHERE id = ?', (item_id,))
        item = db.cursor.fetchone()
        if not item:
            return
        
        title = item[0]
        
        # Research eBay prices
        ebay_prices = self._research_ebay(title)
        for price, url in ebay_prices:
            db.save_research_data(item_id, 'eBay', price, url)
        
        # Research Amazon prices
        amazon_prices = self._research_amazon(title)
        for price, url in amazon_prices:
            db.save_research_data(item_id, 'Amazon', price, url)
    
    def _research_ebay(self, title):
        """Research prices on eBay"""
        prices = []
        try:
            # Construct eBay search URL
            search_query = title.replace(' ', '+')
            url = f"https://www.ebay.com/sch/i.html?_nkw={search_query}&_sop=15"  # Sort by price + shipping
            
            response = self.session.get(url, timeout=self.scraping_config['request_timeout'])
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            items = soup.find_all('div', class_='s-item__info')
            
            for item in items[:5]:  # Get top 5 results
                try:
                    price_element = item.find('span', class_='s-item__price')
                    if price_element:
                        price = float(price_element.text.strip().replace('$', '').replace(',', ''))
                        url_element = item.find('a', class_='s-item__link')
                        url = url_element['href'] if url_element else ''
                        prices.append((price, url))
                except (ValueError, AttributeError):
                    continue
                    
        except requests.RequestException as e:
            print(f"Error researching eBay: {str(e)}")
        
        return prices
    
    def _research_amazon(self, title):
        """Research prices on Amazon"""
        prices = []
        try:
            # Construct Amazon search URL
            search_query = title.replace(' ', '+')
            url = f"https://www.amazon.com/s?k={search_query}&sort=price-asc-rank"
            
            response = self.session.get(url, timeout=self.scraping_config['request_timeout'])
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            items = soup.find_all('div', class_='s-result-item')
            
            for item in items[:5]:  # Get top 5 results
                try:
                    price_element = item.find('span', class_='a-price-whole')
                    if price_element:
                        price = float(price_element.text.strip().replace(',', ''))
                        url_element = item.find('a', class_='a-link-normal')
                        url = f"https://www.amazon.com{url_element['href']}" if url_element else ''
                        prices.append((price, url))
                except (ValueError, AttributeError):
                    continue
                    
        except requests.RequestException as e:
            print(f"Error researching Amazon: {str(e)}")
        
        return prices

    def research_ebay(self, search_terms: Dict[str, str]) -> Dict[str, Optional[float]]:
        """
        Scrape eBay for product pricing information using free methods only.
        
        DISCLAIMER: This function relies on scraping eBay's public search results,
        which is fragile, may violate eBay's Terms of Service, and could be blocked.
        The Terapeak API is not being used due to cost constraints.
        
        Args:
            search_terms: Dictionary containing search terms (name, brand, model, upc)
            
        Returns:
            Dictionary containing eBay pricing information
        """
        try:
            # Construct search query
            search_query = self._construct_ebay_search_query(search_terms)
            
            # Get active listings data
            active_data = self._scrape_ebay_active_listings(search_query)
            time.sleep(2)  # Be nice to eBay's servers
            
            # Get sold listings data
            sold_data = self._scrape_ebay_sold_listings(search_query)
            
            # Combine and return results
            return {
                'ebay_lowest_sold': sold_data.get('lowest_price'),
                'ebay_average_sold': sold_data.get('average_price'),
                'ebay_highest_sold': sold_data.get('highest_price'),
                'ebay_lowest_listed': active_data.get('lowest_price'),
                'ebay_average_listed': active_data.get('average_price'),
                'ebay_highest_listed': active_data.get('highest_price'),
                'ebay_average_shipping': active_data.get('average_shipping'),
                'ebay_active_listings_count': active_data.get('listing_count')
            }
            
        except Exception as e:
            logger.error(f"Error researching eBay: {str(e)}")
            return {
                'ebay_lowest_sold': None,
                'ebay_average_sold': None,
                'ebay_highest_sold': None,
                'ebay_lowest_listed': None,
                'ebay_average_listed': None,
                'ebay_highest_listed': None,
                'ebay_average_shipping': None,
                'ebay_active_listings_count': 0
            }
    
    def _construct_ebay_search_query(self, search_terms: Dict[str, str]) -> str:
        """Construct eBay search query from search terms"""
        query_parts = []
        
        if search_terms.get('name'):
            query_parts.append(search_terms['name'])
        if search_terms.get('brand'):
            query_parts.append(search_terms['brand'])
        if search_terms.get('model'):
            query_parts.append(search_terms['model'])
        if search_terms.get('upc'):
            query_parts.append(search_terms['upc'])
        
        return ' '.join(query_parts)
    
    def _scrape_ebay_active_listings(self, search_query: str) -> Dict[str, Optional[float]]:
        """Scrape active eBay listings"""
        try:
            # Construct search URL
            url = f"https://www.ebay.com/sch/i.html?_nkw={search_query}&_sop=15"  # Sort by price + shipping
            
            response = self.session.get(url, timeout=self.scraping_config['request_timeout'])
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract prices and shipping costs
            prices = []
            shipping_costs = []
            
            # Look for items in the search results
            items = soup.find_all('div', class_='s-item__info')
            
            for item in items[:50]:  # Limit to first 50 results
                try:
                    # Extract price
                    price_element = item.find('span', class_='s-item__price')
                    if price_element:
                        price_text = price_element.text.strip()
                        price = self._extract_price(price_text)
                        if price:
                            prices.append(price)
                    
                    # Extract shipping cost
                    shipping_element = item.find('span', class_='s-item__shipping')
                    if shipping_element:
                        shipping_text = shipping_element.text.strip()
                        shipping = self._extract_shipping_cost(shipping_text)
                        if shipping is not None:
                            shipping_costs.append(shipping)
                except Exception as e:
                    logger.debug(f"Error parsing item: {str(e)}")
                    continue
            
            # Calculate statistics
            if prices:
                return {
                    'lowest_price': min(prices),
                    'average_price': mean(prices),
                    'highest_price': max(prices),
                    'average_shipping': mean(shipping_costs) if shipping_costs else None,
                    'listing_count': len(prices)
                }
            else:
                return {
                    'lowest_price': None,
                    'average_price': None,
                    'highest_price': None,
                    'average_shipping': None,
                    'listing_count': 0
                }
            
        except requests.RequestException as e:
            logger.error(f"Error fetching active listings: {str(e)}")
            return {
                'lowest_price': None,
                'average_price': None,
                'highest_price': None,
                'average_shipping': None,
                'listing_count': 0
            }
    
    def _scrape_ebay_sold_listings(self, search_query: str) -> Dict[str, Optional[float]]:
        """Scrape sold eBay listings"""
        try:
            # Construct search URL for sold items
            url = f"https://www.ebay.com/sch/i.html?_nkw={search_query}&_sop=15&LH_Sold=1&LH_Complete=1"
            
            response = self.session.get(url, timeout=self.scraping_config['request_timeout'])
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract prices
            prices = []
            
            # Look for sold items in the search results
            items = soup.find_all('div', class_='s-item__info')
            
            for item in items[:50]:  # Limit to first 50 results
                try:
                    # Extract price
                    price_element = item.find('span', class_='s-item__price')
                    if price_element:
                        price_text = price_element.text.strip()
                        price = self._extract_price(price_text)
                        if price:
                            prices.append(price)
                except Exception as e:
                    logger.debug(f"Error parsing sold item: {str(e)}")
                    continue
            
            # Calculate statistics
            if prices:
                return {
                    'lowest_price': min(prices),
                    'average_price': mean(prices),
                    'highest_price': max(prices)
                }
            else:
                return {
                    'lowest_price': None,
                    'average_price': None,
                    'highest_price': None
                }
            
        except requests.RequestException as e:
            logger.error(f"Error fetching sold listings: {str(e)}")
            return {
                'lowest_price': None,
                'average_price': None,
                'highest_price': None
            }
    
    def _extract_price(self, price_text: str) -> Optional[float]:
        """Extract price from text"""
        try:
            # Remove currency symbol and commas
            price_text = price_text.replace('$', '').replace(',', '')
            
            # Handle price ranges (e.g., "$10.00 to $20.00")
            if 'to' in price_text:
                prices = [float(p.strip()) for p in price_text.split('to')]
                return mean(prices)
            
            # Handle single price
            return float(price_text.strip())
        except (ValueError, AttributeError):
            return None
    
    def _extract_shipping_cost(self, shipping_text: str) -> Optional[float]:
        """Extract shipping cost from text"""
        try:
            # Handle free shipping
            if 'free' in shipping_text.lower():
                return 0.0
            
            # Extract numeric value
            match = re.search(r'\$(\d+\.?\d*)', shipping_text)
            if match:
                return float(match.group(1))
            
            return None
        except (ValueError, AttributeError):
            return None

    def research_amazon(self, search_terms: Dict[str, str], direct_url: Optional[str] = None) -> Dict[str, Union[float, str, int, bool, None]]:
        """
        Scrape Amazon for product pricing and details using free methods only.
        
        DISCLAIMER: This function relies on scraping Amazon product pages, which is fragile,
        likely violates Amazon's Terms of Service, and could be blocked. Getting data like
        "# Sold This Month", "Frequently Returned Warning", and specific category ranks via
        scraping is unreliable or impossible. We attempt to find price, rating, and review count.
        Official Amazon APIs often have costs or limitations.
        
        Args:
            search_terms: Dictionary containing search terms (name, brand, model, upc)
            direct_url: Optional direct Amazon product URL if known
            
        Returns:
            Dictionary containing Amazon product information
        """
        try:
            # If we have a direct URL, use it; otherwise, try to find the product
            if direct_url:
                product_url = direct_url
            else:
                product_url = self._find_amazon_product(search_terms)
                if not product_url:
                    logger.warning("Could not find Amazon product page")
                    return self._get_default_amazon_results()
            
            # Scrape the product page
            return self._scrape_amazon_product_page(product_url)
            
        except Exception as e:
            logger.error(f"Error researching Amazon: {str(e)}")
            return self._get_default_amazon_results()
    
    def _find_amazon_product(self, search_terms: Dict[str, str]) -> Optional[str]:
        """
        Attempt to find an Amazon product page using search terms.
        This is complex and may not always succeed.
        """
        try:
            # Construct search query
            query_parts = []
            if search_terms.get('upc'):
                query_parts.append(search_terms['upc'])
            if search_terms.get('name'):
                query_parts.append(search_terms['name'])
            if search_terms.get('brand'):
                query_parts.append(search_terms['brand'])
            if search_terms.get('model'):
                query_parts.append(search_terms['model'])
            
            search_query = ' '.join(query_parts)
            encoded_query = quote_plus(search_query)
            
            # Search Amazon
            search_url = f"https://www.amazon.com/s?k={encoded_query}"
            response = self.session.get(search_url, timeout=self.scraping_config['request_timeout'])
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for product links in search results
            # Note: Amazon's HTML structure may change frequently
            product_links = soup.select('div[data-component-type="s-search-result"] a.a-link-normal')
            
            if product_links:
                # Get the first product link
                product_path = product_links[0].get('href')
                if product_path:
                    return f"https://www.amazon.com{product_path}"
            
            return None
            
        except requests.RequestException as e:
            logger.error(f"Error searching Amazon: {str(e)}")
            return None
    
    def _scrape_amazon_product_page(self, url: str) -> Dict[str, Union[float, str, int, bool, None]]:
        """
        Scrape product information from an Amazon product page.
        """
        try:
            response = self.session.get(url, timeout=self.scraping_config['request_timeout'])
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract price
            price = self._extract_amazon_price(soup)
            
            # Extract discount
            discount = self._extract_amazon_discount(soup)
            
            # Extract rating and review count
            rating, review_count = self._extract_amazon_rating(soup)
            
            # Check for frequently returned indicator
            frequently_returned = self._check_frequently_returned(soup)
            
            return {
                'amazon_price': price,
                'amazon_discount': discount,
                'amazon_star_rating': rating,
                'amazon_reviews_count': review_count,
                'amazon_frequently_returned': frequently_returned,
                'amazon_category_rating': None,  # Difficult to get reliably
                'amazon_subcategory_rating': None,  # Difficult to get reliably
                'amazon_sold_per_month': None  # Impossible to get reliably
            }
            
        except requests.RequestException as e:
            logger.error(f"Error scraping Amazon product page: {str(e)}")
            return self._get_default_amazon_results()
    
    def _extract_amazon_price(self, soup: BeautifulSoup) -> Optional[float]:
        """Extract price from Amazon product page"""
        try:
            # Try different price selectors (Amazon's HTML structure varies)
            price_selectors = [
                'span.a-price span.a-offscreen',  # Common price selector
                'span.a-price-whole',  # Alternative price selector
                'span#priceblock_ourprice',  # Another common selector
                'span#priceblock_dealprice'  # Deal price selector
            ]
            
            for selector in price_selectors:
                price_element = soup.select_one(selector)
                if price_element:
                    price_text = price_element.text.strip()
                    # Remove currency symbol and commas
                    price_text = price_text.replace('$', '').replace(',', '')
                    return float(price_text)
            
            return None
            
        except (ValueError, AttributeError) as e:
            logger.debug(f"Error extracting Amazon price: {str(e)}")
            return None
    
    def _extract_amazon_discount(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract discount from Amazon product page"""
        try:
            # Look for discount indicators
            discount_selectors = [
                'span.savingsPercentage',  # Common discount selector
                'span.a-size-large.a-color-price.savingPriceOverride'  # Alternative selector
            ]
            
            for selector in discount_selectors:
                discount_element = soup.select_one(selector)
                if discount_element:
                    return discount_element.text.strip()
            
            return None
            
        except (ValueError, AttributeError) as e:
            logger.debug(f"Error extracting Amazon discount: {str(e)}")
            return None
    
    def _extract_amazon_rating(self, soup: BeautifulSoup) -> Tuple[Optional[float], Optional[int]]:
        """Extract star rating and review count from Amazon product page"""
        try:
            # Extract star rating
            rating_element = soup.select_one('span.a-icon-alt')
            rating = None
            if rating_element:
                rating_text = rating_element.text.strip()
                match = re.search(r'(\d+\.?\d*) out of 5', rating_text)
                if match:
                    rating = float(match.group(1))
            
            # Extract review count
            review_element = soup.select_one('span#acrCustomerReviewText')
            review_count = None
            if review_element:
                review_text = review_element.text.strip()
                match = re.search(r'(\d+,?\d*)', review_text)
                if match:
                    review_count = int(match.group(1).replace(',', ''))
            
            return rating, review_count
            
        except (ValueError, AttributeError) as e:
            logger.debug(f"Error extracting Amazon rating: {str(e)}")
            return None, None
    
    def _check_frequently_returned(self, soup: BeautifulSoup) -> bool:
        """Check if product is marked as frequently returned"""
        try:
            # Look for frequently returned indicators
            indicators = [
                'frequently returned',
                'high return rate',
                'commonly returned'
            ]
            
            page_text = soup.get_text().lower()
            return any(indicator in page_text for indicator in indicators)
            
        except Exception as e:
            logger.debug(f"Error checking frequently returned status: {str(e)}")
            return False
    
    def _get_default_amazon_results(self) -> Dict[str, Union[float, str, int, bool, None]]:
        """Return default values for Amazon research results"""
        return {
            'amazon_price': None,
            'amazon_discount': None,
            'amazon_star_rating': None,
            'amazon_reviews_count': None,
            'amazon_frequently_returned': False,
            'amazon_category_rating': None,
            'amazon_subcategory_rating': None,
            'amazon_sold_per_month': None
        } 