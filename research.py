import requests
from bs4 import BeautifulSoup
import time
import re
from typing import Dict, List, Optional, Tuple, Union
import logging
from statistics import mean
from database import Database
from urllib.parse import quote_plus
import traceback
import json
from sklearn.ensemble import RandomForestRegressor
import numpy as np
import joblib
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PriceResearch:
    """Handles price research using Algopix API and ML predictions"""
    
    def __init__(self, config):
        """Initialize with configuration"""
        self.config = config
        self.scraping_config = config.get_scraping_config()
        self.session = requests.Session()
        self.model = None
        self.model_path = 'price_model.joblib'
        self._load_or_train_model()
    
    def _load_or_train_model(self):
        """Load existing model or train a new one"""
        try:
            if os.path.exists(self.model_path):
                self.model = joblib.load(self.model_path)
                logger.info("Loaded existing price prediction model")
            else:
                self.model = RandomForestRegressor(n_estimators=100, random_state=42)
                logger.info("Created new price prediction model")
        except Exception as e:
            logger.error(f"Error loading/training model: {str(e)}")
            self.model = RandomForestRegressor(n_estimators=100, random_state=42)
    
    def _get_algopix_data(self, search_terms: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Get market data from Algopix API"""
        try:
            # Construct search query
            query_parts = []
            if search_terms.get('upc'):
                query_parts.append(f"upc:{search_terms['upc']}")
            if search_terms.get('brand'):
                query_parts.append(f"brand:{search_terms['brand']}")
            if search_terms.get('model'):
                query_parts.append(f"model:{search_terms['model']}")
            
            search_query = ' '.join(query_parts)
            
            # Make API request
            url = "https://api.algopix.com/v3/market/search"
            headers = {
                'Authorization': f"Bearer {self.scraping_config.get('algopix_api_key', '')}",
                'Accept': 'application/json'
            }
            params = {
                'q': search_query,
                'marketplace': 'ebay',
                'limit': 50,  # Get last 50 sales
                'include': 'prices,competitors,trends'
            }
            
            response = self.session.get(url, headers=headers, params=params, timeout=30)
            if response.status_code != 200:
                logger.error(f"Algopix API error: {response.status_code}")
                return None
            
            data = response.json()
            market_data = data.get('market_data', {})
            
            if not market_data:
                return None
            
            # Extract pricing data
            prices = market_data.get('prices', {})
            competitors = market_data.get('competitors', {})
            trends = market_data.get('trends', {})
            
            return {
                'ebay_lowest_sold': prices.get('lowest_sold', 0.0),
                'ebay_average_sold': prices.get('average_sold', 0.0),
                'ebay_highest_sold': prices.get('highest_sold', 0.0),
                'ebay_active_count': competitors.get('active_listings', 0),
                'competitor_count': competitors.get('total_competitors', 0),
                'market_health': trends.get('market_health', 0.0),
                'price_trend': trends.get('price_trend', 'stable'),
                'demand_trend': trends.get('demand_trend', 'stable')
            }
            
        except Exception as e:
            logger.error(f"Error fetching Algopix data: {str(e)}")
            return None
    
    def _predict_price(self, product_data: Dict[str, Any]) -> Dict[str, float]:
        """Predict prices using ML model"""
        try:
            # Prepare features for prediction
            features = self._prepare_features(product_data)
            
            # Make prediction
            predicted_price = self.model.predict([features])[0]
            
            # Calculate price range based on historical data
            price_range = predicted_price * 0.2  # 20% range
            
            return {
                'ebay_lowest_sold': max(0, predicted_price - price_range),
                'ebay_average_sold': predicted_price,
                'ebay_highest_sold': predicted_price + price_range,
                'ebay_active_count': 0,  # Can't predict this
                'competitor_count': 0,  # Can't predict this
                'market_health': 0.0,  # Can't predict this
                'price_trend': 'unknown',  # Can't predict this
                'demand_trend': 'unknown'  # Can't predict this
            }
            
        except Exception as e:
            logger.error(f"Error making price prediction: {str(e)}")
            return {
                'ebay_lowest_sold': 0.0,
                'ebay_average_sold': 0.0,
                'ebay_highest_sold': 0.0,
                'ebay_active_count': 0,
                'competitor_count': 0,
                'market_health': 0.0,
                'price_trend': 'unknown',
                'demand_trend': 'unknown'
            }
    
    def _prepare_features(self, product_data: Dict[str, Any]) -> List[float]:
        """Prepare features for ML model"""
        # Convert product data into numerical features
        features = []
        
        # Add brand as one-hot encoded feature
        brand = product_data.get('brand', '').lower()
        common_brands = [
            'apple', 'samsung', 'sony', 'lg', 'microsoft', 'dell', 'hp', 'lenovo',
            'amazon', 'google', 'logitech', 'bose', 'jbl', 'anker', 'belkin'
        ]
        features.extend([1 if b == brand else 0 for b in common_brands])
        
        # Add condition as numerical value
        condition = product_data.get('condition', '').lower()
        condition_map = {
            'new': 1.0,
            'like new': 0.9,
            'open box': 0.8,
            'excellent': 0.8,
            'very good': 0.7,
            'good': 0.6,
            'acceptable': 0.5,
            'fair': 0.4,
            'poor': 0.3
        }
        features.append(condition_map.get(condition, 0.5))
        
        # Add damage indicator
        features.append(1.0 if product_data.get('damage', False) else 0.0)
        
        # Add missing items indicator
        features.append(1.0 if product_data.get('missing_items', False) else 0.0)
        
        # Add category-specific features
        category = product_data.get('category', '').lower()
        categories = [
            'electronics', 'computers', 'phones', 'tablets', 'gaming',
            'audio', 'smart home', 'wearables', 'accessories'
        ]
        features.extend([1 if c in category else 0 for c in categories])
        
        return features
    
    def research_ebay(self, search_terms: Dict[str, str]) -> Dict[str, Any]:
        """
        Research product prices using Algopix API and ML predictions.
        
        Args:
            search_terms: Dictionary containing name, brand, model, and UPC
            
        Returns:
            Dictionary containing eBay pricing data
        """
        try:
            # Try to get data from Algopix first
            algopix_data = self._get_algopix_data(search_terms)
            if algopix_data:
                return algopix_data
            
            # Fall back to ML prediction if API fails
            logger.info("Algopix API failed, using ML prediction")
            return self._predict_price(search_terms)
            
        except Exception as e:
            logger.error(f"Error in eBay research: {str(e)}")
            logger.debug(traceback.format_exc())
            return {
                'ebay_lowest_sold': 0.0,
                'ebay_average_sold': 0.0,
                'ebay_highest_sold': 0.0,
                'ebay_active_count': 0,
                'competitor_count': 0,
                'market_health': 0.0,
                'price_trend': 'unknown',
                'demand_trend': 'unknown'
            }
    
    def update_model(self, new_data: List[Dict[str, Any]]):
        """Update the ML model with new data"""
        try:
            if not new_data:
                return
            
            # Prepare training data
            X = [self._prepare_features(item) for item in new_data]
            y = [item.get('ebay_average_sold', 0.0) for item in new_data]
            
            # Update model
            self.model.fit(X, y)
            
            # Save updated model
            joblib.dump(self.model, self.model_path)
            logger.info("Model updated and saved")
            
        except Exception as e:
            logger.error(f"Error updating model: {str(e)}")
            logger.debug(traceback.format_exc())

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
            
            response = self._make_request(url)
            if not response:
                return prices
            
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
            
            response = self._make_request(url)
            if not response:
                return prices
            
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
            response = self._make_request(search_url)
            if not response:
                return None
            
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
            response = self._make_request(url)
            if not response:
                return self._get_default_amazon_results()
            
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