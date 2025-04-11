from typing import Optional, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProfitCalculator:
    def __init__(self, config):
        self.config = config

    def calculate(self, upc: Optional[str] = None):
        """Calculate profit margins for products."""
        try:
            if upc:
                # Calculate for specific product
                product = self.db.get_product_by_upc(upc)
                if product:
                    self._calculate_product(product)
                else:
                    logger.warning(f"Product with UPC {upc} not found")
            else:
                # Calculate for all products
                products = self.db.get_all_products()
                for product in products:
                    self._calculate_product(product)
        except Exception as e:
            logger.error(f"Error calculating profits: {str(e)}")

    def _calculate_product(self, product: Dict[str, Any]) -> None:
        """Calculate profit margins for a single product."""
        try:
            # Get research data
            research_data = product.get('research_data', {})
            
            # Calculate grand average price
            grand_avg = self.calculate_grand_average_price(
                research_data.get('ebay_avg_sold'),
                research_data.get('ebay_avg_listed'),
                research_data.get('amazon_price')
            )
            
            # Calculate recommended highest bid
            recommended_bid = self.calculate_recommended_highest_bid(
                grand_avg,
                self.config
            )
            
            # Update product with calculations
            product['grand_average_price'] = grand_avg
            product['recommended_highest_bid'] = recommended_bid
            
            # Save updated product
            self.db.add_or_update_product(product)
            
        except Exception as e:
            logger.error(f"Error calculating product {product.get('upc')}: {str(e)}")

    @staticmethod
    def calculate_grand_average_price(
        ebay_avg_sold: Optional[float],
        ebay_avg_listed: Optional[float],
        amazon_price: Optional[float]
    ) -> Optional[float]:
        """Calculate the grand average price from multiple sources."""
        try:
            prices = []
            if ebay_avg_sold:
                prices.append(ebay_avg_sold)
            if ebay_avg_listed:
                prices.append(ebay_avg_listed)
            if amazon_price:
                prices.append(amazon_price)
            
            if not prices:
                return None
                
            return sum(prices) / len(prices)
        except Exception as e:
            logger.error(f"Error calculating grand average price: {str(e)}")
            return None

    def calculate_recommended_highest_bid(
        self,
        grand_average_price: Optional[float],
        config: Any
    ) -> Optional[float]:
        """Calculate the recommended highest bid based on average price and config."""
        try:
            if not grand_average_price:
                return None
                
            # Calculate maximum bid as percentage of average price
            max_bid = grand_average_price * (config.max_bid_percent / 100)
            
            # Ensure bid doesn't exceed average price
            return min(max_bid, grand_average_price)
        except Exception as e:
            logger.error(f"Error calculating recommended bid: {str(e)}")
            return None

    def calculate_current_profit_margin(
        self,
        current_bid: float,
        grand_average_price: Optional[float],
        config: Any
    ) -> Optional[float]:
        """Calculate current profit margin based on bid and average price."""
        try:
            if not grand_average_price:
                return None
                
            # Calculate potential profit
            potential_profit = grand_average_price - current_bid
            
            # Calculate profit margin percentage
            profit_margin = (potential_profit / grand_average_price) * 100
            
            return profit_margin
        except Exception as e:
            logger.error(f"Error calculating profit margin: {str(e)}")
            return None 