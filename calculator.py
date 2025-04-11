from database import Database
from typing import Optional, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProfitCalculator:
    def __init__(self, config):
        self.config = config
        self.db = Database(config)
    
    def calculate(self, upc: Optional[str] = None):
        """Calculate profit margins for items"""
        try:
            if upc:
                # Calculate for specific item
                product = self.db.get_product_by_upc(upc)
                if product:
                    self._calculate_product(product)
            else:
                # Calculate for all items
                products = self.db.get_all_products()
                for product in products:
                    self._calculate_product(product)
            
        except Exception as e:
            logger.error(f"Error in calculate: {str(e)}")
        finally:
            self.db.close()
    
    def _calculate_product(self, product: Dict[str, Any]) -> None:
        """Calculate metrics for a single product"""
        try:
            # Calculate grand average price
            grand_avg = self.calculate_grand_average_price(
                product.get('ebay_average_sold'),
                product.get('ebay_average_listed'),
                product.get('amazon_price')
            )
            
            # Calculate recommended highest bid
            recommended_bid = self.calculate_recommended_highest_bid(grand_avg, self.config)
            
            # Update product in database
            update_data = {
                'upc': product['upc'],
                'grand_average_price': grand_avg,
                'recommended_highest_bid': recommended_bid
            }
            self.db.add_or_update_product(update_data)
            
            logger.info(f"Calculated metrics for product {product['upc']}: "
                       f"Grand Average: ${grand_avg:.2f}, "
                       f"Recommended Bid: ${recommended_bid:.2f}")
            
        except Exception as e:
            logger.error(f"Error calculating metrics for product {product.get('upc')}: {str(e)}")
    
    @staticmethod
    def calculate_grand_average_price(
        ebay_avg_sold: Optional[float],
        ebay_avg_listed: Optional[float],
        amazon_price: Optional[float]
    ) -> Optional[float]:
        """
        Calculate the grand average price from available price points.
        
        Args:
            ebay_avg_sold: Average sold price on eBay
            ebay_avg_listed: Average listed price on eBay
            amazon_price: Current price on Amazon
            
        Returns:
            Grand average price or None if no valid prices available
        """
        prices = []
        
        if ebay_avg_sold is not None:
            prices.append(ebay_avg_sold)
        if ebay_avg_listed is not None:
            prices.append(ebay_avg_listed)
        if amazon_price is not None:
            prices.append(amazon_price)
        
        if not prices:
            return None
        
        return sum(prices) / len(prices)
    
    def calculate_recommended_highest_bid(
        self,
        grand_average_price: Optional[float],
        config: Any
    ) -> Optional[float]:
        """
        Calculate the recommended highest bid based on grand average price and fees.
        
        Formula:
        Recommended Bid = (Grand Average Price * (1 - eBay Listing Fee % - Promote Listing Fee % - Lowest Profit Margin %)) 
                         - eBay Seller Fee - Ship to Me Cost) 
                         / (1 + Buyer Premium % + Sales Tax %)
        
        Args:
            grand_average_price: The grand average price
            config: Configuration object containing fee rates
            
        Returns:
            Recommended highest bid or None if calculation not possible
        """
        if grand_average_price is None:
            return None
        
        try:
            # Get fee rates from config
            ebay_listing_fee = config.get('ebay_listing_fee')  # 13%
            promote_listing_fee = config.get('promote_listing_fee')  # 2%
            lowest_profit_margin = config.get('lowest_profit_margin')  # 35%
            ebay_seller_fee = config.get('ebay_seller_fee')  # $0.40
            ship_to_me_cost = config.get('ship_to_me_cost')  # $0
            buyer_premium = config.get('auction_buyer_premium')  # 15%
            sales_tax = config.get('texas_sales_tax')  # 8.25%
            
            # Calculate numerator
            price_multiplier = 1 - ebay_listing_fee - promote_listing_fee - lowest_profit_margin
            numerator = (grand_average_price * price_multiplier) - ebay_seller_fee - ship_to_me_cost
            
            # Calculate denominator
            denominator = 1 + buyer_premium + sales_tax
            
            if denominator == 0:
                logger.error("Invalid fee rates: division by zero")
                return None
            
            return numerator / denominator
            
        except Exception as e:
            logger.error(f"Error calculating recommended bid: {str(e)}")
            return None
    
    def calculate_current_profit_margin(
        self,
        current_bid: float,
        grand_average_price: Optional[float],
        config: Any
    ) -> Optional[float]:
        """
        Calculate the current profit margin based on current bid.
        
        Formula:
        Profit = (Grand Average Price * (1 - eBay Listing Fee % - Promote Listing Fee %)) 
                 - eBay Seller Fee 
                 - (Current Bid * (1 + Buyer Premium % + Sales Tax %)) 
                 - Ship to Me Cost
        Margin = Profit / Grand Average Price
        
        Args:
            current_bid: Current bid amount
            grand_average_price: The grand average price
            config: Configuration object containing fee rates
            
        Returns:
            Profit margin as a percentage or None if calculation not possible
        """
        if grand_average_price is None or grand_average_price == 0:
            return None
        
        try:
            # Get fee rates from config
            ebay_listing_fee = config.get('ebay_listing_fee')  # 13%
            promote_listing_fee = config.get('promote_listing_fee')  # 2%
            ebay_seller_fee = config.get('ebay_seller_fee')  # $0.40
            buyer_premium = config.get('auction_buyer_premium')  # 15%
            sales_tax = config.get('texas_sales_tax')  # 8.25%
            ship_to_me_cost = config.get('ship_to_me_cost')  # $0
            
            # Calculate total cost
            total_cost = current_bid * (1 + buyer_premium + sales_tax) + ship_to_me_cost
            
            # Calculate potential revenue
            revenue_multiplier = 1 - ebay_listing_fee - promote_listing_fee
            potential_revenue = (grand_average_price * revenue_multiplier) - ebay_seller_fee
            
            # Calculate profit and margin
            profit = potential_revenue - total_cost
            margin = (profit / grand_average_price) * 100
            
            return margin
            
        except Exception as e:
            logger.error(f"Error calculating current profit margin: {str(e)}")
            return None 