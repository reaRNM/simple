# Auction Data Scraper and Profit Calculator

A Python CLI application for scraping auction data from HiBid, researching product pricing from eBay and Amazon, and calculating potential profits.

## Features

- Scrape auction data from HiBid URLs
- Research product pricing from eBay and Amazon
- Store data in a local SQLite database
- Calculate potential profits and recommended bids
- Export results to CSV
- Command-line interface for easy operation

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd auction-scraper
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Basic Commands

1. **Scrape an auction:**
```bash
python main.py scrape --url "https://hibid.com/auction-url" --output results.csv
```

2. **Lookup a product:**
```bash
# By UPC
python main.py lookup --upc "123456789012"

# By name
python main.py lookup --name "iPhone 12"

# By brand and model
python main.py lookup --brand "Apple" --model "iPhone 12"
```

3. **Add a product manually:**
```bash
python main.py add
```

4. **Edit an existing product:**
```bash
python main.py edit --upc "123456789012"
```

5. **View or modify configuration:**
```bash
# View current configuration
python main.py config --view

# Set a configuration value
python main.py config --set ebay_fee_percent 10.0
```

### Configuration

The application uses a configuration file (`config.py`) with the following default settings:

- `ebay_fee_percent`: 10.0% (eBay selling fee)
- `paypal_fee_percent`: 2.9% (PayPal processing fee)
- `paypal_fee_fixed`: 0.30 (PayPal fixed fee)
- `shipping_cost`: 10.00 (Default shipping cost)
- `tax_rate`: 0.0 (Tax rate for profit calculations)
- `min_profit_margin`: 20.0 (Minimum desired profit margin)
- `max_bid_percent`: 50.0 (Maximum bid as percentage of lowest sold price)

## Limitations and Disclaimers

1. **Scraping Limitations:**
   - The scraper relies on the structure of HiBid, eBay, and Amazon websites
   - Website changes may break the scraping functionality
   - Rate limiting is implemented to avoid overwhelming servers
   - Some data points may be difficult to obtain through free methods

2. **Legal Considerations:**
   - Web scraping may violate terms of service of some websites
   - Check the terms of service before using the scraper
   - The application is for educational purposes only

3. **Data Accuracy:**
   - Pricing data is based on public listings and may not reflect actual market conditions
   - Profit calculations are estimates and may not account for all costs
   - Research results depend on available listings and search terms

4. **Technical Limitations:**
   - Requires Python 3.x
   - Internet connection required for scraping and research
   - Local SQLite database for data storage
   - No API keys required (uses public data only)

## Error Handling and Logging

- The application includes comprehensive error handling
- Logs are stored in the `logs` directory
- Debug information is available in log files
- User-friendly error messages are displayed in the console

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support, please open an issue in the repository or contact the maintainers. 