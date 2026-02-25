"""
Currency conversion module with Redis caching.

This module handles fetching exchange rates from api.exchangerate.host
and caching them in Redis to minimize API calls. It also fetches the target
currency from PostgreSQL database based on business_id.
"""

import os
import json
import requests
import redis
from datetime import datetime, timedelta
from typing import Dict, Optional
import psycopg2
from dotenv import load_dotenv, find_dotenv

# Load environment variables
load_dotenv(find_dotenv())


class CurrencyConverter:
    """
    Currency converter with Redis caching.

    Features:
    - Fetches exchange rates from api.exchangerate.host
    - Caches rates in Redis for 24 hours
    - Fetches target currency from PostgreSQL
    - Supports batch conversion
    """

    # Exchange rate API endpoint
    API_BASE_URL = "https://api.exchangerate.host"

    # Cache duration: 24 hours
    CACHE_DURATION_SECONDS = 24 * 60 * 60

    def __init__(self, business_id: str):
        """
        Initialize the currency converter.

        Args:
            business_id: The business ID (same as bucket name)
        """
        self.business_id = business_id

        # Initialize Redis connection
        redis_host = os.getenv("REDIS_HOST", "10.5.0.11")
        redis_port = int(os.getenv("REDIS_PORT", "6379"))

        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=0,
            decode_responses=True
        )

        # PostgreSQL connection parameters
        self.pg_config = {
            'host': os.getenv('POSTGRES_SERVER', 'localhost'),
            'database': os.getenv('POSTGRES_DB', 'pulse'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', '')
        }

        # Fetch target currency from database
        self.target_currency = self._fetch_target_currency()

        # Cache key prefix
        self.cache_key_prefix = f"exchange_rate:{self.target_currency}"

    def _fetch_target_currency(self) -> str:
        """
        Fetch the target currency for the business from PostgreSQL.

        Returns:
            str: Target currency code (e.g., 'USD', 'EUR', 'GBP')
        """
        try:
            conn = psycopg2.connect(**self.pg_config)
            cursor = conn.cursor()

            # Query to get business currency
            query = """
                SELECT business_currency
                FROM businesses
                WHERE business_id = %s
            """
            cursor.execute(query, (self.business_id,))
            result = cursor.fetchone()

            cursor.close()
            conn.close()

            if result and result[0]:
                return result[0].upper()
            else:
                # Default to USD if no currency is set
                print(f"⚠️  No currency found for business {self.business_id}, defaulting to USD")
                return "USD"

        except Exception as e:
            print(f"⚠️  Error fetching target currency from database: {e}")
            print(f"   Defaulting to USD")
            return "USD"

    def _get_cache_key(self, source_currency: str) -> str:
        """
        Generate Redis cache key for a currency pair.

        Args:
            source_currency: Source currency code

        Returns:
            str: Cache key
        """
        return f"{self.cache_key_prefix}:{source_currency.upper()}"

    def _get_cached_rate(self, source_currency: str) -> Optional[float]:
        """
        Get cached exchange rate from Redis.

        Args:
            source_currency: Source currency code

        Returns:
            Optional[float]: Exchange rate if cached, None otherwise
        """
        try:
            cache_key = self._get_cache_key(source_currency)
            cached_value = self.redis_client.get(cache_key)

            if cached_value:
                data = json.loads(cached_value)
                return float(data['rate'])

            return None

        except Exception as e:
            print(f"⚠️  Error reading from Redis cache: {e}")
            return None

    def _set_cached_rate(self, source_currency: str, rate: float):
        """
        Cache exchange rate in Redis.

        Args:
            source_currency: Source currency code
            rate: Exchange rate
        """
        try:
            cache_key = self._get_cache_key(source_currency)
            cache_data = {
                'rate': rate,
                'timestamp': datetime.now().isoformat(),
                'source': source_currency.upper(),
                'target': self.target_currency
            }

            self.redis_client.setex(
                cache_key,
                self.CACHE_DURATION_SECONDS,
                json.dumps(cache_data)
            )

        except Exception as e:
            print(f"⚠️  Error writing to Redis cache: {e}")

    def _fetch_exchange_rate(self, source_currency: str) -> Optional[float]:
        """
        Fetch exchange rate from api.exchangerate.host.

        Args:
            source_currency: Source currency code

        Returns:
            Optional[float]: Exchange rate if successful, None otherwise
        """
        try:
            # If source and target are the same, rate is 1.0
            if source_currency.upper() == self.target_currency:
                return 1.0

            # Build API URL for latest rates
            url = f"{self.API_BASE_URL}/latest"
            params = {
                'base': source_currency.upper(),
                'symbols': self.target_currency
            }

            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()

            if data.get('success', False) and 'rates' in data:
                rate = data['rates'].get(self.target_currency)
                if rate:
                    return float(rate)

            print(f"⚠️  Failed to fetch exchange rate: {data.get('error', 'Unknown error')}")
            return None

        except requests.exceptions.RequestException as e:
            print(f"⚠️  Error fetching exchange rate from API: {e}")
            return None
        except Exception as e:
            print(f"⚠️  Unexpected error fetching exchange rate: {e}")
            return None

    def get_exchange_rate(self, source_currency: str) -> Optional[float]:
        """
        Get exchange rate for converting from source to target currency.

        This method first checks Redis cache. If not found, it fetches from API
        and caches the result.

        Args:
            source_currency: Source currency code (e.g., 'USD', 'EUR')

        Returns:
            Optional[float]: Exchange rate if available, None otherwise
        """
        # Check cache first
        cached_rate = self._get_cached_rate(source_currency)
        if cached_rate is not None:
            return cached_rate

        # Fetch from API
        rate = self._fetch_exchange_rate(source_currency)

        # Cache the result if successful
        if rate is not None:
            self._set_cached_rate(source_currency, rate)

        return rate

    def convert_price(self, price: float, source_currency: str) -> Optional[float]:
        """
        Convert a price from source currency to target currency.

        Args:
            price: Price value to convert
            source_currency: Source currency code

        Returns:
            Optional[float]: Converted price, or None if conversion fails
        """
        if price is None or price == 0:
            return price

        rate = self.get_exchange_rate(source_currency)

        if rate is None:
            return None

        return price * rate

    def get_target_currency(self) -> str:
        """
        Get the target currency for this converter.

        Returns:
            str: Target currency code
        """
        return self.target_currency


def test_currency_converter():
    """
    Test function for currency converter.
    """
    print("Testing Currency Converter...")
    print("=" * 60)

    # Test with a sample business ID
    business_id = "test-business-1"

    converter = CurrencyConverter(business_id)
    print(f"Target Currency: {converter.get_target_currency()}")

    # Test conversion
    test_currencies = ['USD', 'EUR', 'GBP', 'JPY']
    test_price = 100.0

    print(f"\nConverting {test_price} from various currencies to {converter.target_currency}:")
    print("-" * 60)

    for currency in test_currencies:
        rate = converter.get_exchange_rate(currency)
        converted = converter.convert_price(test_price, currency)

        if rate and converted:
            print(f"{currency}: Rate={rate:.4f}, {test_price} {currency} = {converted:.2f} {converter.target_currency}")
        else:
            print(f"{currency}: Failed to convert")

    print("=" * 60)
    print("Testing complete!")


if __name__ == "__main__":
    test_currency_converter()
