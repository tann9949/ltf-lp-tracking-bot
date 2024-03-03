import requests

from .constant import Asset


def get_asset_price(asset: Asset) -> float:
    if asset in [Asset.USDC, Asset.USDT, Asset.DAI]:
        return 1.
    
    elif asset == Asset.WETH:
        return get_weth_price()
    
    elif asset == Asset.METIS:
        return get_metis_price()
    
    
def get_weth_price(ma_days: int = 30) -> float:
    return calculate_simple_moving_average("ethereum", days=ma_days)


def get_metis_price(ma_days: int = 30) -> float:
    return calculate_simple_moving_average("metis-token", days=ma_days)


def fetch_coingecko_price(crypto_name: str, days: int = 30):
    """
    Fetch historical price data for a given cryptocurrency from the CoinGecko API.

    :param crypto_name: Name of the cryptocurrency (e.g., 'ethereum', 'bitcoin').
    :param days: Number of days for which to fetch historical data.
    :return: Historical price data or None if an error occurs.
    """
    # Endpoint for the CoinGecko API to get historical data
    url = f"https://api.coingecko.com/api/v3/coins/{crypto_name}/market_chart"
    
    params = {
        'vs_currency': 'usd',
        'days': str(days),
        'interval': 'daily'
    }

    # Make a request to the API
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        return None
    
    
def calculate_simple_moving_average(crypto_name: str, days: int = 30):
    """
    Calculate the simple moving average for a given cryptocurrency over a specified number of days.

    :param crypto_name: Name of the cryptocurrency (e.g., 'ethereum', 'bitcoin').
    :param days: Number of days over which to calculate the moving average.
    :return: The simple moving average or an error message if data cannot be fetched.
    """
    data = fetch_coingecko_price(crypto_name, days)
    
    if data and 'prices' in data and len(data['prices']) >= days:
        # Extract the prices from the API response
        prices = [price for _, price in data['prices']]

        # Calculate the moving average manually
        moving_average = sum(prices[-days:]) / days

        return moving_average
    else:
        raise 
