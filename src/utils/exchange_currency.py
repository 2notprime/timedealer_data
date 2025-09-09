import requests

def get_exchange_rate_usd(currency_code: str) -> float:
    url = "https://open.er-api.com/v6/latest/USD"
    response = requests.get(url)
    data = response.json()

    # Kiểm tra có key "rates" không
    if "rates" not in data:
        raise ValueError("API response không có 'rates'")

    rates = data["rates"]
    currency_code = currency_code.upper()

    # Lấy tỷ giá
    if currency_code in rates:
        return rates[currency_code]
    else:
        raise ValueError(f"Không tìm thấy tỷ giá cho {currency_code}")

