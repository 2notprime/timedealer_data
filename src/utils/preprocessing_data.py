from datetime import datetime
import re

def parse_date(raw_date: str):
    if not raw_date:
        return None, None
    parts = raw_date.split("/")
    if len(parts) == 3:  # DD/MM/YYYY
        day, month, year = parts
        date_value = datetime(int(year), int(month), int(day)).date()
        precision = "day"
    elif len(parts) == 2:  # MM/YYYY
        month, year = parts
        date_value = datetime(int(year), int(month), 1).date()
        precision = "month"
    elif len(parts) == 1:  # YYYY
        year = parts[0]
        date_value = datetime(int(year), 1, 1).date()
        precision = "year"
    else:
        raise ValueError(f"Invalid date format: {raw_date}")
    return date_value, precision

def normalize_ref(ref: str) -> str:
    return re.sub(r'[^a-zA-Z0-9]', '', ref)