import requests
import pandas as pd
import time

API_KEY = "cc8f98b35b571e72a0a3ce9ddc6d3757"
UNITS = "metric"
STATE_CODE = "IN-TN"  # Tamil Nadu state code

# List of major districts in Tamil Nadu with representative cities
DISTRICTS = {
    "Chennai": "Chennai",
    "Coimbatore": "Coimbatore",
    "Madurai": "Madurai",
    "Tiruchirappalli": "Tiruchirappalli",
    "Salem": "Salem",
    "Tirunelveli": "Tirunelveli",
    "Thanjavur": "Thanjavur",
    "Vellore": "Vellore",
    "Erode": "Erode",
    "Dindigul": "Dindigul",
    # Add more districts as needed
}

def get_weather_data(district, city):
    base_url = "http://api.openweathermap.org/data/2.5/forecast"
    params = {
        "q": f"{city},{STATE_CODE}",
        "units": UNITS,
        "appid": API_KEY
    }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"Error for {district}: {err}")
        return None

def parse_weather_data(json_data, district):
    forecast_list = []
    try:
        for entry in json_data['list']:
            forecast_list.append({
                'district': district,
                'timestamp': entry['dt_txt'],
                'temp': entry['main']['temp'],
                'feels_like': entry['main']['feels_like'],
                'temp_min': entry['main']['temp_min'],
                'temp_max': entry['main']['temp_max'],
                'pressure': entry['main']['pressure'],
                'humidity': entry['main']['humidity'],
                'wind_speed': entry['wind']['speed'],
                'wind_deg': entry['wind']['deg'],
                'clouds': entry['clouds']['all'],
                'weather_main': entry['weather'][0]['main'],
                'weather_desc': entry['weather'][0]['description']
            })
        return forecast_list
    except KeyError as e:
        print(f"Data parsing error for {district}: {e}")
        return []

# Main collection process
all_data = []

for district, city in DISTRICTS.items():
    print(f"Fetching data for {district}...")
    data = get_weather_data(district, city)
    
    if data and data['cod'] == '200':
        parsed_data = parse_weather_data(data, district)
        all_data.extend(parsed_data)
    else:
        print(f"Failed to fetch data for {district}")
    
    # Respect API rate limits (60 calls/minute)
    time.sleep(1.2)  # 1.2 second delay between requests

# Create DataFrame and save to CSV
df = pd.DataFrame(all_data)
df['timestamp'] = pd.to_datetime(df['timestamp'])
df.sort_values(['district', 'timestamp'], inplace=True)

# Save raw data
df.to_csv('tamilnadu_weather_forecast.csv', index=False)
print(f"Data collection complete. Saved {len(df)} records.")

# Display sample
print("\nSample data:")
print(df[['district', 'timestamp', 'temp', 'weather_desc']].head(10))