import requests
import pandas as pd

# Function to extract weather data from Open-Meteo API
# fetches hourly temperature and precipitation for London
# from the Open-Meteo API 
# converted it as a pandas DataFrame.

def extract_weather_data():
    url = (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=51.5072"
        "&longitude=-0.1276"
        "&hourly=temperature_2m,precipitation"
    )

    response = requests.get(url)
    response.raise_for_status()

    data = response.json()
    hourly = data["hourly"]

    df = pd.DataFrame(hourly)
    return df


if __name__ == "__main__":
    df = extract_weather_data()
    print(df.head())
