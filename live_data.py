import requests
import pandas as pd
from datetime import datetime, date


cities = {
    "Delhi": {"lat": 28.6139, "lon": 77.2090},
    "Jaipur": {"lat": 26.9124, "lon": 75.7873},
    "Lucknow": {"lat": 26.8467, "lon": 80.9462},
    "Ahmedabad": {"lat": 23.0225, "lon": 72.5714},
    "Kolkata": {"lat": 22.5726, "lon": 88.3639},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777},
    "Pune": {"lat": 18.5204, "lon": 73.8567},
    "Hyderabad": {"lat": 17.3850, "lon": 78.4867},
    "Chennai": {"lat": 13.0827, "lon": 80.2707},
    "Bangalore": {"lat": 12.9716, "lon": 77.5946}
}
all_data = []
today = date.today()


url = "https://api.open-meteo.com/v1/forecast"

for city, coords in cities.items():
    params = {
        "latitude": coords["lat"],
        "longitude": coords["lon"],
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "apparent_temperature_max",
            "apparent_temperature_min",
            "precipitation_sum",
            "rain_sum",
            "weathercode",
            "wind_speed_10m_max",
            "wind_gusts_10m_max",
            "wind_direction_10m_dominant"
        ],
        "timezone": "Asia/Kolkata",
        "start_date": str(today),
        "end_date": str(today)
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        daily = response.json()["daily"]

        for i in range(len(daily["time"])):
            all_data.append({
                "city": city,
                "date": daily["time"][i],
                "temperature_2m_max": daily["temperature_2m_max"][i],
                "temperature_2m_min": daily["temperature_2m_min"][i],
                "apparent_temperature_max": daily["apparent_temperature_max"][i],
                "apparent_temperature_min": daily["apparent_temperature_min"][i],
                "precipitation_sum": daily["precipitation_sum"][i],
                "rain_sum": daily["rain_sum"][i],
                "weather_code": daily["weathercode"][i],
                "wind_speed_10m_max": daily["wind_speed_10m_max"][i],
                "wind_gusts_10m_max": daily["wind_gusts_10m_max"][i],
                "wind_direction_10m_dominant": daily["wind_direction_10m_dominant"][i]
            })

    except Exception as e:
        print(f"Failed to fetch data for {city}: {e}")

# Convert to Pandas DataFrame
df_live = pd.DataFrame(all_data)

from datetime import date

today = date.today().strftime("%Y%m%d")

output_path = f"/home/sunbeam/BD_project_mine/weather_today.csv"
df_live.to_csv(output_path, index=False,header=False)



#df_live.to_csv("home/hp/Desktop/Bigdata_project/BigData-Project/weather_dataset.csv", mode="a", index=False, header=False)



