import json
import requests

def create_json(latitude, longitude, name):
    user_agent = '455SemesterProject.com (denzel.hartshorn@colostate.edu)'
    headers = {'User-Agent': user_agent}
    points_url = f'https://api.weather.gov/points/{latitude},{longitude}'

    try:
        response = requests.get(points_url, headers=headers)
        response.raise_for_status()
        grid_info = response.json()
        forecast_url = grid_info['properties']['forecastHourly']
        forecast_response = requests.get(forecast_url, headers=headers)
        forecast_response.raise_for_status()
        info = forecast_response.json()
        json_object = json.dumps(info, indent=4)
        with open(name.replace(" ", "-") + "-Weather.json", "w") as outfile:
            outfile.write(json_object) 
        return
    except requests.RequestException as e:
        print(f"Error fetching weather data for {latitude}, {longitude}: {e}")
        return
    
if __name__ == "__main__":
    resort_locations = [
        ("Arapahoe Basin", 39.6342, -105.8715),
        ("Copper Mountain", 39.4884, -106.1519),
        ("Aspen", 39.1896, -106.9494),
        ("Steamboat", 40.4539, -106.7708),
        ("Eldora", 39.9388, -105.5820),
        ("Winter Park", 39.8629, -105.7787)
    ]
    choke_points = {
        "Aspen": {"Floyd Hill": (39.7289, -105.4179), "Vail Pass": (39.5293, -106.2195)},
        "Steamboat": {"Larmie": (41.3114, -105.5911), "Walden":(40.6913, -106.2770)},
        "Winter Park": {"Berthoud Pass": (39.7983, -105.7767)},
        "Copper Mountain": {"Floyd Hill": (39.7289, -105.4179), "Idaho Springs": (39.7378, -105.5024)},
        "Arapahoe Basin": {"Floyd Hill": (39.7289, -105.4179), "Idaho Springs": (39.7378, -105.5024)},
        "Eldora": {"Boulder": (40.0245, -105.3059)},
    }

    print("Gathering weather JSON's...")
    for i, (name, lat, lon) in enumerate(resort_locations, start=1):
        create_json(lat, lon, name)
        for point, (plat, plon) in choke_points[name].items():
            create_json(plat, plon, point)
    
    print("Weather JSON files have been written.\n")
