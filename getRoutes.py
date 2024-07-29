import json
import googlemaps

API_KEY = "AIzaSyBv4Pl2ygmRCw-nbV9QF-tRaYnEIc0_tg8"

def get_user_origin(gmaps):
    while True:
        origin = input("Enter your starting location: ")
        geocode_result = gmaps.geocode(origin)
        if geocode_result:
            # print("Our origin: " + geocode_result[0]['formatted_address'])
            return origin
        else:
            print("Invalid location. Please enter a valid location.")


def write_json_route():
    print("Gathering route JSON's...")
    gmaps = googlemaps.Client(key=API_KEY)  
    # origin = get_user_origin(gmaps)  
    origin = "Colorado State University"
    
    ikon_resorts = [
        "Aspen, CO",
        "Steamboat, CO",
        "Winter Park, CO",
        "Copper Mountain, CO",
        "Arapahoe Basin, CO",
        "Eldora, CO"
    ]

    for resort in ikon_resorts:
        directions = gmaps.directions(origin, resort, waypoints=None, optimize_waypoints=False, alternatives=True)

        resort = resort.split(",")[0].replace(" ", "-")  # Get rid of the spaces
        # print("Writing route to {}...".format(resort))

        json_object = json.dumps(directions, indent=4)
        with open(resort + "-Route.json", "w") as outfile:
            outfile.write(json_object)

        # i = 1
        # for route in directions:
        #     print("\t{}. {} --- {} --> {}".format(i, origin, route['legs'][0]['duration']['text'], resort))
        
        # print()

    print("All routes have been written to JSON files.\n")

def main():
    write_json_route()

main()