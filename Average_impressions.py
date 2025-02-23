from pymongo import MongoClient
import math

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["traffic_data_db"]  # Correct database name
collection = db["traffic_data_csv2"]  # Correct collection name

# Weather multipliers
weather_multipliers = {
    "Clear": 1.0,
    "Rainy": 0.8,
    "Foggy": 0.6,
    "Snowy": 0.7
}

# Function to calculate average impressions with highway type consideration
def calculate_average_impressions(trips_volume, segment_length, match_dir, weather):
    # Base impressions calculation
    base_impressions = trips_volume * (segment_length / 1000)  # Segment length in kilometers
    
    # Adjust impressions based on match_dir
    if match_dir == 1:
        impressions = base_impressions * 1.5  # Forward traffic on the right side (higher visibility)
    elif match_dir == 2:
        impressions = base_impressions * .9  # Opposite direction (on the left side)
    else:
        impressions = base_impressions * 1.2  # Idle or low traffic would be the average of the two
    
    # Apply weather multiplier
    weather_multiplier = weather_multipliers.get(weather, 1.0)  # Default to 1.0 if weather is not recognized
    impressions *= weather_multiplier
    
    return impressions

# Function to search for routes and calculate impressions
def get_route_data(route_name, weather):
    # Escape special characters in the route name for regex
    escaped_route_name = route_name.replace("/", "\\/").replace(".", "\\.").replace("*", "\\*")
    
    # Query the database to retrieve all matching routes, with case-insensitive and partial matching
    query = {"segment_name": {"$regex": f".*{escaped_route_name}.*", "$options": "i"}}
    print(f"Query being executed: {query}")  # Debugging print
    
    routes = collection.find(query)
    routes_list = list(routes)
    
    if len(routes_list) > 0:
        # Dictionary to store aggregated data by store number
        store_data = {}
        
        for route in routes_list:
            # Extract store number and osm_id from segment_name (assuming format: "Route 85 / 13138206 / 12")
            segment_name_parts = route["segment_name"].split(" / ")
            if len(segment_name_parts) >= 3:
                store_number = segment_name_parts[2]  # Store number is the third part
                osm_id = segment_name_parts[1]  # OSM ID is the second part
            else:
                store_number = "Unknown"  # Fallback if the format is unexpected
                osm_id = "Unknown"
            
            # Calculate average impressions for this route
            traffic = route["trips_volume"]
            segment_length = route["segment_length_m"]
            match_dir = route["match_dir"]
            geom = route.get("geom", "Unknown")  # Retrieve geom data
            
            avg_impressions = calculate_average_impressions(traffic, segment_length, match_dir, weather)
            
            # Aggregate data by store number
            if store_number not in store_data:
                store_data[store_number] = {
                    "total_impressions": 0,
                    "count": 0,
                    "osm_id": osm_id,  # Store OSM ID
                    "geom": geom  # Store geometry data
                }
            
            store_data[store_number]["total_impressions"] += avg_impressions
            store_data[store_number]["count"] += 1
        
        # Calculate and print the absolute average impressions per store
        print("\nAbsolute Average Impressions per Store:")
        for store_number, data in store_data.items():
            absolute_avg_impressions = data["total_impressions"] / data["count"]
            print(f"Store {store_number}, OSM ID: {data['osm_id']}, Geometry: {data['geom']}, Avg Impressions: {round(absolute_avg_impressions)}")
    else:
        print("No data found for the specified route.")

# Get user input for route name and weather
route_name = input("Enter the route name: ")
weather = input("Enter the weather (Clear/Rainy/Foggy/Snowy): ").capitalize()

# Validate weather input
if weather not in weather_multipliers:
    print("Invalid weather input. Defaulting to Clear.")
    weather = "Clear"

get_route_data(route_name, weather)
