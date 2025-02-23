from pymongo import MongoClient
import math

# --- MongoDB Connection ---
source_client = MongoClient("mongodb://localhost:27017/")
source_db = source_client["traffic_data_db"]  # Source database name
source_collection = source_db["traffic_data_csv2"]  # Source collection name (reading from csv2 now)

destination_client = MongoClient("mongodb://localhost:27017/")
destination_db = destination_client["processed_traffic_data_db"]  # New database name
destination_collection = destination_db["processed_impressions"]  # New collection name

# --- Weather multipliers (same as before) ---
weather_multipliers = {
    "Clear": 1.0,
    "Rainy": 0.8,
    "Foggy": 0.6,
    "Snowy": 0.7
}

def calculate_average_impressions(trips_volume, segment_length, match_dir, weather):
    """
    Calculates average impressions. (Same function as before)
    """
    base_impressions = trips_volume * (segment_length / 1000)  # Segment length in kilometers

    if match_dir == 1:
        impressions = base_impressions * 1.5
    elif match_dir == 2:
        impressions = base_impressions * 0.9
    else:
        impressions = base_impressions * 1.2

    weather_multiplier = weather_multipliers.get(weather, 1.0)
    impressions *= weather_multiplier
    return round(impressions)

def get_representative_coordinate(geom_data):
    """
    Parses LINESTRING geom_data and returns the average longitude and latitude. (Same function as before)
    """
    if geom_data and geom_data != "Unknown":
        try:
            if geom_data.startswith("LINESTRING (") and geom_data.endswith(")"):
                coords_str = geom_data[len("LINESTRING ("):-1]
                coords_pairs_str = coords_str.split(', ')
                coords = []
                for pair_str in coords_pairs_str:
                    try:
                        lon_str, lat_str = pair_str.strip().split(' ')
                        lon = float(lon_str)
                        lat = float(lat_str)
                        coords.append((lon, lat))
                    except ValueError:
                        print(f"Warning: Could not parse coordinate pair: {pair_str}")
                        continue

                if coords:
                    avg_lon = sum(lon for lon, lat in coords) / len(coords)
                    avg_lat = sum(lat for lon, lat in coords) / len(coords)
                    return avg_lon, avg_lat
                else:
                    print("No valid coordinates found in geom_data after parsing.")
                    return None, None
            else:
                print("Warning: Unexpected geom_data format. Cannot parse as LINESTRING.")
                return None, None
        except Exception as e:
            print(f"Error parsing geom_data: {e}")
            return None, None
    else:
        print("No geom_data available or geom_data is 'Unknown'.")
        return None, None

def process_and_store_data(batch_size=50000):
    """
    Fetches data from source collection, processes it, and stores in the destination collection in batches.
    """
    routes = source_collection.find()
    processed_documents = []
    total_processed_count = 0

    for route in routes:
        segment_name_parts = route["segment_name"].split(" / ")
        if len(segment_name_parts) >= 3:
            osm_id = segment_name_parts[1]
            sub_segment_id = segment_name_parts[2]
            combined_id = f"{osm_id} / {sub_segment_id}"
        else:
            print(f"Warning: Unexpected segment_name format: {route['segment_name']}. Skipping.")
            continue # Skip this document if segment_name is not in the expected format

        traffic_volume = route["trips_volume"]
        segment_length = route["segment_length_m"]
        match_dir = route["match_dir"]
        weather = "Clear" # Default weather for processing all data
        geom = route.get("geometry", "Unknown")

        avg_impressions = calculate_average_impressions(traffic_volume, segment_length, match_dir, weather)
        representative_coord = get_representative_coordinate(geom)
        longitude, latitude = representative_coord if representative_coord else (None, None)

        processed_doc = {
            "combined_id": combined_id,
            "coordinates": [longitude, latitude] if longitude is not None and latitude is not None else None, # Store as list, None if extraction fails
            "traffic_volume": traffic_volume,
            "line_segment": geom,
            "average_impressions": round(avg_impressions)
        }
        processed_documents.append(processed_doc)
        total_processed_count += 1

        if total_processed_count % batch_size == 0:
            if processed_documents:
                destination_collection.insert_many(processed_documents)
                print(f"Inserted {total_processed_count} records into MongoDB collection...")
                processed_documents = [] # Clear the batch

    # Insert any remaining documents after the loop
    if processed_documents:
        destination_collection.insert_many(processed_documents)
        print(f"Inserted final {total_processed_count} records into MongoDB collection...")

    print(f"Successfully processed and stored {total_processed_count} documents in 'processed_impressions' collection in 'processed_traffic_data_db'.")


if __name__ == "__main__":
    print("Processing and inserting data into MongoDB in batches...")
    process_and_store_data()
    source_client.close()
    destination_client.close()
    print("Data processing and storage complete!")