from pymongo import MongoClient

# MongoDB Database connection
client = MongoClient("mongodb://localhost:27017/")  # Adjust the URI as necessary
db = client['traffic_data_db']  # Existing database
collection = db['traffic_data_csv2']  # Collection with the data

# Columns to remove
columns_to_drop = ['id', 'mode', 'country', 'osm_id', 'segment_id', 'day_type', 'day_part', 
                   'trips_sample_count', 'created_at', 'updated_at']

# Use $unset to remove those columns from all documents in the collection
unset_query = {col: "" for col in columns_to_drop}  # Prepare the $unset query

# Apply the unset operation to all documents
collection.update_many({}, {'$unset': unset_query})

print("Columns dropped from the collection!")

# Close the connection after operations
client.close()