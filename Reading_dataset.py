import pandas as pd
from pymongo import MongoClient

# Path to your CSV file
csv_path = 'traffic_data_sample.csv'

# Read the CSV file into a DataFrame
df = pd.read_csv(csv_path)

# MongoDB Database connection
client = MongoClient("mongodb://localhost:27017/")  # Adjust the URI as necessary
db = client['traffic_data_db']  # Database name
collection = db['traffic_data_csv2']  # New collection name for CSV data

# Convert the DataFrame to a list of dictionaries (to insert into MongoDB)
records = df.to_dict(orient="records")

# Insert the data into MongoDB
def batch_insert(data, collection, batch_size=50000):
    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        collection.insert_many(batch)
        print(f"Inserted {i + len(batch)} records into MongoDB collection...")

# Inserting CSV data into MongoDB
print("Inserting CSV data into MongoDB...")
batch_insert(records, collection)

print("Insertion complete!")

# Close the connection after operations
client.close()
