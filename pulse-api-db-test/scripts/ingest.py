import os
import csv
from pymongo import MongoClient

MONGO_URI = "mongodb://root:rootpassword@127.0.0.1:27017/?authSource=admin&directConnection=true"
DB_NAME   = "pulse-test"
FILES_DIR = "/data/import-files"

client = MongoClient(MONGO_URI)
db     = client[DB_NAME]

for filename in os.listdir(FILES_DIR):
    if not filename.endswith(".csv"):
        continue

    collection_name = os.path.splitext(filename)[0]   # customers.csv → customers
    filepath        = os.path.join(FILES_DIR, filename)

    with open(filepath, newline="", encoding="utf-8") as f:
        reader  = csv.DictReader(f)
        records = list(reader)

    if not records:
        print(f"[SKIP] {filename} is empty.")
        continue

    collection = db[collection_name]
    collection.drop()                            # re-ingest cleanly on each restart
    result = collection.insert_many(records)
    print(f"[OK] {filename} → collection '{collection_name}' ({len(result.inserted_ids)} docs)")

client.close()
print("All files ingested.")