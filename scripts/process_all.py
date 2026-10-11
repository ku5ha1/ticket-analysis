import pandas as pd
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["tickets_db"]

# List all original brand collections
brand_collections = [
]

all_raw_records = []

# Loop through every brand collection and pull raw documents
for coll_name in brand_collections:
    brand_name = coll_name.replace("tickets_", "")
    collection = db[coll_name]
    
    for doc in collection.find({}, {"messages": 1}):
        messages = doc.get("messages", [])
        msg_count = len(messages)
        
        all_raw_records.append({
            "ticket_id": str(doc.get("_id")),
            "brand": brand_name,
            "total_messages": msg_count,
            "has_messages": msg_count > 0
        })

df_raw = pd.DataFrame(all_raw_records)

print("==================================================")
print("📈 RAW DATASET OVERVIEW (ALL TICKETS)")
print("==================================================\n")
print(f"Total raw tickets across all collections: {len(df_raw)}")
print(df_raw.groupby("brand")["total_messages"].describe())