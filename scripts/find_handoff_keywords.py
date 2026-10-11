import pandas as pd
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client["tickets_db"]

# List all original brand collections
brand_collections = [
]

# Regex pattern for handoff/agent keywords (case-insensitive)
handoff_regex = r"(agent|human|transfer|support|talk to someone|speak with)"

results = []

for coll_name in brand_collections:
    brand_name = coll_name.replace("tickets_", "")
    collection = db[coll_name]
    
    total_brand_tickets = collection.count_documents({})
    
    # Query tickets where at least one message body matches the handoff regex
    # Note: Assumes messages are stored as an array of objects containing a 'body' string
    matching_tickets = collection.count_documents({
        "messages.body": {"$regex": handoff_regex, "$options": "i"}
    })
    
    percentage = (matching_tickets / total_brand_tickets * 100) if total_brand_tickets > 0 else 0
    
    results.append({
        "brand": brand_name,
        "total_tickets": total_brand_tickets,
        "tickets_with_handoff_keywords": matching_tickets,
        "handoff_keyword_rate_%": round(percentage, 2)
    })

df_handoff = pd.DataFrame(results)

print("==================================================")
print("🔍 RAW TICKETS WITH AGENT/HANDOFF KEYWORDS")
print("==================================================\n")
print(df_handoff.to_string(index=False))