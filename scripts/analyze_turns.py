import pandas as pd
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client["tickets_db"]
collection = db["source_data"]

docs = list(collection.find({"evaluation.intent": {"$exists": True}}))

records = []
for doc in docs:
    eval_data = doc.get("evaluation", {})
    messages = doc.get("messages", [])
    records.append({
        "brand": doc.get("brand"),
        "total_messages": len(messages),
        "resolved_by_bot": eval_data.get("resolved_by_bot"),
        "requires_human": eval_data.get("requires_human")
    })

df = pd.DataFrame(records)

print("==================================================")
print("📊 CONVERSATION LENGTH (MESSAGE COUNT) COMPARISON")
print("==================================================\n")

comparison = df.groupby(["resolved_by_bot", "requires_human"])["total_messages"].agg(
    count="count",
    avg_messages="mean",
    median_messages="median",
    max_messages="max"
)
print(comparison)