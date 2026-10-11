import pandas as pd
from pymongo import MongoClient

# Connect to local MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["tickets_db"]
collection = db["source_data"]

# Fetch all evaluated documents
cursor = collection.find({"evaluation.intent": {"$exists": True}})
docs = list(cursor)

if not docs:
    print("❌ No evaluated documents found!")
    exit()

# Flatten MongoDB documents into a Pandas DataFrame
records = []
for doc in docs:
    eval_data = doc.get("evaluation", {})
    records.append({
        "ticket_id": doc.get("ticket_id"),
        "brand": doc.get("brand"),
        "intent": eval_data.get("intent"),
        "sub_intent": eval_data.get("sub_intent"),
        "resolved_by_bot": eval_data.get("resolved_by_bot"),
        "bot_failure_reason": eval_data.get("bot_failure_reason"),
        "requires_human": eval_data.get("requires_human"),
        "sentiment": eval_data.get("customer_sentiment"),
        "urgency": eval_data.get("urgency"),
        "confidence_score": eval_data.get("confidence_score")
    })

df = pd.DataFrame(records)

print("==================================================")
print("📊 AI BOT PERFORMANCE SUMMARY BY BRAND")
print("==================================================\n")

# Brand Performance Summary
brand_summary = df.groupby("brand").agg(
    total_tickets=("ticket_id", "count"),
    resolved_by_bot=("resolved_by_bot", lambda x: x.sum()),
    human_escalations=("requires_human", lambda x: x.sum()),
    avg_confidence=("confidence_score", "mean")
)

brand_summary["bot_resolution_rate_%"] = (brand_summary["resolved_by_bot"] / brand_summary["total_tickets"] * 100).round(2)
brand_summary["human_escalation_rate_%"] = (brand_summary["human_escalations"] / brand_summary["total_tickets"] * 100).round(2)

print(brand_summary[["total_tickets", "bot_resolution_rate_%", "human_escalation_rate_%", "avg_confidence"]])

print("\n==================================================")
print("⚠️ TOP BOT FAILURE MODES (UNRESOLVED TICKETS)")
print("==================================================\n")

# Filter for bot failures
failed_tickets = df[df["resolved_by_bot"] == False]
failure_counts = failed_tickets["bot_failure_reason"].value_counts().head(10)
print(failure_counts)

print("\n==================================================")
print("🔥 TOP CUSTOMER INTENTS ACROSS BRANDS")
print("==================================================\n")

intent_counts = df["intent"].value_counts().head(10)
print(intent_counts)