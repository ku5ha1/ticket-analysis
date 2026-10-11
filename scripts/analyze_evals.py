import pandas as pd
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client["tickets_db"]
collection = db["source_data"]

# Pull all evaluated documents
docs = list(collection.find({"evaluation.intent": {"$exists": True}}))

records = []
for doc in docs:
    eval_data = doc.get("evaluation", {})
    records.append({
        "brand": doc.get("brand"),
        "intent": eval_data.get("intent"),
        "sub_intent": eval_data.get("sub_intent"),
        "resolved_by_bot": eval_data.get("resolved_by_bot"),
        "bot_failure_reason": eval_data.get("bot_failure_reason"),
        "requires_human": eval_data.get("requires_human"),
        "customer_sentiment": eval_data.get("customer_sentiment"),
        "urgency": eval_data.get("urgency"),
        "confidence_score": eval_data.get("confidence_score"),
        "reasoning": eval_data.get("reasoning")
    })

df = pd.DataFrame(records)

print("==================================================")
print("🔍 INTENT VS. HUMAN ESCALATION BREAKDOWN")
print("==================================================\n")

# Check which intents trigger the highest human handoffs
intent_escalation = df.groupby("intent").agg(
    total_tickets=("requires_human", "count"),
    escalated_count=("requires_human", lambda x: x.sum())
)
intent_escalation["escalation_rate_%"] = (intent_escalation["escalated_count"] / intent_escalation["total_tickets"] * 100).round(2)
print(intent_escalation.sort_values(by="total_tickets", ascending=False))

print("\n==================================================")
print("⚠️ BOT FAILURE REASONS (DETAILED)")
print("==================================================\n")
print(df["bot_failure_reason"].value_counts().head(10))

print("\n==================================================")
print("😤 CUSTOMER SENTIMENT ACROSS EVALUATED TICKETS")
print("==================================================\n")
print(df["customer_sentiment"].value_counts())