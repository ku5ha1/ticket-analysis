from pymongo import MongoClient
import json

client = MongoClient("mongodb://localhost:27017")
db = client["tickets_db"]
collection = db["source_data"]

# Fetch all evaluated tickets
docs = list(collection.find({"evaluation.intent": {"$exists": True}}))

total_tickets = len(docs)
total_messages = 0
incoming_customer_msgs = 0
outgoing_bot_msgs = 0
outgoing_agent_msgs = 0
unknown_outgoing = 0

for doc in docs:
    messages = doc.get("messages", [])
    for m in messages:
        total_messages += 1
        body_raw = m.get("body", "")
        
        # Parse body if stored as a JSON string
        text_content = body_raw
        try:
            parsed = json.loads(body_raw)
            if isinstance(parsed, dict):
                text_content = parsed.get("text", body_raw)
        except:
            pass

        # Heuristic classification based on sender patterns in support bots:
        # 1. Customer messages are incoming (e.g. questions, tracking checks)
        # 2. Bot messages often contain automated greetings, templates, tracking links, or escalation notices ("Let me connect you to a human agent")
        # 3. Human agent messages are manual replies ("Yes sir what you want to know?", specific agent responses)
        
        # Note: You can also inspect specific keys if your DB schema stores 'sender' or 'from' explicitly.
        # Let's inspect the fields present in your message objects:
        pass

# Let's print out sample message keys to be 100% precise
if docs and docs[0].get("messages"):
    print("Sample message keys:", list(docs[0]["messages"][0].keys()))

print(f"Total evaluated tickets: {total_tickets}")
print(f"Total messages across all tickets: {total_messages}")