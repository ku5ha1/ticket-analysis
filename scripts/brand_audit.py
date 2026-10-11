import json
import pandas as pd
from pymongo import MongoClient

# Connect to local MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["tickets_db"]

# Brand collections map
brand_collections = [()]

results = []

# Deterministic signatures for bot vs agent classification
bot_signatures = [
    "heybob.in", "tracking", "http://", "https://", 
    "automated", "select an option", "menu", "welcome", "faq"
]

agent_signatures = [
    "agent", "human", "support team", "transfer", "connected to", 
    "checking with", "will update", "sorry for the inconvenience"
]

print("🔍 Running comprehensive non-llm brand audit across raw collections...\n")

for brand_name, coll_name in brand_collections:
    collection = db[coll_name]
    docs = list(collection.find({}, {"messages": 1}))
    
    total_tickets = len(docs)
    total_messages = 0
    bot_replies = 0
    agent_replies = 0
    customer_messages = 0
    other_elements = 0
    
    autonomous_success_count = 0
    
    for doc in docs:
        messages = doc.get("messages", [])
        total_messages += len(messages)
        
        has_agent_handoff = False
        
        for m in messages:
            body_raw = m.get("body", "")
            m_type = m.get("type", "")
            
            # Parse JSON body string if stored as serialized JSON
            text_content = body_raw
            try:
                parsed = json.loads(body_raw)
                if isinstance(parsed, dict):
                    text_content = parsed.get("text", parsed.get("body", body_raw))
            except:
                pass
                
            text_lower = str(text_content).lower()
            
            # Heuristic Classification Rule Set:
            if m_type in ["button", "interactive", "image", "media"]:
                other_elements += 1
                bot_replies += 1  # Interactive buttons/media are part of bot flows
            elif any(sig in text_lower for sig in bot_signatures):
                bot_replies += 1
            elif any(sig in text_lower for sig in agent_signatures):
                agent_replies += 1
                has_agent_handoff = True
            else:
                # Remaining conversational turns treated as customer inputs
                customer_messages += 1
                
        # Deterministic Accuracy/Resolution Heuristic:
        # If a ticket contains zero agent handoff keywords, it was successfully handled by the automated workflow.
        if not has_agent_handoff:
            autonomous_success_count += 1

    accuracy_rate = (autonomous_success_count / total_tickets * 100) if total_tickets > 0 else 0

    results.append({
        "brand": brand_name,
        "total_tickets": total_tickets,
        "total_messages": total_messages,
        "bot_replies": bot_replies,
        "agent_replies": agent_replies,
        "customer_inputs": customer_messages,
        "media_elements": other_elements,
        "autonomous_resolution_%": round(accuracy_rate, 2)
    })

df = pd.DataFrame(results)

print("==================================================================")
print("📊 COMPREHENSIVE BRAND SUPPORT AUDIT (DETERMINISTIC)")
print("==================================================================\n")
print(df.to_string(index=False))