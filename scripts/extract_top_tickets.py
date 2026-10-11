import asyncio
import json
import urllib.request
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "tickets_db"
SOURCE_COLLECTION = "source_data"
LOG_FILE = "failed_tickets.txt"

async def log_failure(brand, ticket_id, error_msg):
    """Appends failed evaluation metadata to a dead-letter log file."""
    timestamp = datetime.now().isoformat()
    log_entry = f"[{timestamp}] Brand: {brand} | Ticket ID: {ticket_id} | Error: {error_msg}\n"
    await asyncio.get_running_loop().run_in_executor(
        None, lambda: open(LOG_FILE, "a", encoding="utf-8").write(log_entry)
    )

async def evaluate_single_ticket(collection, ticket, semaphore):
    async with semaphore:
        doc_id = ticket["_id"]
        ticket_id = ticket.get("ticket_id", str(doc_id))
        brand = ticket.get("brand", "unknown")
        
        # Build clean chronological transcript text
        messages_text = "\n".join([
            f"[{m.get('timestamp', 'N/A')}] {m.get('type', 'msg').upper()}: {m.get('body', '')}" 
            for m in ticket.get("messages", [])
        ])
        
        prompt = f"""You are a principal customer support quality analyst evaluating automated AI customer service interactions. Analyze the transcript and extract structured evaluation metrics.

### FEW-SHOT EXAMPLES FOR GUIDANCE:

--- Example 1 ---
Transcript:
[2026-09-01] USER: Where is my order #1002?
[2026-09-01] BOT: Your order #1002 was delivered yesterday via FedEx. Tracking: link.
[2026-09-01] USER: Thanks, got it!

Response JSON:
{{
  "intent": "Order Tracking",
  "sub_intent": "Delivery Status Inquiry",
  "resolved_by_bot": true,
  "bot_failure_reason": "N/A",
  "requires_human": false,
  "customer_sentiment": "Positive",
  "urgency": "Low",
  "confidence_score": 0.98,
  "reasoning": "Bot supplied immediate tracking details and user explicitly confirmed resolution."
}}

--- Example 2 ---
Transcript:
[2026-09-02] USER: My package arrived damaged and open!
[2026-09-02] BOT: Hello! For shipping questions, please check our FAQ page.
[2026-09-02] USER: Human agent now!! This is unacceptable!

Response JSON:
{{
  "intent": "Damaged Product",
  "sub_intent": "Replacement / Refund Request",
  "resolved_by_bot": false,
  "bot_failure_reason": "Irrelevant Bot Response / Misunderstood Intent",
  "requires_human": true,
  "customer_sentiment": "Frustrated",
  "urgency": "High",
  "confidence_score": 0.95,
  "reasoning": "Bot provided generic FAQ link for damaged goods and triggered customer anger, requiring human intervention."
}}

---

### TRANSCRIPT TO EVALUATE:
Brand: {brand}
Transcript:
{messages_text}

Provide your response strictly in valid JSON using this format:
{{
  "intent": "Core customer issue (e.g., Order Tracking, Quality Defect, Refund Request, Product Inquiry)",
  "sub_intent": "Specific granular sub-category",
  "resolved_by_bot": true or false,
  "bot_failure_reason": "N/A if resolved, otherwise specific failure mode (e.g., Infinite Loop, Premature Escalation, Generic Template Loop, Hallucination)",
  "requires_human": true or false,
  "customer_sentiment": "Positive" | "Neutral" | "Frustrated" | "Angry",
  "urgency": "Low" | "Medium" | "High" | "Critical",
  "confidence_score": float between 0.00 and 1.00,
  "reasoning": "Concise summary of support flow, bot performance, and why escalation was or wasn't needed"
}}
"""
        payload = json.dumps({
            "model": "llama3.2:3b",
            "prompt": prompt,
            "format": "json",
            "stream": False
        }).encode("utf-8")
        
        req = urllib.request.Request(
            "http://localhost:11434/api/generate",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        
        loop = asyncio.get_running_loop()
        max_retries = 3
        
        for attempt in range(1, max_retries + 1):
            try:
                def call_api():
                    with urllib.request.urlopen(req, timeout=90) as response:
                        res_body = json.loads(response.read().decode("utf-8"))
                        return json.loads(res_body.get("response", "{}"))
                
                eval_data = await loop.run_in_executor(None, call_api)
                
                # Persist enriched evaluation document back into source_data
                await collection.update_one(
                    {"_id": doc_id},
                    {"$set": {
                        "evaluation": eval_data,
                        "evaluated_at": datetime.now().isoformat()
                    }}
                )
                return True, ticket_id

            except Exception as e:
                error_str = str(e)
                if attempt < max_retries:
                    await asyncio.sleep(attempt * 2)  # Exponential backoff (2s, 4s...)
                else:
                    print(f"❌ Permanent failure on Ticket {ticket_id}: {error_str}")
                    await log_failure(brand, ticket_id, error_str)
                    await collection.update_one(
                        {"_id": doc_id},
                        {"$set": {"evaluation": {"error": error_str}}}
                    )
                    return False, ticket_id

async def run_full_pipeline():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[SOURCE_COLLECTION]
    
    semaphore = asyncio.Semaphore(2)  # Bounded concurrency window
    
    print("==================================================")
    print(f"🚀 Starting Enriched Evaluation on '{SOURCE_COLLECTION}'")
    print("==================================================")
    
    start_time = datetime.now()
    total_processed = 0
    
    while True:
        # Fetch unanalyzed records in batches of 25
        cursor = collection.find({"evaluation": {"$exists": False}}).limit(25)
        batch = await cursor.to_list(length=25)
        
        if not batch:
            print(f"\n✅ All tickets in '{SOURCE_COLLECTION}' are fully evaluated!")
            break
            
        print(f"📦 Processing batch of {len(batch)} tickets...")
        
        tasks = [evaluate_single_ticket(collection, ticket, semaphore) for ticket in batch]
        results = await asyncio.gather(*tasks)
        
        success_count = sum(1 for success, _ in results if success)
        total_processed += success_count
        
        elapsed_min = (datetime.now() - start_time).total_seconds() / 60
        rate = total_processed / elapsed_min if elapsed_min > 0 else 0
        print(f"✨ Batch done ({success_count}/{len(batch)} ok). Total: {total_processed} | Speed: ~{rate:.1f} tickets/min")
        
        await asyncio.sleep(0.5)

    client.close()
    duration = (datetime.now() - start_time).total_seconds() / 60
    print(f"\n🎉 Pipeline Finished in {duration:.2f} minutes! Total evaluated: {total_processed}")

if __name__ == "__main__":
    asyncio.run(run_full_pipeline())