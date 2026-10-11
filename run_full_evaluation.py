import asyncio
import itertools
import json
import os
from datetime import datetime
from dotenv import load_dotenv
from google import genai
from google.genai import types
from motor.motor_asyncio import AsyncIOMotorClient

# Load environment variables from .env file
load_dotenv()

# Fetch the 4 keys from environment variables
API_KEYS = [
    os.getenv("GEMINI_API_KEY_1"),
    os.getenv("GEMINI_API_KEY_2"),
    os.getenv("GEMINI_API_KEY_3"),
    os.getenv("GEMINI_API_KEY_4")
]

# Validate that all 4 keys are present
if not all(API_KEYS):
    raise ValueError("❌ Error: One or more GEMINI_API_KEY variables are missing from your .env file!")

# Initialize client instances for each key and set up round-robin cycling
CLIENT_POOL = [genai.Client(api_key=key) for key in API_KEYS]
client_cycle = itertools.cycle(CLIENT_POOL)

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "tickets_db"
SOURCE_COLLECTION = "source_data"
LOG_FILE = "failed_tickets.txt"

async def log_failure(brand, ticket_id, error_msg):
    """Appends failed evaluation metadata to a local dead-letter file log."""
    timestamp = datetime.now().isoformat()
    log_entry = f"[{timestamp}] Brand: {brand} | Ticket ID: {ticket_id} | Error: {error_msg}\n"
    await asyncio.get_running_loop().run_in_executor(
        None, lambda: open(LOG_FILE, "a", encoding="utf-8").write(log_entry)
    )

async def evaluate_ticket_with_rotation(collection, ticket, semaphore):
    async with semaphore:
        doc_id = ticket["_id"]
        ticket_id = ticket.get("ticket_id", str(doc_id))
        brand = ticket.get("brand", "unknown")
        
        # Build clean chronological transcript text
        messages_text = "\n".join([
            f"[{m.get('timestamp', 'N/A')}] {m.get('type', 'msg').upper()}: {m.get('body', '')}" 
            for m in ticket.get("messages", [])
        ])
        
        # Optimized prompt with few-shot examples for robust extraction
        prompt = f"""You are a principal customer support quality analyst evaluating automated AI customer service interactions. Analyze the transcript and extract structured evaluation metrics.

### FEW-SHOT EXAMPLES FOR GUIDANCE:

--- Example 1 ---
Transcript:
[2026-09-01] USER: Where is my order #1002?
[2026-09-01] BOT: Your order #1002 was delivered yesterday via FedEx. Tracking: link.
[2026-09-01] USER: Thanks, got it!

Expected Analysis:
- intent: Order Tracking
- sub_intent: Delivery Status Inquiry
- resolved_by_bot: true
- bot_failure_reason: N/A
- requires_human: false
- customer_sentiment: Positive
- urgency: Low
- confidence_score: 0.98
- reasoning: Bot supplied immediate tracking details and user explicitly confirmed resolution.

--- Example 2 ---
Transcript:
[2026-09-02] USER: My package arrived damaged and open!
[2026-09-02] BOT: Hello! For shipping questions, please check our FAQ page.
[2026-09-02] USER: Human agent now!! This is unacceptable!

Expected Analysis:
- intent: Damaged Product
- sub_intent: Replacement / Refund Request
- resolved_by_bot: false
- bot_failure_reason: Irrelevant Bot Response / Misunderstood Intent
- requires_human: true
- customer_sentiment: Frustrated
- urgency: High
- confidence_score: 0.95
- reasoning: Bot provided generic FAQ link for damaged goods and triggered customer anger, requiring human intervention.

---

### TRANSCRIPT TO EVALUATE:
Brand: {brand}
Transcript:
{messages_text}
"""

        # Strict response schema enforcement for Gemini
        config = types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema={
                "type": "OBJECT",
                "properties": {
                    "intent": {"type": "STRING", "description": "Core customer issue (e.g., Order Tracking, Quality Defect, Refund Request)"},
                    "sub_intent": {"type": "STRING", "description": "Specific granular sub-category"},
                    "resolved_by_bot": {"type": "BOOLEAN"},
                    "bot_failure_reason": {"type": "STRING", "description": "N/A if resolved, otherwise specific failure mode"},
                    "requires_human": {"type": "BOOLEAN"},
                    "customer_sentiment": {"type": "STRING", "enum": ["Positive", "Neutral", "Frustrated", "Angry"]},
                    "urgency": {"type": "STRING", "enum": ["Low", "Medium", "High", "Critical"]},
                    "confidence_score": {"type": "NUMBER", "description": "Float between 0.00 and 1.00 representing confidence"},
                    "reasoning": {"type": "STRING", "description": "Concise summary of support flow, bot performance, and escalation triggers"}
                },
                "required": [
                    "intent", "sub_intent", "resolved_by_bot", "bot_failure_reason", 
                    "requires_human", "customer_sentiment", "urgency", "confidence_score", "reasoning"
                ]
            },
            system_instruction="You are an expert customer support auditor specializing in evaluating chatbot efficacy, user sentiment, and escalation triggers."
        )

        loop = asyncio.get_running_loop()
        max_retries = 3
        
        for attempt in range(1, max_retries + 1):
            try:
                # Rotate to the next client in the pool
                current_client = next(client_cycle)

                def call_gemini():
                    response = current_client.models.generate_content(
                        model='gemini-3.5-flash-lite',
                        contents=prompt,
                        config=config
                    )
                    return response.text

                # Execute API call in background thread executor
                result_json_str = await loop.run_in_executor(None, call_gemini)
                eval_data = json.loads(result_json_str)

                # Persist evaluation back to source_data collection in MongoDB
                await collection.update_one(
                    {"_id": doc_id},
                    {"$set": {
                        "evaluation": eval_data,
                        "evaluated_at": datetime.now().isoformat()
                    }}
                )
                
                # Inter-request safety pacing delay to ensure no key hits rate limits
                await asyncio.sleep(0.4)
                return True, ticket_id

            except Exception as e:
                error_str = str(e)
                if attempt < max_retries:
                    # Exponential backoff delay before switching to the next rotated key
                    await asyncio.sleep(attempt * 2)
                else:
                    print(f"❌ Permanent failure on Ticket {ticket_id}: {error_str}")
                    await log_failure(brand, ticket_id, error_str)
                    await collection.update_one(
                        {"_id": doc_id},
                        {"$set": {"evaluation": {"error": error_str}}}
                    )
                    return False, ticket_id

async def run_cloud_pipeline():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[SOURCE_COLLECTION]
    
    # Concurrency set to 4 (matching your 4 keys) for optimal parallel throughput
    semaphore = asyncio.Semaphore(4)
    
    print("==================================================")
    print(f"🚀 Starting Gemini 3.5 Flash-Lite Evaluation (4-Key Rotation)")
    print("==================================================")
    
    start_time = datetime.now()
    total_processed = 0
    
    while True:
        # Fetch unanalyzed records idempotently from source_data
        cursor = collection.find({"evaluation": {"$exists": False}}).limit(40)
        batch = await cursor.to_list(length=40)
        
        if not batch:
            print(f"\n✅ All tickets in '{SOURCE_COLLECTION}' are fully evaluated!")
            break
            
        print(f"📦 Processing batch of {len(batch)} high-context tickets...")
        
        tasks = [evaluate_ticket_with_rotation(collection, ticket, semaphore) for ticket in batch]
        results = await asyncio.gather(*tasks)
        
        success_count = sum(1 for success, _ in results if success)
        total_processed += success_count
        
        elapsed_min = (datetime.now() - start_time).total_seconds() / 60
        rate = total_processed / elapsed_min if elapsed_min > 0 else 0
        print(f"✨ Batch done ({success_count}/{len(batch)} ok). Total: {total_processed} | Speed: ~{rate:.1f} tickets/min")

    client.close()
    duration = (datetime.now() - start_time).total_seconds() / 60
    print(f"\n🎉 Cloud Pipeline Finished in {duration:.2f} minutes! Total evaluated: {total_processed}")

if __name__ == "__main__":
    asyncio.run(run_cloud_pipeline())