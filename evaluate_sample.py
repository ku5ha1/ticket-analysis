import asyncio
import json
import urllib.request
from motor.motor_asyncio import AsyncIOMotorClient

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "tickets_db"
COLLECTION_NAME = "tickets_tbof"  # Testing with TBOF brand

async def run_evaluation():
    # Connect to MongoDB
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    # Fetch 5 tickets for testing
    print(f"🔍 Fetching 5 sample tickets from '{COLLECTION_NAME}'...")
    cursor = collection.find().limit(5)
    tickets = await cursor.to_list(length=5)
    
    if not tickets:
        print("❌ No tickets found in collection!")
        client.close()
        return

    # Synchronous helper function to call local Ollama REST API
    def call_ollama(ticket):
        # Format the conversation transcript neatly
        messages_text = "\n".join([
            f"[{m.get('timestamp', 'N/A')}] {m.get('type', 'msg').upper()}: {m.get('body', '')}" 
            for m in ticket.get('messages', [])
        ])
        
        prompt = f"""You are an expert AI support analyst. Analyze the following customer service chat transcript and evaluate the performance of the AI bot.
        
Transcript:
{messages_text}

Provide your response strictly in the following JSON format:
{{
  "intent": "Short description of customer's core query or issue (e.g., order tracking, refund, product inquiry)",
  "resolved_by_bot": true or false,
  "requires_human": true or false,
  "reasoning": "Brief explanation of why"
}}
"""
        payload = json.dumps({
            "model": "llama3.2:3b",
            "prompt": prompt,
            "format": "json",
            "stream": False
        }).encode('utf-8')
        
        req = urllib.request.Request(
            "http://localhost:11434/api/generate",
            data=payload,
            headers={'Content-Type': 'application/json'},
            method='POST'
        )
        
        try:
            with urllib.request.urlopen(req, timeout=90) as response:
                res_body = json.loads(response.read().decode('utf-8'))
                eval_data = json.loads(res_body.get("response", "{}"))
                return ticket["_id"], eval_data, None
        except Exception as e:
            return ticket["_id"], None, str(e)

    print("🚀 Processing 5 tickets sequentially using local Ollama (llama3.2:3b)...")
    
    print("\n📊 --- OLLAMA EVALUATION RESULTS (Sample of 5) ---")
    
    # Strict sequential loop (one by one)
    for ticket in tickets:
        tid = ticket["_id"]
        print(f"\nProcessing Ticket ID: {tid}...")
        
        # Run the synchronous API call in an executor thread to keep async event loop unblocked
        loop = asyncio.get_running_loop()
        _, eval_res, error = await loop.run_in_executor(None, call_ollama, ticket)
        
        if error:
            print(f"❌ Error on {tid}: {error}")
        else:
            print(json.dumps(eval_res, indent=2))
            # Save evaluation back to MongoDB document immediately
            await collection.update_one(
                {"_id": tid},
                {"$set": {"evaluation": eval_res}}
            )
            print(f"💾 Saved evaluation for {tid} to MongoDB.")

    client.close()
    print("\n🎉 Sequential sample test complete and evaluations stored in MongoDB!")

if __name__ == "__main__":
    asyncio.run(run_evaluation())

# import os
# from dotenv import load_dotenv
# from google import genai

# # Load environment variables from .env
# load_dotenv()

# KEYS = {
#     "GEMINI_API_KEY_1": os.getenv("GEMINI_API_KEY_1"),
#     "GEMINI_API_KEY_2": os.getenv("GEMINI_API_KEY_2"),
#     "GEMINI_API_KEY_3": os.getenv("GEMINI_API_KEY_3"),
#     "GEMINI_API_KEY_4": os.getenv("GEMINI_API_KEY_4"),
# }

# print("🔍 Testing Gemini API Keys...\n")

# for key_name, key_value in KEYS.items():
#     if not key_value:
#         print(f"❌ {key_name}: Missing from .env file!")
#         continue

#     try:
#         # Initialize client with explicit key
#         client = genai.Client(api_key=key_value)
        
#         # Test request
#         response = client.models.generate_content(
#             model='gemini-3.5-flash-lite',
#             contents='Respond with the single word: READY'
#         )
        
#         output = response.text.strip() if response.text else "No response"
#         print(f"✅ {key_name}: SUCCESS (Response: '{output}')")
        
#     except Exception as e:
#         print(f"❌ {key_name}: FAILED | Error: {e}")

# print("\n🎉 Key check complete!")