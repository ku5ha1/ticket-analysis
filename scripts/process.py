import asyncio
import os
import pandas as pd
from motor.motor_asyncio import AsyncIOMotorClient

# MongoDB Connection URI (pointing to your local docker container)
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "tickets_db"

async def process_brand_csv(file_path, db):
    brand_name = os.path.basename(file_path).replace("TicketConversationData_", "").replace(".csv", "")
    collection_name = f"tickets_{brand_name}"
    collection = db[collection_name]
    
    print(f"🚀 Starting processing for brand: {brand_name.upper()} from {file_path}")
    
    # Read CSV using pandas (in a real-world multi-gigabyte file scenario, chunking can also be used)
    # Using run_in_executor to avoid blocking the event loop with synchronous pandas operations
    loop = asyncio.get_running_loop()
    df = await loop.run_in_executor(None, pd.read_csv, file_path)
    
    ticket_col = 'pc.meta->>"$.ticketId"'
    
    if ticket_col not in df.columns:
        print(f"❌ Error: Ticket column not found in {file_path}")
        return

    print(f"📦 Grouping {len(df):,} rows for {brand_name} by ticket ID...")
    
    # Group by ticket ID
    grouped = df.groupby(ticket_col)
    
    batch = []
    batch_size = 500
    total_inserted = 0

    for ticket_id, group in grouped:
        # Sort messages chronologically
        group = group.sort_values(by='timestamp')
        
        messages = []
        for _, row in group.iterrows():
            messages.append({
                "timestamp": str(row['timestamp']),
                "type": str(row['type']),
                "body": str(row['body'])
            })
            
        doc = {
            "_id": str(ticket_id), # Using string to handle any large integer or string ID formats safely
            "brand": brand_name,
            "recipient_id": str(group['recipient_id'].iloc[0]),
            "total_messages": len(messages),
            "first_message_time": messages[0]["timestamp"],
            "last_message_time": messages[-1]["timestamp"],
            "messages": messages
        }
        
        # Use replace_one with upsert=true to make the script idempotent (safe to re-run)
        batch.append(
            collection.replace_one(
                {"_id": doc["_id"]}, 
                doc, 
                upsert=True
            )
        )
        
        # Flush batch when it reaches batch_size
        if len(batch) >= batch_size:
            await asyncio.gather(*batch)
            total_inserted += len(batch)
            batch = []
            print(f"[{brand_name.upper()}] Upserted batch... Total processed so far: {total_inserted:,}")

    # Flush remaining items in batch
    if batch:
        await asyncio.gather(*batch)
        total_inserted += len(batch)

    print(f"✅ Successfully finished {brand_name.upper()}: {total_inserted:,} tickets saved to collection '{collection_name}'.")

async def main():
    # Initialize Async Motor Client
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    
    # Ensure connection works
    try:
        await client.admin.command('ping')
        print(" Connected successfully to MongoDB!")
    except Exception as e:
        print(f"❌ Failed to connect to MongoDB: {e}")
        return

    # Look for all CSV files in current or data directory
    data_dir = "./data" if os.path.exists("./data") else "."
    csv_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.startswith("TicketConversationData_") and f.endswith(".csv")]
    
    if not csv_files:
        print("❌ No matching ticket CSV files found!")
        return

    print(f"📂 Found {len(csv_files)} brand dataset(s) to process concurrently.")

    # Process all brands concurrently using asyncio.gather
    tasks = [process_brand_csv(file_path, db) for file_path in csv_files]
    await asyncio.gather(*tasks)

    print("\n🎉 All brand datasets successfully preprocessed and loaded into MongoDB!")
    client.close()

if __name__ == "__main__":
    asyncio.run(main())