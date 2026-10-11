import os
from langsmith import Client
from dotenv import load_dotenv

load_dotenv()

# Verify environment variable is visible to Python
api_key = os.environ.get("LANGSMITH_API_KEY")
print(f"🔑 LANGSMITH_API_KEY detected in environment: {bool(api_key)}")

client = Client()

project_name = os.getenv("PROJECT_NAME")
project_id = os.getenv("PROJECT_ID")

print("\n🔌 Step 1: Listing accessible projects in your Langsmith workspace...")
try:
    projects = list(client.list_projects(limit=10))
    print(f"✅ Successfully connected! Found {len(projects)} accessible project(s):")
    for p in projects:
        print(f"   - Name: '{p.name}' | ID: {p.id}")
except Exception as e:
    print(f"❌ Project listing failed: {e}")

print(f"\n🔍 Step 2: Querying runs for project '{project_name}' using modern SDK...")
try:
    # Using modern client.runs.query() instead of deprecated list_runs()
    runs = list(client.runs.query(
        project_name=project_name,
        is_root=True,
        limit=5
    ))
    print(f"✅ Success! Retrieved {len(runs)} root run(s) from '{project_name}'.")
    for r in runs:
        print(f"   - Run ID: {r.id} | Name: {r.name} | Status: {r.status} | Latency: {r.end_time - r.start_time if r.end_time and r.start_time else 'N/A'}")
except Exception as e:
    print(f"❌ Run query failed: {e}")