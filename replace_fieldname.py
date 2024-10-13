import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from aiomultiprocess import Pool

# MongoDB connection URI
MONGO_URI = 'mongodb://localhost:27017/'  # Replace with your connection URI
CHUNK_SIZE = 1000  # Process 1000 documents at a time

# Function to recursively remove " ' ` from field names
def remove_quotes_from_keys(doc):
    if isinstance(doc, dict):
        new_doc = {}
        for key, value in doc.items():
            new_key = key.replace('"', '').replace("'", '').replace('`', '')  # Remove " ' `
            new_doc[new_key] = remove_quotes_from_keys(value)  # Recursively apply to nested documents
        return new_doc
    elif isinstance(doc, list):
        return [remove_quotes_from_keys(item) for item in doc]  # Apply to list elements
    else:
        return doc  # Return the value as is for non-dict types

# Check if a field contains " ' ` characters
def field_contains_special_chars(doc):
    if isinstance(doc, dict):
        for key in doc.keys():
            if '"' in key or "'" in key or '`' in key:
                return True
    return False

# Asynchronous function to update documents in chunks
async def update_collection(db_name, collection_name):
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[db_name]
    collection = db[collection_name]
    
    # Check if any document contains fields with " ' ` characters
    async for document in collection.find({}, limit=1):  # Only check the first document
        if not field_contains_special_chars(document):
            print(f"Skipping collection '{collection_name}' in database '{db_name}' - no special characters in field names.")
            return  # Skip the collection if no field contains the special characters
    
    print(f"Processing collection '{collection_name}' in database '{db_name}'...")

    cursor = collection.find({})
    async for doc in cursor:
        updated_doc = remove_quotes_from_keys(doc)
        
        # Keep the original _id intact
        updated_doc['_id'] = doc['_id']
        
        # Update document
        await collection.replace_one({'_id': doc['_id']}, updated_doc)

# Asynchronous function to process each database and its collections
async def process_database(db_name):
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[db_name]
    collections = await db.list_collection_names()

    tasks = [update_collection(db_name, collection_name) for collection_name in collections]
    await asyncio.gather(*tasks)

# Main function to exclude certain databases and run the update
async def main():
    client = AsyncIOMotorClient(MONGO_URI)
    
    # Get all databases except 'admin', 'local', and 'config'
    databases = await client.list_database_names()
    databases = [db for db in databases if db not in ('admin', 'local', 'config')]

    async with Pool() as pool:
        await pool.map(run_process_database, databases)

# Helper function to ensure pickling of arguments
async def run_process_database(db_name):
    await process_database(db_name)

# Run the script
if __name__ == "__main__":
    asyncio.run(main())
