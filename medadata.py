import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from aiomultiprocess import Pool
from bson import ObjectId

# MongoDB connection
client = AsyncIOMotorClient("mongodb://localhost:27017")
meta_db = client['meta']
personal_collection = meta_db['personal']

# Define the structure of the 'personal' collection (fields with example default values)
async def create_personal_collection():
    # Check if the collection already exists; if not, create it
    collections = await meta_db.list_collection_names()
    if 'personal' not in collections:
        await meta_db.create_collection('personal')

    # Optional: Create an index on the 'full_name', 'email', and 'phone_number' fields for faster search
    await personal_collection.create_index([('full_name', 'text'), ('email', 'text'), ('phone_number', 'text')])

# Fields to search
search_fields = ['full_name', 'email', 'first_name', 'middle_name', 'last_name', 'phone_number', 'birth_date']

# Function to check if any search field exists in a document
def contains_search_field(document):
    return any(field in document for field in search_fields)

# Function to process one collection
async def process_collection(db_name, collection_name, collection):
    async for document in collection.find({}):
        # Check if any of the search fields exist in the document
        if contains_search_field(document):
            # Build the query to search in 'personal' collection
            query = {
                "$or": [
                    {"full_name": document.get('full_name')},
                    {"email": document.get('email')},
                    {"phone_number": document.get('phone_number')},
                    {"birthday": document.get('birth_date')}
                ]
            }
            # Check if document exists in the personal collection
            existing = await personal_collection.find_one(query)
            
            # Prepare the source_data field
            source_data = {
                "id": document.get('_id'),
                "database": db_name,
                "collection": collection_name
            }
            
            if existing:
                # If the document already exists in the personal collection, update the source_data
                await personal_collection.update_one(
                    {"_id": existing['_id']},
                    {"$addToSet": {"source_data": source_data}}  # Add source_data only if not already present
                )
            else:
                # Add missing fields if they exist in the document
                if 'first_name' in document:
                    new_doc['full_name'] = document['first_name']
                if 'middle_name' in document:
                    new_doc['full_name'] += document['middle_name']
                if 'last_name' in document:
                    new_doc['full_name'] += document['last_name']
                # If the document doesn't exist, add it to the personal collection with all fields
                new_doc = {
                    "full_name": document.get('full_name'),
                    "email": document.get('email'),
                    "phone_number": document.get('phone_number'),
                    "birthday": document.get('birth_date'),
                    "source_data": [source_data]
                }
                await personal_collection.insert_one(new_doc)

# Function to process one database
async def process_database(db_name):
    db = client[db_name]
    collections = await db.list_collection_names()

    for collection_name in collections:
        collection = db[collection_name]
        await process_collection(db_name, collection_name, collection)

# Main function to process all databases
async def main():
    # Create 'meta' database and 'personal' collection with necessary fields
    await create_personal_collection()

    # Get list of databases except 'admin', 'config', 'local'
    databases = await client.list_database_names()
    databases = [db for db in databases if db not in ['admin', 'config', 'local']]

    # Use aiomultiprocess to process multiple databases in parallel
    async with Pool() as pool:
        await pool.map(process_database, databases)

# Run the main function
asyncio.run(main())
