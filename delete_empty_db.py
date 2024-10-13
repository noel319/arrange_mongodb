import motor.motor_asyncio
import asyncio
from aiomultiprocess import Pool

# Create a global MongoDB client
client = motor.motor_asyncio.AsyncIOMotorClient('')

async def remove_null_column_keys(db_name, batch_size=1000):
    db = client[db_name]  # Get the database by name
    print(f"Working on database: {db_name}")

    # Get all collections in the current database
    collections = await db.list_collection_names()

    for collection_name in collections:
        collection = db[collection_name]
        print(f"Working on collection: {collection_name}")

        # Process documents in batches
        cursor = collection.find().batch_size(batch_size)

        async for doc in cursor:
            keys_to_remove = {key: "" for key in doc if 'null_column' in key}  # Dictionary comprehension

            # If we found any keys to remove, perform the $unset operation
            if keys_to_remove:
                result = await collection.update_one(
                    {"_id": doc["_id"]},  # Use _id to target the document
                    {"$unset": keys_to_remove}
                )
                if result.modified_count > 0:
                    print(f"Updated document with _id: {doc['_id']} in collection: {collection_name}")

async def process_database(db_name):
    await remove_null_column_keys(db_name)

async def del_nullcol():
    # Get the list of all databases
    databases = await client.list_database_names()
    databases = [db for db in databases if db not in ['admin', 'local', 'config']]
    print(f"Total databases: {len(databases)}")

    async with Pool() as pool:  # Limit the number of processes
        await pool.map(process_database, databases)  # Use map to apply to all databases

async def main():
    await del_nullcol()
    client.close()  # Close the MongoDB connection after all operations are complete

# Run the async function
if __name__ == "__main__":
    asyncio.run(main())