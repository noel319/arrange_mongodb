import motor.motor_asyncio
import asyncio
from aiomultiprocess import Pool

# MongoDB connection URI (change according to your MongoDB setup)
MONGO_URI = 'mongodb://'

# Create a motor async client
client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)


async def get_field_count(database_name):
    """Get the number of fields in a document in the 'main' collection."""
    try:
        db = client[database_name]
        collections = await db.list_collection_names()

        if 'main' not in collections:
            print(f"'main' collection does not exist in {database_name}")
            return 0

        document = await db['main'].find_one()
        if document is None:
            print(f"No documents found in the 'main' collection for {database_name}")
            return 0

        # Count the number of fields in the document
        return len(document.keys())
    except Exception as e:
        print(f"Error getting field count for {database_name}: {e}")
        return 0


async def copy_collection(database_name, target_db, target_collection):
    """Copy all documents from the source collection to the target collection."""
    try: 
        db = client[database_name]
        cursor = db['main'].find().batch_size(3000)
        documents = []

        async for doc in cursor:
        # Prepare document for insertion (excluding '_id' to avoid duplicates)
            doc.pop('_id', None)            
            documents.append(doc)
            if len(documents) >= 3000:
                await client[target_db][target_collection].insert_many(documents)
                documents.clear()

        if documents:
            await client[target_db][target_collection].insert_many(documents)
        with open('db.txt', 'a') as f:
            f.write(f"{database_name}\n")
        print(f"Copied {len(documents)} documents from 'main' collection to '{target_collection}' in {database_name}")
    except Exception as e:
        print(f"Error copying collection from {database_name} to {target_db}/{target_collection}: {e}")


async def migrate_database(database_name):
    """Migrate 'main' collection with the same document count in a database."""

    # Get document key count in the 'main' collection
    main_count = await get_field_count(database_name)

    if main_count != 0:
        # Target DB name is the database name suffixed with field count
        target_db_name = f"database_{main_count}"
        target_collection = database_name  # The collection name will remain the same

        # Copy the data from 'main' to target collection in target_db
        await copy_collection(database_name, target_db_name, target_collection)


async def run_migration(database_name):
    """Wrapper function to run the async migrate_database function."""
    await migrate_database(database_name)


async def migrate_all_databases():
    """Main function to migrate collections across all databases."""
    try:
        databases = await client.list_database_names()
        task = []
        # Filter out system databases (like admin, config, local)
        user_databases = [db for db in databases if db not in ('admin', 'config', 'local')]
        with open('db.txt', 'r') as file:
            db_names = file.read().splitlines()
            for db in user_databases:
                if db not in db_names:
                    print(f"Database '{db}' does not exists in db.txt file.")
                    task.append(db)
                else:
                    print(f"Database '{db}' exist in db.txt file.")
                    
        # Use aiomultiprocessing to parallelize the migration process across databases
        async with Pool() as pool:
            await pool.map(run_migration, task)
    except Exception as e:
        print(f"Error migrating databases:{e}")


# Run the migration
if __name__ == "__main__":
    asyncio.run(migrate_all_databases())
