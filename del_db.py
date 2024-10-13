import motor.motor_asyncio
import os
import asyncio
from aiomultiprocess import Pool

async def delete_databases_except_pattern():
    # Step 1: Connect to MongoDB using Motor
    client = motor.motor_asyncio.AsyncIOMotorClient("")

    # Step 2: Get a list of all databases
    db_list = await client.list_database_names()
    db_list = [db for db in db_list if db not in ['admin', 'local', 'config']]
    # Step 3: Filter out databases containing the word 'database'
    databases_to_delete = [db for db in db_list if 'database' not in db]

    # Step 4: Loop over the filtered databases and delete them
    for db_name in databases_to_delete:
        print(f"Deleting database: {db_name}")
        await client.drop_database(db_name)
        print(f"Database '{db_name}' has been deleted.")

    print("Finished deleting databases.")

async def check_and_delete_database(db_name):
    # Step 2: Connect to MongoDB using Motor
    client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://twuser:moniThmaRtio@192.168.20.75:27017/admin")

    # Step 3: Check if the database exists
    db_list = await client.list_database_names()
    
    if db_name in db_list:
        print(f"Database '{db_name}' exists. Deleting it...")
        # Step 4: Drop the database if it exists
        await client.drop_database(db_name)
        print(f"Database '{db_name}' has been deleted.")
    else:
        print(f"Database '{db_name}' does not exist.")

async def main():
    # Step 1: Read the database names from db.txt (support for multiple DBs)
    if not os.path.exists("db.txt"):
        print("db.txt file not found.")
        return
    
    with open("db.txt", "r") as file:
        db_names = [line.strip() for line in file if line.strip()]
    
    # Step 5: Use aiomultiprocess Pool to process databases in parallel
    async with Pool() as pool:
        await pool.map(check_and_delete_database, db_names)

# Run the async main function
if __name__ == "__main__":
    # asyncio.run(main())
    asyncio.run(delete_databases_except_pattern())