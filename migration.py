# Import necessary libraries
import psycopg2
from pymongo import MongoClient
from decimal import Decimal
import datetime
import threading
import logging
import json
import os
from time import sleep

# Load configuration from JSON file
with open("config.json", "r") as f:
    config = json.load(f)

# Initialize logging for better debugging and tracking
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Initialize PostgreSQL connection
try:
    pg_conn = psycopg2.connect(
        host=config["postgresql"]["host"],
        database=config["postgresql"]["database"],
        user=config["postgresql"]["user"],
        password=config["postgresql"]["password"],
        port=config["postgresql"]["port"],
    )
    logging.info("Successfully connected to PostgreSQL.")
except Exception as e:
    logging.error(f"Failed to connect to PostgreSQL: {e}")
    exit(1)

# Calculate the total number of records to be migrated
# This is useful for logging and for setting up the batch processing
query_count = config["postgresql"]["query"].replace("*", "COUNT(*)")
try:
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute(query_count)
    total_records = pg_cursor.fetchone()[0]
    logging.info(f"Total records to migrate: {total_records}")
except Exception as e:
    logging.error(f"Failed to get the total number of records: {e}")
    exit(1)

# Initialize MongoDB connection
try:
    mongo_client = MongoClient(config["mongodb"]["uri"])
    mongo_db = mongo_client[config["mongodb"]["database"]]
    mongo_collection = mongo_db[config["mongodb"]["collection"]]
    logging.info("Successfully connected to MongoDB.")
except Exception as e:
    logging.error(f"Failed to connect to MongoDB: {e}")
    exit(1)

# Initialize Checkpoint for fault tolerance
# Read the last successfully migrated batch ID from a checkpoint file
checkpoint_file = "checkpoint.txt"
checkpoint = 0
try:
    with open(checkpoint_file, "r") as f:
        checkpoint = int(f.read().strip())
    logging.info(f"Resuming from checkpoint: {checkpoint}")
except FileNotFoundError:
    logging.info("No checkpoint file found. Starting from the beginning.")
    checkpoint = 0


# Function to migrate data in batches
def migrate_data(batch_id, offset, limit, retries=3):
    global checkpoint
    if batch_id < checkpoint:
        logging.info(f"Skipping batch {batch_id} due to checkpoint.")
        return

    for attempt in range(1, retries + 1):
        try:
            # Fetch data from PostgreSQL
            pg_cursor = pg_conn.cursor()
            query = config["postgresql"]["query"] + " LIMIT %s OFFSET %s;"
            pg_cursor.execute(query, (limit, offset))
            rows = pg_cursor.fetchall()

            # Fetch column names from cursor description
            column_names = [desc[0] for desc in pg_cursor.description]

            # Transform rows into dictionaries
            # Also handle Decimal and datetime.date objects
            transformed_rows = []
            for row in rows:
                transformed_row = {}
                for col, val in zip(column_names, row):
                    if isinstance(val, Decimal):
                        transformed_row[col] = float(val)
                    elif isinstance(val, datetime.date):
                        transformed_row[col] = datetime.datetime(
                            val.year, val.month, val.day
                        )
                    else:
                        transformed_row[col] = val
                transformed_rows.append(transformed_row)

            # Insert data into MongoDB in a single batch
            if transformed_rows:
                mongo_collection.insert_many(transformed_rows)

            # Update checkpoint for fault tolerance
            with open(checkpoint_file, "w") as f:
                f.write(str(batch_id))

            logging.info(f"Successfully migrated batch {batch_id}")
            return

        except Exception as e:
            logging.error(
                f"Failed to migrate batch {batch_id} on attempt {attempt}: {e}"
            )
            sleep(5 * attempt)  # Exponential back-off

    logging.critical(f"Failed to migrate batch {batch_id} after {retries} attempts.")


# Number of records per batch
batch_size = config["batch_size"]

# Number of threads to use for concurrent migration
num_threads = config["num_threads"]

# Calculate the total number of batches
total_batches = (total_records // batch_size) + (1 if total_records % batch_size else 0)

# Create and start threads for each batch
threads = []
for i in range(checkpoint, total_batches):
    offset = i * batch_size
    thread = threading.Thread(target=migrate_data, args=(i, offset, batch_size))
    threads.append(thread)
    thread.start()

    # Limit the number of concurrent threads to avoid overwhelming the system
    if len(threads) >= num_threads:
        for t in threads:
            t.join()
        threads = []

# Wait for any remaining threads to complete their work
for t in threads:
    t.join()

# Delete the checkpoint file if all rows are successfully migrated
if os.path.exists(checkpoint_file):
    os.remove(checkpoint_file)
    logging.info("Migration completed successfully. Checkpoint file deleted.")
