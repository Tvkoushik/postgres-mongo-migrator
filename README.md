
# Data Migration from PostgreSQL to MongoDB

This repository contains a Python script for migrating data from a PostgreSQL database to a MongoDB database. The script is designed to be robust and fault-tolerant, capable of handling large datasets.

## Features

- Batch processing for efficient data transfer
- Multi-threading for concurrent data migration
- Checkpointing for fault tolerance
- Configuration-driven for flexibility
- Logging for debugging and tracking
- Handles various data types like `Decimal` and `datetime.date`

## Prerequisites

- Python 3.x
- `psycopg2` for PostgreSQL connectivity
- `pymongo` for MongoDB connectivity

You can install the required packages using the following command:

```bash
pip install psycopg2 pymongo
```

## Configuration

Before running the script, you need to set up the `config.json` file. Here's a sample configuration:

```json
{
  "postgresql": {
    "host": "localhost",
    "database": "your_database",
    "user": "your_user",
    "password": "your_password",
    "query": "SELECT * FROM your_table_or_view"
  },
  "mongodb": {
    "uri": "mongodb://localhost:27017/",
    "database": "your_mongo_database",
    "collection": "your_mongo_collection"
  },
  "batch_size": 1000,
  "num_threads": 4
}
```

## How to Run

1. Set up the `config.json` file as described above.
2. Run the migration script:

```bash
python migration.py
```

## Checkpointing

The script uses a checkpoint file named `checkpoint.txt` to keep track of the last successfully migrated batch. If the migration process is interrupted, you can resume from the last checkpoint. The checkpoint file will be automatically deleted once all data is migrated.

## Logging

The script uses Python's built-in logging library to log events and issues. You can check the logs for debugging and tracking the migration process.

## License

MIT
