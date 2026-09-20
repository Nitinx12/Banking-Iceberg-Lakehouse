"""scripts/seed_mongo.py — load sample dataset into local Mongo (PROJECT_PLAN Phase 0 task).

Usage: uv run python scripts/seed_mongo.py [--data-dir data] [--uri mongodb://admin:pass@localhost:27017/banking?replicaSet=rs0&authSource=admin]
If data/ is empty, inserts minimal synthetic docs for smoke testing.
"""

import argparse
import json
import os
import pathlib

from dotenv import load_dotenv

load_dotenv()


def get_uri(args_uri):
    return args_uri or os.getenv(
        "MONGO_URI",
        "mongodb://admin:password@localhost:27017/banking?replicaSet=rs0&authSource=admin",
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="tests/data")
    ap.add_argument("--uri", default=None)
    args = ap.parse_args()
    uri = get_uri(args.uri)
    print(f"Connecting to {uri.split('@')[-1]} ...")
    try:
        from pymongo import MongoClient
    except ImportError:
        print("pymongo not installed — run: uv sync --group ingestion")
        return
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
    except Exception as e:
        print(f"✖ mongo ping failed: {e}")
        print("  ensure: docker compose --profile core up -d && docker compose ps")
        return
    db = client.get_database()
    print(f"✓ connected to db={db.name}")
    # list collections
    cols = db.list_collection_names()
    print(f"existing collections: {cols or '(none)'}")
    data_dir = pathlib.Path(args.data_dir)
    if data_dir.exists():
        json_files = list(data_dir.glob("*.json"))
        if json_files:
            for jf in json_files:
                coll = jf.stem
                docs = json.loads(jf.read_text())
                if isinstance(docs, dict):
                    docs = [docs]
                if docs:
                    db[coll].delete_many({})
                    db[coll].insert_many(docs)
                    print(f"  loaded {len(docs)} docs into {coll} from {jf}")
            return
    # fallback synthetic
    print("no data files in tests/data — inserting synthetic smoke docs")
    db.customers.delete_many({})
    db.customers.insert_one(
        {"_id": "cust_001", "name": "Test User", "updated_at": "2026-09-20T00:00:00Z"}
    )
    print("  inserted 1 synthetic customer")
    print("✓ seed_mongo done")


if __name__ == "__main__":
    main()
