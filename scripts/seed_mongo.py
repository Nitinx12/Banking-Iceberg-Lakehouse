"""scripts/seed_mongo.py — load sample dataset into local Mongo (PROJECT_PLAN Phase 0).

Usage: uv run python scripts/seed_mongo.py [--data-dir data] [--uri mongodb://admin:pass@localhost:27017/banking?replicaSet=rs0&authSource=admin]
If data/ is empty, inserts synthetic smoke docs for all collections.
"""

import argparse
import os
import uuid
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()


def get_uri(args_uri):
    return args_uri or os.getenv(
        "MONGO_URI",
        "mongodb://admin:password@localhost:27017/banking?replicaSet=rs0&authSource=admin",
    )


def _now():
    """Fixed seed timestamp as a real BSON DateTime (contracts type created_at as timestamp)."""
    return datetime(2026, 9, 15, 10, 0, 0)


def _random_id():
    return f"{uuid.uuid4().hex[:8]}"


def _customers():
    """Generate 5 synthetic customer docs."""
    docs = []
    names = ["Alice Johnson", "Bob Williams", "Carol Davis", "David Chen", "Eva Martinez"]
    cities = ["Pune", "Mumbai", "Delhi", "Bangalore", "Chennai"]
    states = ["MH", "MH", "DL", "KA", "TN"]
    occupations = ["Engineer", "Doctor", "Teacher", "Business", "Student"]

    for i in range(5):
        docs.append(
            {
                "_id": f"cust_{i + 1:03d}",
                "customer_id": i + 1,
                "name": names[i],
                "gender": "F" if i % 2 == 0 else "M",
                "date_of_birth": f"{1980 + i}-0{i + 1}-01",
                "city": cities[i],
                "state": states[i],
                "phone": 9876543210 + i,
                "email": f"customer{i}@example.com",
                "occupation": occupations[i],
                "annual_income": 500000 + (i * 200000),
                "join_date": f"2020-{i + 1:02d}-01",
                "credit_score": 650 + (i * 50),
                "created_at": _now(),
                "updated_at": _now(),
            }
        )
    return docs


def _accounts():
    """Generate 8 synthetic account docs."""
    docs = []
    account_types = ["Savings", "Current", "Fixed Deposit", "Recurring"]
    statuses = ["Active", "Active", "Active", "Dormant"]

    for i in range(8):
        docs.append(
            {
                "_id": f"acct_{i + 1:03d}",
                "account_id": i + 1,
                "customer_id": (i % 5) + 1,
                "branch_id": (i % 3) + 1,
                "account_type": account_types[i % 4],
                "open_date": f"2024-0{(i % 9) + 1}-15",
                "balance": round(10000 + (i * 50000) * 1.0, 2),
                "status": statuses[i % 4],
                "created_at": _now(),
                "updated_at": _now(),
            }
        )
    return docs


def _transactions():
    """Generate 20 synthetic transaction docs."""
    docs = []
    channels = ["ATM", "POS", "Online", "Branch", "NEFT"]
    txn_types = ["DEBIT", "CREDIT", "TRANSFER", "WITHDRAWAL"]

    for i in range(20):
        docs.append(
            {
                "_id": f"txn_{i + 1:04d}",
                "transaction_id": i + 1,
                "account_id": (i % 8) + 1,
                "txn_date": f"2026-09-{(i % 20) + 1:02d}",
                "txn_type": txn_types[i % 4],
                "amount": round(100 + (i * 500) * 1.0, 2),
                "channel": channels[i % 5],
                "created_at": _now(),
                "updated_at": _now(),
            }
        )
    return docs


def _branches():
    """Generate 5 synthetic branch docs."""
    docs = []
    branch_names = [
        "Pune Main",
        "Mumbai Central",
        "Delhi Junction",
        "Bangalore Tech",
        "Chennai Port",
    ]
    cities = ["Pune", "Mumbai", "Delhi", "Bangalore", "Chennai"]

    for i in range(5):
        docs.append(
            {
                "_id": f"brn_{i + 1:03d}",
                "branch_id": i + 1,
                "branch_name": branch_names[i],
                "city": cities[i],
                "state": ["MH", "MH", "DL", "KA", "TN"][i],
                "ifsc_code": f"SBIN{i + 1:04d}",
                "created_at": _now(),
                "updated_at": _now(),
            }
        )
    return docs


def _loans():
    """Generate 6 synthetic loan docs."""
    docs = []
    loan_types = ["Home", "Personal", "Vehicle", "Education", "Gold", "Business"]
    statuses = ["Active", "Active", "Closed", "Active", "Defaulted", "Active"]

    for i in range(6):
        docs.append(
            {
                "_id": f"ln_{i + 1:03d}",
                "loan_id": i + 1,
                "customer_id": (i % 5) + 1,
                "branch_id": (i % 3) + 1,
                "loan_type": loan_types[i % 6],
                "loan_amount": round(100000 + (i * 50000) * 1.0, 2),
                "interest_rate": round(8.5 + (i * 0.5), 2),
                "term_months": 120 + (i * 12),
                "start_date": f"2025-{(i % 12) + 1:02d}-01",
                "status": statuses[i % 6],
                "created_at": _now(),
                "updated_at": _now(),
            }
        )
    return docs


def _cards():
    """Generate 7 synthetic card docs."""
    docs = []
    card_types = ["Visa", "Mastercard", "RuPay", "Amex"]
    statuses = ["Active", "Active", "Blocked", "Active", "Expired", "Active", "Active"]

    for i in range(7):
        docs.append(
            {
                "_id": f"card_{i + 1:03d}",
                "card_id": i + 1,
                "customer_id": (i % 5) + 1,
                "account_id": (i % 8) + 1,
                "card_type": card_types[i % 4],
                "issue_date": f"2023-{(i % 12) + 1:02d}-10",
                "status": statuses[i % 7],
                "created_at": _now(),
                "updated_at": _now(),
            }
        )
    return docs


def _card_transactions():
    """Generate 15 synthetic card transaction docs."""
    docs = []

    for i in range(15):
        docs.append(
            {
                "_id": f"ctxn_{i + 1:04d}",
                "card_txn_id": i + 1,
                "card_id": (i % 7) + 1,
                "txn_date": f"2026-09-{(i % 15) + 1:02d}",
                "amount": round(50 + (i * 200) * 1.0, 2),
                "is_fraud": 0 if i < 13 else 1,
                "created_at": _now(),
            }
        )
    return docs


def _loan_payments():
    """Generate 10 synthetic loan payment docs."""
    docs = []

    for i in range(10):
        docs.append(
            {
                "_id": f"pay_{i + 1:04d}",
                "payment_id": i + 1,
                "loan_id": (i % 6) + 1,
                "payment_date": f"2026-08-{(i % 28) + 1:02d}",
                "amount_paid": round(5000 + (i * 1000) * 1.0, 2),
                "principal_component": round(4000 + (i * 800) * 1.0, 2),
                "interest_component": round(1000 + (i * 200) * 1.0, 2),
                "created_at": _now(),
            }
        )
    return docs


def _support_tickets():
    """Generate 5 synthetic support ticket docs."""
    docs = []
    statuses = ["Open", "Resolved", "In Progress", "Closed", "Escalated"]

    for i in range(5):
        docs.append(
            {
                "_id": f"tk_{i + 1:03d}",
                "ticket_id": i + 1,
                "customer_id": (i % 5) + 1,
                "issue_type": ["Card", "Account", "Loan", "App", "Other"][i % 5],
                "date_opened": f"2026-09-{(i % 15) + 1:02d}",
                "status": statuses[i % 5],
                "satisfaction_score": 1 + (i % 5),
                "created_at": _now(),
            }
        )
    return docs


def _employees():
    """Generate 4 synthetic employee docs."""
    docs = []
    roles = ["Manager", "Officer", "Clerk", "Assistant"]
    branch_ids = [1, 2, 3, 1]

    for i in range(4):
        docs.append(
            {
                "_id": f"emp_{i + 1:03d}",
                "employee_id": i + 1,
                "name": f"Employee {i + 1}",
                "branch_id": branch_ids[i],
                "role": roles[i],
                "hire_date": f"2021-0{(i % 9) + 1}-05",
                "salary": 500000 + (i * 150000),
                "created_at": _now(),
                "updated_at": _now(),
            }
        )
    return docs


def load_collection(db, name, generator):
    """Load a collection from the generator function."""
    docs = generator()
    db[name].delete_many({})
    if docs:
        db[name].insert_many(docs)
    print(f"  loaded {len(docs)} docs into {name}")


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
        print(f"[FAIL] mongo ping failed: {e}")
        print("  ensure: docker compose --profile core up -d && docker compose ps")
        return

    db = client.get_database()
    print(f"[ok] connected to db={db.name}")

    # Load synthetic data for all 10 collections
    load_collection(db, "customers", _customers)
    load_collection(db, "accounts", _accounts)
    load_collection(db, "transactions", _transactions)
    load_collection(db, "branches", _branches)
    load_collection(db, "loans", _loans)
    load_collection(db, "cards", _cards)
    load_collection(db, "card_transactions", _card_transactions)
    load_collection(db, "loan_payments", _loan_payments)
    load_collection(db, "support_tickets", _support_tickets)
    load_collection(db, "employees", _employees)

    # Print summary
    print("\n[ok] seed_mongo done — all 10 collections loaded with synthetic data")
    for name in [
        "customers",
        "accounts",
        "transactions",
        "branches",
        "loans",
        "cards",
        "card_transactions",
        "loan_payments",
        "support_tickets",
        "employees",
    ]:
        count = db[name].count_documents({})
        print(f"  {name}: {count} docs")


if __name__ == "__main__":
    main()
