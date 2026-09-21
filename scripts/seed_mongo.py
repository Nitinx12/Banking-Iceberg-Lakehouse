"""scripts/seed_mongo.py — load sample dataset into local Mongo (PROJECT_PLAN Phase 0).

Usage: uv run python scripts/seed_mongo.py [--scale 20] [--customers 100]
If scale >1, generates scaled synthetic volume. 1=smoke (5/8/20), 20=100/160/400, 100=500/800/2000.
"""

import argparse
import os
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()


def get_uri(args_uri):
    return args_uri or os.getenv(
        "MONGO_URI",
        "mongodb://admin:password@localhost:27017/banking?replicaSet=rs0&authSource=admin",
    )


def _now():
    return datetime(2026, 9, 15, 10, 0, 0)


def _customers(n=5):
    docs = []
    try:
        from faker import Faker

        faker = Faker("en_IN")
        use_faker = n > 20
    except Exception:
        faker = None
        use_faker = False
    base_names = ["Alice Johnson", "Bob Williams", "Carol Davis", "David Chen", "Eva Martinez"]
    cities = ["Pune", "Mumbai", "Delhi", "Bangalore", "Chennai", "Hyderabad", "Kolkata", "Jaipur"]
    states = ["MH", "MH", "DL", "KA", "TN", "TG", "WB", "RJ"]
    occupations = [
        "Engineer",
        "Doctor",
        "Teacher",
        "Business",
        "Student",
        "Salaried - Government",
        "Salaried - Private",
    ]
    for i in range(n):
        name = (
            faker.name()
            if use_faker and faker
            else base_names[i % len(base_names)]
            + (f" {i // len(base_names) + 1}" if i >= len(base_names) else "")
        )
        docs.append(
            {
                "_id": f"cust_{i + 1:05d}",
                "customer_id": i + 1,
                "name": name,
                "gender": "F" if i % 2 == 0 else "M",
                "date_of_birth": f"{1970 + (i % 30):04d}-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
                "city": cities[i % len(cities)],
                "state": states[i % len(states)],
                "phone": 9000000000 + i,
                "email": f"customer{i + 1}@example.com",
                "occupation": occupations[i % len(occupations)],
                "annual_income": 300000 + (i * 12345) % 2000000,
                "join_date": f"2020-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
                "credit_score": 600 + (i % 200),
                "created_at": _now(),
                "updated_at": _now(),
            }
        )
    return docs


def _accounts(n=8, n_customers=5):
    docs = []
    account_types = ["Savings", "Current", "Fixed Deposit", "Recurring"]
    statuses = ["Active", "Active", "Active", "Dormant"]
    for i in range(n):
        docs.append(
            {
                "_id": f"acct_{i + 1:05d}",
                "account_id": i + 1,
                "customer_id": (i % n_customers) + 1,
                "branch_id": (i % 5) + 1,
                "account_type": account_types[i % len(account_types)],
                "open_date": f"2024-{(i % 12) + 1:02d}-15",
                "balance": round(10000 + (i * 1732) % 500000, 2),
                "status": statuses[i % len(statuses)],
                "created_at": _now(),
                "updated_at": _now(),
            }
        )
    return docs


def _transactions(n=20, n_accounts=8):
    docs = []
    channels = ["ATM", "POS", "Online", "Branch", "NEFT"]
    txn_types = ["DEBIT", "CREDIT", "TRANSFER", "WITHDRAWAL"]
    for i in range(n):
        docs.append(
            {
                "_id": f"txn_{i + 1:06d}",
                "transaction_id": i + 1,
                "account_id": (i % n_accounts) + 1,
                "txn_date": f"2026-09-{(i % 28) + 1:02d}",
                "txn_type": txn_types[i % len(txn_types)],
                "amount": round(100 + (i * 317) % 50000, 2),
                "channel": channels[i % len(channels)],
                "created_at": _now(),
                "updated_at": _now(),
            }
        )
    return docs


def _branches(n=5):
    docs = []
    branch_names = [
        "Pune Main",
        "Mumbai Central",
        "Delhi Junction",
        "Bangalore Tech",
        "Chennai Port",
    ]
    cities = ["Pune", "Mumbai", "Delhi", "Bangalore", "Chennai"]
    for i in range(n):
        idx = i % len(branch_names)
        docs.append(
            {
                "_id": f"brn_{i + 1:03d}",
                "branch_id": i + 1,
                "branch_name": branch_names[idx]
                + (f" {i // len(branch_names) + 1}" if i >= len(branch_names) else ""),
                "city": cities[idx],
                "state": ["MH", "MH", "DL", "KA", "TN"][idx],
                "ifsc_code": f"SBIN{i + 1:04d}",
                "created_at": _now(),
                "updated_at": _now(),
            }
        )
    return docs


def _loans(n=6, n_customers=5):
    docs = []
    loan_types = ["Home", "Personal", "Vehicle", "Education", "Gold", "Business"]
    statuses = ["Active", "Active", "Closed", "Active", "Defaulted", "Active"]
    for i in range(n):
        docs.append(
            {
                "_id": f"ln_{i + 1:05d}",
                "loan_id": i + 1,
                "customer_id": (i % n_customers) + 1,
                "branch_id": (i % 5) + 1,
                "loan_type": loan_types[i % len(loan_types)],
                "loan_amount": round(100000 + (i * 50000) % 900000, 2),
                "interest_rate": round(8.5 + (i % 5) * 0.5, 2),
                "term_months": 120 + (i % 60),
                "start_date": f"2025-{(i % 12) + 1:02d}-01",
                "status": statuses[i % len(statuses)],
                "created_at": _now(),
                "updated_at": _now(),
            }
        )
    return docs


def _cards(n=7, n_customers=5, n_accounts=8):
    docs = []
    card_types = ["Visa", "Mastercard", "RuPay", "Amex"]
    statuses = ["Active", "Active", "Blocked", "Active", "Expired", "Active", "Active"]
    for i in range(n):
        docs.append(
            {
                "_id": f"card_{i + 1:05d}",
                "card_id": i + 1,
                "customer_id": (i % n_customers) + 1,
                "account_id": (i % n_accounts) + 1,
                "card_type": card_types[i % len(card_types)],
                "issue_date": f"2023-{(i % 12) + 1:02d}-10",
                "status": statuses[i % len(statuses)],
                "created_at": _now(),
                "updated_at": _now(),
            }
        )
    return docs


def _card_transactions(n=15, n_cards=7):
    docs = []
    for i in range(n):
        docs.append(
            {
                "_id": f"ctxn_{i + 1:06d}",
                "card_txn_id": i + 1,
                "card_id": (i % n_cards) + 1,
                "txn_date": f"2026-09-{(i % 28) + 1:02d}",
                "amount": round(50 + (i * 137) % 20000, 2),
                "is_fraud": 1 if i % 50 == 0 else 0,
                "created_at": _now(),
            }
        )
    return docs


def _loan_payments(n=10, n_loans=6):
    docs = []
    for i in range(n):
        docs.append(
            {
                "_id": f"pay_{i + 1:06d}",
                "payment_id": i + 1,
                "loan_id": (i % n_loans) + 1,
                "payment_date": f"2026-08-{(i % 28) + 1:02d}",
                "amount_paid": round(5000 + (i * 311) % 20000, 2),
                "principal_component": round(4000 + (i * 211) % 15000, 2),
                "interest_component": round(1000 + (i % 5) * 200, 2),
                "created_at": _now(),
            }
        )
    return docs


def _support_tickets(n=5, n_customers=5):
    docs = []
    statuses = ["Open", "Resolved", "In Progress", "Closed", "Escalated"]
    for i in range(n):
        docs.append(
            {
                "_id": f"tk_{i + 1:05d}",
                "ticket_id": i + 1,
                "customer_id": (i % n_customers) + 1,
                "issue_type": ["Card", "Account", "Loan", "App", "Other"][i % 5],
                "date_opened": f"2026-09-{(i % 28) + 1:02d}",
                "status": statuses[i % 5],
                "satisfaction_score": 1 + (i % 5),
                "created_at": _now(),
            }
        )
    return docs


def _employees(n=4):
    docs = []
    roles = ["Manager", "Officer", "Clerk", "Assistant"]
    for i in range(n):
        docs.append(
            {
                "_id": f"emp_{i + 1:03d}",
                "employee_id": i + 1,
                "name": f"Employee {i + 1}",
                "branch_id": (i % 5) + 1,
                "role": roles[i % len(roles)],
                "hire_date": f"2021-{(i % 12) + 1:02d}-05",
                "salary": 500000 + (i * 150000),
                "created_at": _now(),
                "updated_at": _now(),
            }
        )
    return docs


def load_collection(db, name, docs):
    db[name].delete_many({})
    if docs:
        db[name].insert_many(docs)
    print(f"  loaded {len(docs)} docs into {name}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", default=None)
    ap.add_argument(
        "--scale",
        type=int,
        default=1,
        help="scale factor (1=5/8/20, 20=100/160/400, 100=500/800/2000)",
    )
    ap.add_argument("--customers", type=int, default=None)
    ap.add_argument("--accounts", type=int, default=None)
    ap.add_argument("--transactions", type=int, default=None)
    ap.add_argument("--card-transactions", type=int, default=None, dest="card_transactions")
    ap.add_argument("--loans", type=int, default=None)
    ap.add_argument("--cards", type=int, default=None)
    args = ap.parse_args()
    uri = get_uri(args.uri)

    scale = args.scale
    n_customers = args.customers or 5 * scale
    n_accounts = args.accounts or 8 * scale
    n_transactions = args.transactions or 20 * scale
    n_card_txns = args.card_transactions or 15 * scale
    n_loans = args.loans or 6 * scale
    n_cards = args.cards or 7 * scale
    n_branches = 5  # keep branches small (Type1 dim)
    n_payments = 10 * scale
    n_tickets = 5 * scale
    n_employees = 4

    print(
        f"scale={scale} -> customers={n_customers} accounts={n_accounts} transactions={n_transactions} card_txns={n_card_txns}"
    )

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
        return

    db = client.get_database()
    print(f"[ok] connected to db={db.name}")

    load_collection(db, "customers", _customers(n_customers))
    load_collection(db, "accounts", _accounts(n_accounts, n_customers))
    load_collection(db, "transactions", _transactions(n_transactions, n_accounts))
    load_collection(db, "branches", _branches(n_branches))
    load_collection(db, "loans", _loans(n_loans, n_customers))
    load_collection(db, "cards", _cards(n_cards, n_customers, n_accounts))
    load_collection(db, "card_transactions", _card_transactions(n_card_txns, n_cards))
    load_collection(db, "loan_payments", _loan_payments(n_payments, n_loans))
    load_collection(db, "support_tickets", _support_tickets(n_tickets, n_customers))
    load_collection(db, "employees", _employees(n_employees))

    print("\n[ok] seed_mongo done")
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
        print(f"  {name}: {db[name].count_documents({})} docs")


if __name__ == "__main__":
    main()
