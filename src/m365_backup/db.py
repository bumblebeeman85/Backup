import os
import hashlib
import json
from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlalchemy import (
    Table,
    Column,
    Integer,
    String,
    DateTime,
    MetaData,
    Text,
    ForeignKey,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.orm import registry, sessionmaker

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@db:5432/m365backup"
)

# Use sync SQLAlchemy for now via a small sync engine wrapper using asyncpg only for connection URL.
# For production you may prefer async SQLAlchemy usage.
engine = create_engine(DATABASE_URL.replace("+asyncpg", ""), future=True)
Session = sessionmaker(bind=engine, future=True)
metadata = MetaData()

mapper_registry = registry()


snapshots_table = Table(
    "snapshots",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("created_at", DateTime, default=datetime.utcnow),
    Column("label", String(255)),
)

messages_table = Table(
    "messages",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("snapshot_id", Integer, ForeignKey("snapshots.id", ondelete="CASCADE")),
    Column("tenant", String(255)),
    Column("user_principal", String(255)),
    Column("message_id", String(255)),
    Column("message_hash", String(64)),
    Column("raw_json", Text),
    UniqueConstraint("message_hash", name="uq_message_hash"),
)


def init_db():
    metadata.create_all(engine)


def compute_hash(message: Dict[str, Any]) -> str:
    # compute a stable hash from important fields
    keys = ["subject", "from", "to", "cc", "bcc", "receivedDateTime"]
    rep = {k: message.get(k) for k in keys}
    s = json.dumps(rep, sort_keys=True, default=str)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def store_snapshot(label: Optional[str], collected: List[Dict[str, Any]]):
    """Store a snapshot and messages. Performs global dedup based on computed message_hash.

    collected: list of dicts with keys: tenant, user_principal, message_id, message_json
    """
    session = Session()
    try:
        from sqlalchemy import insert

        # create snapshot
        res = session.execute(snapshots_table.insert().values(label=label))
        snapshot_id = res.inserted_primary_key[0]
        inserted = 0
        for item in collected:
            message_json = item["message_json"]
            mhash = compute_hash(message_json)
            # check dedup
            q = session.execute(
                messages_table.select().where(messages_table.c.message_hash == mhash)
            )
            if q.first():
                continue
            session.execute(
                messages_table.insert().values(
                    snapshot_id=snapshot_id,
                    tenant=item.get("tenant"),
                    user_principal=item.get("user_principal"),
                    message_id=item.get("message_id"),
                    message_hash=mhash,
                    raw_json=json.dumps(message_json, ensure_ascii=False),
                )
            )
            inserted += 1
        session.commit()
        return snapshot_id, inserted
    finally:
        session.close()


def list_snapshots(limit: int = 50):
    session = Session()
    try:
        res = session.execute(
            snapshots_table.select()
            .order_by(snapshots_table.c.created_at.desc())
            .limit(limit)
        )
        return [dict(row) for row in res]
    finally:
        session.close()


def get_snapshot_messages(snapshot_id: int):
    session = Session()
    try:
        res = session.execute(
            messages_table.select().where(messages_table.c.snapshot_id == snapshot_id)
        )
        return [dict(row) for row in res]
    finally:
        session.close()
