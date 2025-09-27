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
    Column("eml_file_path", String(512)),  # Path to EML file
    Column("subject", Text),  # Extracted subject for easy access
    Column("from_address", String(255)),  # Sender email
    Column("received_datetime", DateTime),  # When email was received
    Column("body_text", Text),  # Plain text body content
    Column("body_html", Text),  # HTML body content
    Column("has_attachments", Integer, default=0),  # Boolean as integer
    Column("attachment_count", Integer, default=0),  # Number of attachments
    Column("importance", String(20), default='normal'),  # Email importance
    UniqueConstraint("message_hash", name="uq_message_hash"),
)

tenants_table = Table(
    "tenants",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(255), nullable=False),
    Column("tenant_id", String(255), nullable=False),
    Column("client_id", String(255), nullable=False),
    Column("client_secret", String(500), nullable=False),
    Column("created_at", DateTime, default=datetime.utcnow),
    Column("updated_at", DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
    Column("is_active", Integer, default=1),
    UniqueConstraint("tenant_id", name="uq_tenant_id"),
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
            # Extract additional fields for easier access
            text_content = item.get('text_content', {})
            eml_path = item.get('eml_file_path', '')
            
            # Parse received datetime
            received_dt = None
            received_datetime_str = text_content.get('received_datetime') or message_json.get('receivedDateTime')
            if received_datetime_str:
                try:
                    received_dt = datetime.fromisoformat(received_datetime_str.replace('Z', '+00:00'))
                except:
                    pass
            
            session.execute(
                messages_table.insert().values(
                    snapshot_id=snapshot_id,
                    tenant=item.get("tenant"),
                    user_principal=item.get("user_principal"),
                    message_id=item.get("message_id"),
                    message_hash=mhash,
                    raw_json=json.dumps(message_json, ensure_ascii=False),
                    eml_file_path=eml_path,
                    subject=text_content.get('subject', message_json.get('subject', '')),
                    from_address=text_content.get('from_address', ''),
                    received_datetime=received_dt,
                    body_text=text_content.get('body_text', ''),
                    body_html=text_content.get('body_html', ''),
                    has_attachments=1 if text_content.get('has_attachments', False) else 0,
                    attachment_count=text_content.get('attachment_count', 0),
                    importance=text_content.get('importance', 'normal'),
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
        return [dict(row._mapping) for row in res]
    finally:
        session.close()


def get_snapshot_messages(snapshot_id: int):
    session = Session()
    try:
        res = session.execute(
            messages_table.select().where(messages_table.c.snapshot_id == snapshot_id)
        )
        return [dict(row._mapping) for row in res]
    finally:
        session.close()


# Tenant management functions
def create_tenant(name: str, tenant_id: str, client_id: str, client_secret: str):
    """Create a new tenant with credentials."""
    session = Session()
    try:
        # Insert tenant
        res = session.execute(
            tenants_table.insert().values(
                name=name,
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
            )
        )
        tenant_db_id = res.inserted_primary_key[0]
        session.commit()
        return tenant_db_id
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def list_tenants():
    """List all active tenants."""
    session = Session()
    try:
        # Get tenants
        res = session.execute(
            tenants_table.select().where(tenants_table.c.is_active == 1).order_by(tenants_table.c.name)
        )
        return [dict(row._mapping) for row in res]
    finally:
        session.close()


def get_tenant(tenant_db_id: int):
    """Get a specific tenant by ID."""
    session = Session()
    try:
        res = session.execute(
            tenants_table.select().where(tenants_table.c.id == tenant_db_id)
        )
        tenant = res.first()
        if not tenant:
            return None
        
        return dict(tenant._mapping)
    finally:
        session.close()


def update_tenant(tenant_db_id: int, name: str = None, tenant_id: str = None, 
                 client_id: str = None, client_secret: str = None):
    """Update a tenant's information."""
    session = Session()
    try:
        # Update tenant basic info
        update_values = {"updated_at": datetime.utcnow()}
        if name is not None:
            update_values["name"] = name
        if tenant_id is not None:
            update_values["tenant_id"] = tenant_id
        if client_id is not None:
            update_values["client_id"] = client_id
        if client_secret is not None:
            update_values["client_secret"] = client_secret
            
        session.execute(
            tenants_table.update().where(tenants_table.c.id == tenant_db_id).values(**update_values)
        )
        
        session.commit()
        return True
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def delete_tenant(tenant_db_id: int):
    """Soft delete a tenant (mark as inactive)."""
    session = Session()
    try:
        session.execute(
            tenants_table.update().where(tenants_table.c.id == tenant_db_id).values(is_active=0)
        )
        session.commit()
        return True
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def get_tenants_for_backup():
    """Get tenants in the format expected by the backup system."""
    tenants = list_tenants()
    result = []
    
    for tenant in tenants:
        result.append({
            "name": tenant["name"],
            "tenant_id": tenant["tenant_id"],
            "client_id": tenant["client_id"],
            "client_secret": tenant["client_secret"],
        })
    
    return result


def get_message_by_id(message_id: int):
    """Get a specific message by its database ID."""
    session = Session()
    try:
        q = session.execute(
            messages_table.select().where(messages_table.c.id == message_id)
        )
        row = q.fetchone()
        if not row:
            return None
        
        message_data = dict(row._mapping)
        
        # Parse the raw JSON to get full message data
        if message_data.get('raw_json'):
            try:
                raw_message = json.loads(message_data['raw_json'])
                message_data['raw_message'] = raw_message
            except:
                message_data['raw_message'] = {}
        
        return message_data
    finally:
        session.close()
