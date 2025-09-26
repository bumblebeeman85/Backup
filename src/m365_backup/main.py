#!/usr/bin/env python3
"""M365 Mailbox backup tool

This module provides a simple backup-only implementation for Microsoft 365 mailboxes.
It reads a `tenants.yaml` file (or uses environment vars) describing one or more tenants,
acquires app-only tokens via MSAL, lists users, and downloads messages + attachments.

Secrets must be provided via environment variables or the tenants file. Do NOT commit secrets.
"""
import os
import sys
import json
import base64
import logging
import argparse
from typing import Dict, Any, List, Optional

import requests
import msal
import yaml
from email import policy
from email.parser import BytesParser
from email.utils import getaddresses

# local modules
from . import db
from .scheduler import start_scheduler

# Configuration via env vars (can be overridden per-tenant)
BACKUP_DIR = os.environ.get("BACKUP_DIR", "./m365_mail_backups")
DEFAULT_MAILS_PER_USER = int(os.environ.get("MAILS_PER_USER", "200"))
DEFAULT_DOWNLOAD_ATTACHMENTS = os.environ.get("DOWNLOAD_ATTACHMENTS", "true").lower() in ("1", "true", "yes")
SCOPES = ["https://graph.microsoft.com/.default"]

# logging
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger("m365_backup")

os.makedirs(BACKUP_DIR, exist_ok=True)

# --- Utilities ---

def load_tenants(path: str = "tenants.yaml") -> List[Dict[str, Any]]:
    """Load tenant definitions from YAML file. Returns a list of tenant dicts."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Tenants file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    tenants = data.get("tenants") or []
    # allow single-tenant top-level mapping
    if isinstance(tenants, dict):
        tenants = [tenants]
    return tenants

# --- Auth ---

def get_token_for_tenant(client_id: str, client_secret: str, tenant_id: str, scopes: List[str] = SCOPES) -> str:
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(client_id, authority=authority, client_credential=client_secret)
    # try silent (useful if persistent cache added later)
    result = app.acquire_token_silent(scopes, account=None)
    if not result:
        result = app.acquire_token_for_client(scopes=scopes)
    if not result or "access_token" not in result:
        raise RuntimeError(f"Failed to acquire token: {result}")
    return result["access_token"]

# --- Graph helpers ---

def has_mailbox(user_id: str, token: str) -> bool:
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/mailFolders/Inbox"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers)
    return r.status_code == 200


def download_message_attachments(user_id: str, msg_id: str, token: str, attach_target_dir: str) -> None:
    os.makedirs(attach_target_dir, exist_ok=True)
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages/{msg_id}/attachments"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        logger.warning("Failed to fetch attachments for %s: %s", msg_id, r.status_code)
        return
    data = r.json()
    for att in data.get("value", []):
        name = att.get("name") or att.get("id")
        content_bytes = att.get("contentBytes")
        if content_bytes:
            try:
                decoded = base64.b64decode(content_bytes)
                path = os.path.join(attach_target_dir, name)
                with open(path, "wb") as f:
                    f.write(decoded)
                logger.debug("Saved attachment %s", path)
            except Exception as e:
                logger.exception("Error saving attachment %s: %s", name, e)
        else:
            meta_path = os.path.join(attach_target_dir, f"{name}.json")
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(att, f, indent=2, ensure_ascii=False)
            logger.debug("Saved attachment metadata %s", meta_path)


def backup_mailbox(user: Dict[str, Any], token: str, mails_per_user: Optional[int], download_attachments: bool, tenant_dir: str) -> List[Dict[str, Any]]:
    user_dir = os.path.join(tenant_dir, user.get("userPrincipalName") or user.get("id"))
    os.makedirs(user_dir, exist_ok=True)
    logger.info("Backing up %s (%s)", user.get("displayName"), user.get("userPrincipalName"))

    remaining = mails_per_user if mails_per_user is not None else None
    page_size = 100 if (remaining is None or remaining > 100) else remaining
    url = f"https://graph.microsoft.com/v1.0/users/{user['id']}/messages?$top={page_size}"
    headers = {"Authorization": f"Bearer {token}"}
    downloaded = 0
    collected: List[Dict[str, Any]] = []

    while url:
        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            logger.warning("Error fetching messages for %s: %s - %s", user.get("id"), r.status_code, r.text)
            break
        data = r.json()
        for msg in data.get("value", []):
            msg_id = msg["id"]
            filename = os.path.join(user_dir, f"{msg_id}.json")
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(msg, f, indent=2, ensure_ascii=False)
            # Save raw MIME (.eml)
            try:
                mime_url = f"https://graph.microsoft.com/v1.0/users/{user['id']}/messages/{msg_id}/$value"
                rm = requests.get(mime_url, headers=headers)
                if rm.status_code == 200:
                    eml_path = os.path.join(user_dir, f"{msg_id}.eml")
                    with open(eml_path, "wb") as ef:
                        ef.write(rm.content)
                    logger.debug("Saved raw EML %s", eml_path)
                else:
                    logger.debug("Could not fetch raw MIME for %s: %s", msg_id, rm.status_code)
            except Exception:
                logger.exception("Error fetching raw MIME for %s", msg_id)
            downloaded += 1
            try:
                collected.append({
                    'tenant': os.path.basename(tenant_dir),
                    'user_principal': user.get('userPrincipalName'),
                    'message_id': msg_id,
                    'message_json': msg,
                })
            except Exception:
                logger.exception('Failed to collect message for DB')
            if download_attachments:
                attach_dir = os.path.join(user_dir, "attachments", msg_id)
                download_message_attachments(user["id"], msg_id, token, attach_dir)
                if remaining is not None and downloaded >= mails_per_user:
                    url = None
                    break
        if url:
            url = data.get("@odata.nextLink", None)
            if url and remaining is not None:
                left = mails_per_user - downloaded
                if left <= 0:
                    url = None
                else:
                    if left < page_size:
                        url = f"https://graph.microsoft.com/v1.0/users/{user['id']}/messages?$top={left}"

    # finished paging
    return collected

def backup_tenant(tenant: Dict[str, Any], global_options: Dict[str, Any]) -> List[Dict[str, Any]]:
    # get credentials from tenant dict or env vars
    client_id = tenant.get("client_id") or os.environ.get("CLIENT_ID")
    client_secret = tenant.get("client_secret") or os.environ.get("CLIENT_SECRET")
    tenant_id = tenant.get("tenant_id") or os.environ.get("TENANT_ID")
    if not (client_id and client_secret and tenant_id):
        logger.error("Missing credentials for tenant: %s", tenant.get("name") or tenant_id)
        return []
    mails_per_user = tenant.get("mails_per_user", global_options.get("mails_per_user"))
    download_attachments_flag = tenant.get("download_attachments", global_options.get("download_attachments"))

    # token
    try:
        token = get_token_for_tenant(client_id, client_secret, tenant_id)
    except Exception as e:
        logger.exception("Failed to get token for tenant %s: %s", tenant.get("name"), e)
        return []

    # create tenant dir
    tenant_dir = os.path.join(BACKUP_DIR, tenant.get("name") or tenant_id)
    os.makedirs(tenant_dir, exist_ok=True)

    # list users
    url = "https://graph.microsoft.com/v1.0/users?$select=id,displayName,userPrincipalName"
    headers = {"Authorization": f"Bearer {token}"}
    users: List[Dict[str, Any]] = []
    while url:
        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            logger.error("Failed to list users for tenant %s: %s", tenant.get("name"), r.text)
            return []
        data = r.json()
        users.extend(data.get("value", []))
        url = data.get("@odata.nextLink", None)
    logger.info("Found %d users for tenant %s", len(users), tenant.get("name"))

    collected_all: List[Dict[str, Any]] = []
    for user in users:
        if has_mailbox(user["id"], token):
            try:
                msgs = backup_mailbox(user, token, mails_per_user, download_attachments_flag, tenant_dir)
                if msgs:
                    collected_all.extend(msgs)
            except Exception:
                logger.exception('Error backing up user %s', user.get('userPrincipalName'))
        else:
            logger.debug("Skipping user without mailbox: %s", user.get("userPrincipalName"))

    return collected_all

# --- Restore helpers ---

def parse_eml(eml_path: str):
    """Parse an .eml file and return a dict for Graph message and a list of attachments.

    Returns: (message_dict, attachments_list)
    attachments_list: list of dicts with keys: name, contentBytes (base64), contentType
    """
    with open(eml_path, "rb") as f:
        msg = BytesParser(policy=policy.default).parse(f)

    subject = msg.get("subject", "")
    # body: prefer html
    body_html = None
    body_text = None
    attachments = []

    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            disp = part.get_content_disposition()
            if disp == "attachment" or part.get_filename():
                name = part.get_filename() or "attachment"
                payload = part.get_payload(decode=True) or b""
                attachments.append({
                    "name": name,
                    "contentBytes": base64.b64encode(payload).decode("ascii"),
                    "contentType": part.get_content_type(),
                })
            elif ctype == "text/html" and body_html is None:
                body_html = part.get_content()
            elif ctype == "text/plain" and body_text is None:
                body_text = part.get_content()
    else:
        ctype = msg.get_content_type()
        if ctype == "text/html":
            body_html = msg.get_content()
        else:
            body_text = msg.get_content()

    # recipients
    tos = [a[1] for a in getaddresses(msg.get_all("to", []) )]
    ccs = [a[1] for a in getaddresses(msg.get_all("cc", []) )]
    bccs = [a[1] for a in getaddresses(msg.get_all("bcc", []) )]

    recipients = []
    for addr in tos:
        recipients.append({"emailAddress": {"address": addr}})
    for addr in ccs:
        recipients.append({"emailAddress": {"address": addr}})

    body = {"contentType": "HTML" if body_html else "Text", "content": body_html or body_text or ""}

    message = {
        "subject": subject,
        "body": body,
        "toRecipients": recipients,
    }

    return message, attachments


def restore_user_from_eml(user_principal: str, user_id: str, token: str, user_dir: str, dry_run: bool = False) -> None:
    """Restore all .eml files found in user_dir into the user's Inbox."""
    eml_dir = user_dir
    files = [f for f in os.listdir(eml_dir) if f.endswith('.eml')]
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    for fname in files:
        path = os.path.join(eml_dir, fname)
        logger.info("Restoring %s -> %s", path, user_principal)
        message_json, attachments = parse_eml(path)
        if dry_run:
            logger.debug("Dry-run message: %s", message_json)
            continue

        # create message directly in Inbox
        url = f"https://graph.microsoft.com/v1.0/users/{user_id}/mailFolders/Inbox/messages"
        r = requests.post(url, headers=headers, json=message_json)
        if r.status_code not in (200, 201):
            logger.error("Failed to create message for %s: %s %s", user_principal, r.status_code, r.text)
            continue
        created = r.json()
        msg_id = created.get('id')

        # attachments (simple path: small files via contentBytes)
        for att in attachments:
            att_payload = {
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": att['name'],
                "contentType": att.get('contentType', 'application/octet-stream'),
                "contentBytes": att['contentBytes']
            }
            aurl = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages/{msg_id}/attachments"
            ar = requests.post(aurl, headers=headers, json=att_payload)
            if ar.status_code not in (200, 201):
                logger.error("Failed to attach %s to %s: %s %s", att['name'], user_principal, ar.status_code, ar.text)
        logger.info("Restored %s as message %s", path, msg_id)


# --- CLI ---

def cli(argv: Optional[List[str]] = None) -> int:
    global BACKUP_DIR
    parser = argparse.ArgumentParser(description="M365 mailbox backup tool")
    parser.add_argument("action", choices=["backup", "restore", "web", "snapshot"], help="Action to perform")
    parser.add_argument("--tenants", default="tenants.yaml", help="Path to tenants YAML file")
    parser.add_argument("--backup-dir", default=BACKUP_DIR, help="Directory to store backups")
    parser.add_argument("--mails-per-user", type=int, default=DEFAULT_MAILS_PER_USER)
    parser.add_argument("--no-attachments", dest="attachments", action="store_false", default=DEFAULT_DOWNLOAD_ATTACHMENTS)
    args = parser.parse_args(argv)

    BACKUP_DIR = args.backup_dir
    os.makedirs(BACKUP_DIR, exist_ok=True)

    if args.action == "restore":
        logger.warning("Restore is not implemented yet. Exiting.")
        return 2

    if args.action == "web":
        # run uvicorn directly when using python -m m365_backup.main web
        try:
            import uvicorn
        except Exception:
            logger.error('uvicorn not installed')
            return 2
        # serve app
        uvicorn.run('m365_backup.web:app', host='0.0.0.0', port=6666)
        return 0

    if args.action == 'snapshot':
        label = os.environ.get('SNAPSHOT_LABEL') or None
        # load tenants
        tenants = load_tenants(args.tenants)
        global_options = {"mails_per_user": args.mails_per_user, "download_attachments": args.attachments}
        # initialize DB and store snapshot results
        db.init_db()
        total_inserted = 0
        for tenant in tenants:
            collected = backup_tenant(tenant, global_options)
            if collected:
                sid, inserted = db.store_snapshot(label, collected)
                total_inserted += inserted
                logger.info('Snapshot stored %s inserted %d', sid, inserted)
        logger.info('Snapshot complete, inserted %d messages', total_inserted)
        return 0

    # load tenants
    try:
        tenants = load_tenants(args.tenants)
    except Exception as e:
        logger.exception("Failed to load tenants file: %s", e)
        return 1

    global_options = {"mails_per_user": args.mails_per_user, "download_attachments": args.attachments}

    for tenant in tenants:
        backup_tenant(tenant, global_options)

    return 0


if __name__ == "__main__":
    raise SystemExit(cli())
