import os
from typing import Optional, Dict, Any
from fastapi import FastAPI, Request, HTTPException, Form
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sse_starlette.sse import EventSourceResponse
import json
import asyncio
from celery.result import AsyncResult

from . import db as _db
from .tasks import celery_app, backup_tenant_async, backup_all_tenants_async
from .search import MeilisearchClient

app = FastAPI(title="M365 Iron Backup GUI")

BASE_DIR = os.path.dirname(__file__)
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
app.mount(
    "/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static"
)


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    snaps = _db.list_snapshots(50)
    return templates.TemplateResponse(
        "index.html", {"request": request, "snapshots": snaps}
    )


@app.get("/snapshots/{snapshot_id}", response_class=HTMLResponse)
def snapshot_detail(request: Request, snapshot_id: int):
    snaps = _db.get_snapshot_messages(snapshot_id)
    if snaps is None:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    return templates.TemplateResponse(
        "snapshot.html",
        {"request": request, "messages": snaps, "snapshot_id": snapshot_id},
    )


# Tenant management routes
@app.get("/tenants", response_class=HTMLResponse)
def tenants_list(request: Request):
    tenants = _db.list_tenants()
    return templates.TemplateResponse(
        "tenants.html", {"request": request, "tenants": tenants}
    )


@app.get("/tenants/new", response_class=HTMLResponse)
def tenant_new(request: Request):
    return templates.TemplateResponse(
        "tenant_form.html", {"request": request, "tenant": None, "action": "create"}
    )


@app.post("/tenants/new")
def tenant_create(
    request: Request,
    name: str = Form(...),
    tenant_id: str = Form(...),
    client_id: str = Form(...),
    client_secret: str = Form(...),
):
    try:
        _db.create_tenant(name, tenant_id, client_id, client_secret)
        return RedirectResponse(url="/tenants", status_code=303)
    except Exception as e:
        return templates.TemplateResponse(
            "tenant_form.html",
            {
                "request": request,
                "tenant": {
                    "name": name,
                    "tenant_id": tenant_id,
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
                "action": "create",
                "error": str(e),
            },
        )


@app.get("/tenants/{tenant_id}/edit", response_class=HTMLResponse)
def tenant_edit(request: Request, tenant_id: int):
    tenant = _db.get_tenant(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return templates.TemplateResponse(
        "tenant_form.html", {"request": request, "tenant": tenant, "action": "edit"}
    )


@app.post("/tenants/{tenant_id}/edit")
def tenant_update(
    request: Request,
    tenant_id: int,
    name: str = Form(...),
    tenant_id_field: str = Form(...),
    client_id: str = Form(...),
    client_secret: str = Form(...),
):
    try:
        _db.update_tenant(tenant_id, name, tenant_id_field, client_id, client_secret)
        return RedirectResponse(url="/tenants", status_code=303)
    except Exception as e:
        tenant = _db.get_tenant(tenant_id)
        return templates.TemplateResponse(
            "tenant_form.html",
            {
                "request": request,
                "tenant": tenant,
                "action": "edit",
                "error": str(e),
            },
        )


@app.post("/tenants/{tenant_id}/delete")
def tenant_delete(tenant_id: int):
    _db.delete_tenant(tenant_id)
    return RedirectResponse(url="/tenants", status_code=303)


# API endpoints for manual backup
@app.post("/api/backup/trigger")
def trigger_backup(
    mails_per_user: int = 200,
    download_attachments: bool = True,
    label: Optional[str] = None
):
    """Trigger a manual backup of all configured tenants with custom options."""
    try:
        from . import main
        tenants = _db.get_tenants_for_backup()
        
        if not tenants:
            return {"error": "No tenants configured"}
        
        backup_options = {
            'mails_per_user': mails_per_user, 
            'download_attachments': download_attachments
        }
        
        total_collected = []
        tenant_results = []
        
        for tenant in tenants:
            try:
                collected = main.backup_tenant(tenant, backup_options)
                tenant_result = {
                    "tenant_name": tenant['name'],
                    "messages_collected": len(collected) if collected else 0,
                    "status": "success"
                }
                if collected:
                    total_collected.extend(collected)
                tenant_results.append(tenant_result)
            except Exception as e:
                tenant_results.append({
                    "tenant_name": tenant['name'],
                    "messages_collected": 0,
                    "status": "error",
                    "error": str(e)
                })
                print(f"Error backing up tenant {tenant['name']}: {e}")
        
        if total_collected:
            _db.init_db()  # Ensure tables exist
            snapshot_label = label or 'manual-web'
            snapshot_id, inserted = _db.store_snapshot(snapshot_label, total_collected)
            return {
                "success": True, 
                "snapshot_id": snapshot_id, 
                "messages_inserted": inserted,
                "total_messages": len(total_collected),
                "tenant_results": tenant_results,
                "backup_options": backup_options
            }
        else:
            return {
                "error": "No messages collected", 
                "tenant_results": tenant_results
            }
            
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/backup/tenant/{tenant_id}")
def trigger_tenant_backup(
    tenant_id: int,
    mails_per_user: int = 200,
    download_attachments: bool = True,
    label: Optional[str] = None
):
    """Trigger a manual backup of a specific tenant."""
    try:
        from . import main
        tenant_data = _db.get_tenant(tenant_id)
        
        if not tenant_data:
            return {"error": "Tenant not found"}
        
        # Convert to backup format
        tenant = {
            "name": tenant_data["name"],
            "tenant_id": tenant_data["tenant_id"],
            "client_id": tenant_data["client_id"],
            "client_secret": tenant_data["client_secret"],
        }
        
        backup_options = {
            'mails_per_user': mails_per_user, 
            'download_attachments': download_attachments
        }
        
        collected = main.backup_tenant(tenant, backup_options)
        
        if collected:
            _db.init_db()  # Ensure tables exist
            snapshot_label = label or f'manual-{tenant["name"]}'
            snapshot_id, inserted = _db.store_snapshot(snapshot_label, collected)
            return {
                "success": True, 
                "snapshot_id": snapshot_id, 
                "messages_inserted": inserted,
                "total_messages": len(collected),
                "tenant_name": tenant["name"],
                "backup_options": backup_options
            }
        else:
            return {"error": f"No messages collected for tenant {tenant['name']}"}
            
    except Exception as e:
        return {"error": str(e)}


@app.get("/backup", response_class=HTMLResponse)
def backup_page(request: Request):
    """Show backup configuration and trigger page."""
    tenants = _db.list_tenants()
    snapshots = _db.list_snapshots(10)  # Last 10 snapshots
    return templates.TemplateResponse(
        "backup.html", 
        {
            "request": request, 
            "tenants": tenants, 
            "recent_snapshots": snapshots
        }
    )


# Asynchronous Backup API Endpoints
@app.post("/api/backup/async/trigger")
def trigger_async_backup(
    mails_per_user: int = 200,
    download_attachments: bool = True,
    label: Optional[str] = None
):
    """Trigger an asynchronous backup of all tenants."""
    try:
        tenants = _db.get_tenants_for_backup()
        if not tenants:
            return {"error": "No tenants configured"}
        
        options = {
            'mails_per_user': mails_per_user,
            'download_attachments': download_attachments,
            'label': label
        }
        
        # Start async task
        task = backup_all_tenants_async.delay(options)
        
        return {
            "success": True,
            "task_id": task.id,
            "message": f"Backup started for {len(tenants)} tenants",
            "status_url": f"/api/backup/status/{task.id}"
        }
        
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/backup/async/tenant/{tenant_id}")
def trigger_async_tenant_backup(
    tenant_id: int,
    mails_per_user: int = 200,
    download_attachments: bool = True,
    label: Optional[str] = None
):
    """Trigger an asynchronous backup of a specific tenant."""
    try:
        tenant_data = _db.get_tenant(tenant_id)
        if not tenant_data:
            return {"error": "Tenant not found"}
        
        # Convert to format expected by task
        tenant = {
            "name": tenant_data["name"],
            "tenant_id": tenant_data["tenant_id"],
            "client_id": tenant_data["client_id"],
            "client_secret": tenant_data["client_secret"]
        }
        
        options = {
            'mails_per_user': mails_per_user,
            'download_attachments': download_attachments,
            'label': label or f'async-{tenant["name"]}'
        }
        
        # Start async task
        task = backup_tenant_async.delay(tenant, options)
        
        return {
            "success": True,
            "task_id": task.id,
            "tenant_name": tenant["name"],
            "message": f"Backup started for tenant {tenant['name']}",
            "status_url": f"/api/backup/status/{task.id}"
        }
        
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/backup/status/{task_id}")
def get_backup_status(task_id: str):
    """Get the status of an asynchronous backup task."""
    try:
        task = AsyncResult(task_id, app=celery_app)
        
        if task.state == 'PENDING':
            return {
                "task_id": task_id,
                "state": "PENDING",
                "progress": 0,
                "message": "Task is waiting to be processed..."
            }
        elif task.state == 'PROGRESS':
            return {
                "task_id": task_id,
                "state": "PROGRESS",
                "progress": task.info.get('progress', 0),
                "stage": task.info.get('stage', 'unknown'),
                "message": task.info.get('message', 'Processing...')
            }
        elif task.state == 'SUCCESS':
            return {
                "task_id": task_id,
                "state": "SUCCESS",
                "progress": 100,
                "message": "Backup completed successfully!",
                "result": task.result
            }
        elif task.state == 'FAILURE':
            return {
                "task_id": task_id,
                "state": "FAILURE", 
                "progress": 0,
                "message": f"Backup failed: {str(task.info)}",
                "error": str(task.info)
            }
        else:
            return {
                "task_id": task_id,
                "state": task.state,
                "progress": 0,
                "message": f"Unknown task state: {task.state}"
            }
            
    except Exception as e:
        return {"error": f"Failed to get task status: {str(e)}"}


@app.get("/api/backup/status/{task_id}/stream")
async def stream_backup_status(task_id: str):
    """Stream real-time backup status updates via Server-Sent Events."""
    async def event_stream():
        last_state = None
        
        while True:
            try:
                task = AsyncResult(task_id, app=celery_app)
                
                # Get current status
                if task.state == 'PENDING':
                    status = {
                        "task_id": task_id,
                        "state": "PENDING",
                        "progress": 0,
                        "message": "Task is waiting to be processed..."
                    }
                elif task.state == 'PROGRESS':
                    status = {
                        "task_id": task_id,
                        "state": "PROGRESS",
                        "progress": task.info.get('progress', 0),
                        "stage": task.info.get('stage', 'unknown'),
                        "message": task.info.get('message', 'Processing...')
                    }
                elif task.state == 'SUCCESS':
                    status = {
                        "task_id": task_id,
                        "state": "SUCCESS",
                        "progress": 100,
                        "message": "Backup completed successfully!",
                        "result": task.result
                    }
                elif task.state == 'FAILURE':
                    status = {
                        "task_id": task_id,
                        "state": "FAILURE",
                        "progress": 0,
                        "message": f"Backup failed: {str(task.info)}",
                        "error": str(task.info)
                    }
                else:
                    status = {
                        "task_id": task_id,
                        "state": task.state,
                        "progress": 0,
                        "message": f"Task state: {task.state}"
                    }
                
                # Send update if state changed
                if status != last_state:
                    yield f"data: {json.dumps(status)}\n\n"
                    last_state = status.copy()
                
                # Exit if task finished
                if task.state in ['SUCCESS', 'FAILURE']:
                    break
                    
                await asyncio.sleep(1)  # Check every second
                
            except Exception as e:
                yield f"data: {json.dumps({'error': str(e)})}\n\n"
                break
    
    return EventSourceResponse(event_stream())


# Search API Endpoints  
@app.get("/search")
def search_page(request: Request):
    """Show search interface page."""
    return templates.TemplateResponse(
        "search.html",
        {"request": request}
    )


@app.get("/api/search")
def search_emails(
    q: str = "",
    snapshot_id: Optional[int] = None,
    from_address: Optional[str] = None,
    limit: int = 20,
    offset: int = 0
):
    """Search emails using Meilisearch."""
    try:
        search_client = MeilisearchClient()
        
        # Build filters
        filters = {}
        if snapshot_id:
            filters['snapshot_id'] = snapshot_id
        if from_address:
            filters['from_address'] = from_address
        
        # Perform search
        results = search_client.search_messages(
            query=q,
            filters=filters if filters else None,
            limit=limit,
            offset=offset,
            sort=['received_datetime:desc'] if not q else None  # Sort by date if no query
        )
        
        return results
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "hits": [],
            "total_hits": 0
        }


@app.get("/api/search/stats")
def get_search_stats():
    """Get search index statistics."""
    try:
        search_client = MeilisearchClient()
        return search_client.get_index_stats()
    except Exception as e:
        return {"success": False, "error": str(e)}


# EML File serving endpoints
@app.get("/api/message/{message_id}/eml")
def download_eml(message_id: int):
    """Download EML file for a specific message."""
    try:
        # Get message from database
        message = _db.get_message_by_id(message_id)
        if not message:
            raise HTTPException(status_code=404, detail="Message not found")
        
        eml_path = message.get('eml_file_path')
        if not eml_path or not os.path.exists(eml_path):
            raise HTTPException(status_code=404, detail="EML file not found")
        
        # Generate filename
        subject = message.get('subject', 'No Subject')[:50]  # Limit length
        # Clean filename
        import re
        clean_subject = re.sub(r'[<>:"/\\|?*]', '_', subject)
        filename = f"{clean_subject}_{message_id}.eml"
        
        return FileResponse(
            path=eml_path,
            filename=filename,
            media_type='message/rfc822'
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/message/{message_id}")
def get_message_details(message_id: int):
    """Get detailed message information including text content."""
    try:
        message = _db.get_message_by_id(message_id)
        if not message:
            raise HTTPException(status_code=404, detail="Message not found")
        
        return {
            "success": True,
            "message": message
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/message/{message_id}", response_class=HTMLResponse)
def message_detail_page(request: Request, message_id: int):
    """Show detailed message view page."""
    try:
        message = _db.get_message_by_id(message_id)
        if not message:
            raise HTTPException(status_code=404, detail="Message not found")
        
        return templates.TemplateResponse(
            "message_detail.html",
            {
                "request": request,
                "message": message,
                "message_id": message_id
            }
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
