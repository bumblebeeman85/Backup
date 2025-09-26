import os
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from . import db as _db

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
