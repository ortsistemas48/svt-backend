# app/jobs.py
import uuid
import time
import asyncio

_JOBS = {}  # job_id -> {"status": "pending|running|done|error", "result": dict|None, "error": str|None, "updated_at": float}

def new_job() -> str:
    jid = str(uuid.uuid4())
    _JOBS[jid] = {"status": "pending", "result": None, "error": None, "updated_at": time.time()}
    return jid

def set_status(jid: str, status: str, result=None, error: str | None = None):
    if jid in _JOBS:
        _JOBS[jid].update({"status": status, "result": result, "error": error, "updated_at": time.time()})

def get_job(jid: str):
    return _JOBS.get(jid)

async def run_job(coro, jid: str):
    try:
        set_status(jid, "running")
        result = await coro
        set_status(jid, "done", result=result)
    except Exception as e:
        set_status(jid, "error", error=str(e))
