from fastapi import FastAPI, HTTPException, Request, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel
from typing import Optional
import uuid, time, asyncio, os, smtplib, shutil
from datetime import datetime
from collections import deque
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

try:
    import qrcode
    QR_AVAILABLE = True
except ImportError:
    QR_AVAILABLE = False

app = FastAPI(title="SmartQueue API", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

UPLOAD_DIR = "/app/uploads"
OUTPUT_DIR = "/app/outputs"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

jobs_db: dict = {}
queue: deque = deque()
workers_status = {
    "worker-1": {"status": "idle", "current_job": None, "processed": 0},
    "worker-2": {"status": "idle", "current_job": None, "processed": 0},
    "worker-3": {"status": "idle", "current_job": None, "processed": 0},
}
rate_limit_store: dict = {}
stats = {"total_submitted": 0, "total_completed": 0, "total_failed": 0}

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")

JOB_TYPES = {
    "email": {"label": "Send Email"},
    "image_resize": {"label": "Resize Image"},
    "qrcode": {"label": "Generate QR Code"},
}

class JobSubmit(BaseModel):
    job_type: str
    payload: dict = {}
    priority: int = 1

def check_rate_limit(client_ip: str, limit: int = 10) -> bool:
    now = time.time()
    if client_ip not in rate_limit_store:
        rate_limit_store[client_ip] = []
    rate_limit_store[client_ip] = [t for t in rate_limit_store[client_ip] if now - t < 60]
    if len(rate_limit_store[client_ip]) >= limit:
        return False
    rate_limit_store[client_ip].append(now)
    return True

async def handle_email(job: dict) -> str:
    payload = job.get("payload", {})
    to_addr = payload.get("to", "")
    subject = payload.get("subject", "Message from SmartQueue")
    body = payload.get("body", "Hello from SmartQueue!")
    
    if not to_addr:
        raise ValueError("Missing 'to' email address in payload")
    if not SMTP_USER or not SMTP_PASSWORD:
        raise ValueError("SMTP credentials not configured. Set SMTP_USER and SMTP_PASSWORD env vars.")
    
    msg = MIMEMultipart()
    msg["From"] = SMTP_USER
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))
    
    loop = asyncio.get_event_loop()
    def send():
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, to_addr, msg.as_string())
    await loop.run_in_executor(None, send)
    return f"Email sent to {to_addr}"

async def handle_image_resize(job: dict) -> str:
    if not PIL_AVAILABLE:
        raise ValueError("Pillow not installed")
    payload = job.get("payload", {})
    filename = payload.get("filename", "")
    width = int(payload.get("width", 800))
    height = int(payload.get("height", 600))
    input_path = os.path.join(UPLOAD_DIR, filename)
    if not filename or not os.path.exists(input_path):
        raise ValueError(f"File '{filename}' not found. Upload it first via /api/upload")
    name, ext = os.path.splitext(filename)
    out_name = f"{name}_resized_{width}x{height}{ext}"
    out_path = os.path.join(OUTPUT_DIR, out_name)
    loop = asyncio.get_event_loop()
    def resize():
        with Image.open(input_path) as img:
            if payload.get("keep_aspect"):
                img.thumbnail((width, height), Image.LANCZOS)
            else:
                img = img.resize((width, height), Image.LANCZOS)
            img.save(out_path)
    await loop.run_in_executor(None, resize)
    return f"/api/download/{out_name}"

async def handle_qrcode(job: dict) -> str:
    if not QR_AVAILABLE:
        raise ValueError("qrcode not installed")
    payload = job.get("payload", {})
    data = payload.get("data", "https://github.com/yourusername/smartqueue")
    
    job_id = job.get("id")
    filename = f"qrcode_{job_id}.png"
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    loop = asyncio.get_event_loop()
    def generate():
        img = qrcode.make(data)
        img.save(filepath, "PNG")
    
    await loop.run_in_executor(None, generate)
    return f"/api/download/{filename}"

async def process_jobs():
    while True:
        await asyncio.sleep(1)
        for worker_id, worker in workers_status.items():
            if worker["status"] == "idle" and queue:
                sorted_q = sorted(list(queue), key=lambda jid: -jobs_db[jid]["priority"])
                job_id = sorted_q[0]
                queue.remove(job_id)
                job = jobs_db[job_id]
                job["status"] = "running"
                job["started_at"] = time.time()
                job["worker"] = worker_id
                worker["status"] = "busy"
                worker["current_job"] = job_id
                asyncio.create_task(run_job(job_id, worker_id))

async def run_job(job_id: str, worker_id: str):
    job = jobs_db[job_id]
    job["progress"] = 10
    try:
        jtype = job["job_type"]
        job["progress"] = 30
        if jtype == "email":
            result = await handle_email(job)
        elif jtype == "image_resize":
            result = await handle_image_resize(job)
        elif jtype == "qrcode":
            result = await handle_qrcode(job)
        else:
            raise ValueError(f"Unknown job type: {jtype}")
        job["status"] = "completed"
        job["result"] = result
        job["progress"] = 100
        stats["total_completed"] += 1
    except Exception as e:
        job["status"] = "failed"
        job["result"] = str(e)
        job["progress"] = 100
        stats["total_failed"] += 1
    job["completed_at"] = time.time()
    workers_status[worker_id]["status"] = "idle"
    workers_status[worker_id]["current_job"] = None
    workers_status[worker_id]["processed"] += 1

@app.on_event("startup")
async def startup():
    asyncio.create_task(process_jobs())

@app.get("/", response_class=HTMLResponse)
async def root():
    with open("/app/frontend/index.html") as f:
        return f.read()

@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    dest = os.path.join(UPLOAD_DIR, file.filename)
    with open(dest, "wb") as f:
        shutil.copyfileobj(file.file, f)
    return {"filename": file.filename, "message": "Uploaded successfully"}

@app.get("/api/download/{filename}")
async def download_file(filename: str):
    path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path, filename=filename)

@app.post("/api/jobs")
async def submit_job(job_in: JobSubmit, request: Request):
    client_ip = request.client.host
    if not check_rate_limit(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded. Max 10 jobs/minute.")
    if job_in.job_type not in JOB_TYPES:
        raise HTTPException(status_code=400, detail=f"Valid types: {list(JOB_TYPES.keys())}")
    job_id = str(uuid.uuid4())[:8].upper()
    job = {"id": job_id, "job_type": job_in.job_type, "payload": job_in.payload,
           "priority": max(1, min(3, job_in.priority)), "status": "pending",
           "created_at": time.time(), "started_at": None, "completed_at": None,
           "result": None, "worker": None, "progress": 0}
    jobs_db[job_id] = job
    queue.append(job_id)
    stats["total_submitted"] += 1
    return {"job_id": job_id, "status": "pending", "message": "Job queued"}

@app.get("/api/jobs")
async def list_jobs(status: Optional[str] = None, limit: int = 50):
    jobs = list(jobs_db.values())
    if status:
        jobs = [j for j in jobs if j["status"] == status]
    jobs.sort(key=lambda j: -j["created_at"])
    return jobs[:limit]

@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    job = jobs_db.get(job_id.upper())
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@app.delete("/api/jobs/{job_id}")
async def cancel_job(job_id: str):
    job = jobs_db.get(job_id.upper())
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] != "pending":
        raise HTTPException(status_code=400, detail="Can only cancel pending jobs")
    if job_id.upper() in queue:
        queue.remove(job_id.upper())
    job["status"] = "failed"
    job["result"] = "Cancelled by user"
    stats["total_failed"] += 1
    return {"message": "Job cancelled"}

@app.get("/api/workers")
async def get_workers():
    return workers_status

@app.get("/api/stats")
async def get_stats():
    return {**stats,
            "pending": sum(1 for j in jobs_db.values() if j["status"] == "pending"),
            "running": sum(1 for j in jobs_db.values() if j["status"] == "running"),
            "queue_depth": len(queue), "worker_count": len(workers_status),
            "job_types": {k: v["label"] for k, v in JOB_TYPES.items()}}

@app.get("/api/health")
async def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat(),
            "pillow": PIL_AVAILABLE, "qrcode": QR_AVAILABLE,
            "smtp_configured": bool(SMTP_USER and SMTP_PASSWORD)}