import os
from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel
from app.engine import execute_sync
from pathlib import Path
from dotenv import load_dotenv

app = FastAPI(title="IBKR to Power BI Sync Engine", version="1.0.0")

# 1. Locate the root directory (one level up from 'app/')
BASE_DIR = Path(__file__).resolve().parent.parent

# 2. Explicitly load the .env file from the root directory
load_dotenv(dotenv_path=BASE_DIR / ".env")

# 3. Fetch SPREADSHEET_URL with your fallback link
SPREADSHEET_URL = os.getenv(
    "SPREADSHEET_URL",
    "https://docs.google.com/spreadsheets/d/1vG7KPOj4vlmxy8aoMeXt-tZfqprLmTexcOGoTdj9sqE/edit",
)

# 4. Point directly to the root directory for service_account.json
CREDENTIALS_PATH = os.getenv("CREDENTIALS_PATH", str(BASE_DIR / "service_account.json"))


@app.get("/")
def health_check():
    return {"status": "online", "message": "IBKR Sync Engine is running"}


@app.post("/api/v1/sync")
def trigger_sync():
    """Synchronous sync endpoint (ideal for Power Automate HTTP action)."""
    try:
        result = execute_sync(
            spreadsheet_url=SPREADSHEET_URL, credentials_path=CREDENTIALS_PATH
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))