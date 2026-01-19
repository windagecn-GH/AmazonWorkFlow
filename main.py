import os
import uvicorn
from spapi_probe.main import app

if __name__ == "__main__":
    # Allow running locally via `python main.py`
    # Cloud Run populates PORT env var.
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)