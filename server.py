from app.main import app
from app.config import PORT, WORKERS, logger

if __name__ == '__main__':
    logger.info(f"Starting Brihas ERP Server on port {PORT} with {WORKERS} worker(s)...")
    app.run(host="0.0.0.0", port=PORT, workers=WORKERS)