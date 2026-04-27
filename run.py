"""Run the Geopolitical News web app."""
import logging

from config import DATABASE_PATH
from app.models import init_db

init_db(DATABASE_PATH)

from app import create_app
from app.scheduler import start_macro_scheduler, start_scheduler

# Log scheduler at INFO so refresh runs are visible in console
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

app = create_app()

if __name__ == "__main__":
    # Refresh news and run jobs (topics, impact, analysis, digest, cluster) every 60s
    start_scheduler(interval_seconds=60, first_delay_seconds=15)
    # Macro indicators ingest (World Bank + FX) on a slower cadence
    start_macro_scheduler(interval_seconds=6 * 60 * 60, first_delay_seconds=30)
    # Run the app on port 5003 (change here if needed)
    app.run(debug=True, port=5003)
