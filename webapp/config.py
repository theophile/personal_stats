from pathlib import Path
import os


DEFAULT_DB_PATH = Path(os.environ.get("PERSONAL_STATS_DB_PATH", "ascdatabase2.db"))
DEFAULT_HOST = os.environ.get("PERSONAL_STATS_HOST", "0.0.0.0")
DEFAULT_PORT = int(os.environ.get("PERSONAL_STATS_PORT", "8080"))
