from nicegui import ui

from webapp.config import DEFAULT_DB_PATH, DEFAULT_HOST, DEFAULT_PORT
from webapp.db import ReadOnlyDatabase
from webapp.services import StatsService
from webapp.ui import PersonalStatsApp


def create_app() -> PersonalStatsApp:
    """Create a fresh app instance for a single page/client."""
    db = ReadOnlyDatabase(DEFAULT_DB_PATH)
    service = StatsService(db)
    return PersonalStatsApp(service)


@ui.page("/", reconnect_timeout=30)
def index_page() -> None:
    """Build the UI inside an explicit page to avoid shared auto-index state."""
    create_app().build()


if __name__ in {"__main__", "__mp_main__"}:
    ui.run(host=DEFAULT_HOST, port=DEFAULT_PORT, title="Personal Stats", reload=False)
