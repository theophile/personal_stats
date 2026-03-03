from nicegui import ui

from webapp.config import DEFAULT_DB_PATH, DEFAULT_HOST, DEFAULT_PORT
from webapp.db import ReadOnlyDatabase
from webapp.services import StatsService
from webapp.ui import PersonalStatsApp


def create_app() -> PersonalStatsApp:
    db = ReadOnlyDatabase(DEFAULT_DB_PATH)
    service = StatsService(db)
    app = PersonalStatsApp(service)
    app.build()
    return app


if __name__ in {"__main__", "__mp_main__"}:
    create_app()
    ui.run(host=DEFAULT_HOST, port=DEFAULT_PORT, title="Personal Stats", reload=False)
