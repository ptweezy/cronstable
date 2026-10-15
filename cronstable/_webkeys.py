"""The settings one build of the daemon's web app serves, kept on the app.

``cron.py`` imports this module lazily, since importing
:mod:`cronstable.cron` loads no aiohttp.  The key is built here, at
import, because aiohttp's ``AppKey`` constructor needs a module frame on
the stack, and a thread that runs the event loop, such as the Windows
service's, has none.
"""

from collections.abc import Mapping
from typing import Any, NamedTuple

from aiohttp import web

from cronstable.config import resolve_badges_config


class WebAppSettings(NamedTuple):
    """What one app build routes, read back by its handlers."""

    #: resolve_badges_config's answer: ``None`` while the badge routes
    #: are off
    badges: dict[str, Any] | None
    #: whether the dashboard page is served (web.ui)
    ui: bool

    @classmethod
    def resolve(cls, web_config: Mapping[str, Any]) -> "WebAppSettings":
        return cls(
            badges=resolve_badges_config(web_config),
            ui=bool(web_config.get("ui", True)),
        )


WEB_APP_SETTINGS = web.AppKey("WEB_APP_SETTINGS", WebAppSettings)
