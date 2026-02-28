"""Route registration modules for frontend endpoints."""

from .queue import register_queue_routes
from .reports import register_report_routes
from .scripts import register_script_routes
from .system import register_system_routes

__all__ = [
    "register_queue_routes",
    "register_report_routes",
    "register_script_routes",
    "register_system_routes",
]
