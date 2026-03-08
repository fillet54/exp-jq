"""Frontend routes blueprints."""

from .queue import bp as queue_bp
from .reports import bp as reports_bp
from .scripts import bp as scripts_bp
from .system import bp as system_bp

__all__ = ["queue_bp", "reports_bp", "scripts_bp", "system_bp"]
