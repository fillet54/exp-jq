"""automationv3 plugins package.

Uses ``pkgutil.extend_path`` so plugins can be distributed from separate pip
packages under the shared ``automationv3.plugins`` namespace.
"""

from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)
