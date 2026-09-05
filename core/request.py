"""Compatibility facade for provider payload construction.

The provider-specific implementation lives under ``uni_api.providers``.
Keep this module as a stable import path for existing users and tests,
including legacy imports of private helper names used by the current suite.
"""

from uni_api.providers import payloads as _payloads

for _name in dir(_payloads):
    if _name.startswith("__"):
        continue
    globals()[_name] = getattr(_payloads, _name)

__all__ = tuple(name for name in globals() if not name.startswith("__"))
