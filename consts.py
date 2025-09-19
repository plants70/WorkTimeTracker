from __future__ import annotations

import re
from typing import Optional

STATUS_ACTIVE: str = "В работе"
STATUS_LOGOUT: str = "LOGOUT"
STATUS_FORCE_LOGOUT: str = "FORCE_LOGOUT"


def _canonical_key(value: str) -> str:
    """Normalize status strings for alias lookup."""
    return re.sub(r"[\s_-]+", " ", value.strip().lower())


_ALIAS_MAP: dict[str, str] = {
    _canonical_key("active"): STATUS_ACTIVE,
    _canonical_key("в работе"): STATUS_ACTIVE,
    _canonical_key("finished"): STATUS_LOGOUT,
    _canonical_key("logout"): STATUS_LOGOUT,
    _canonical_key("logoff"): STATUS_LOGOUT,
    _canonical_key("force_logout"): STATUS_FORCE_LOGOUT,
    _canonical_key("force logout"): STATUS_FORCE_LOGOUT,
    _canonical_key("kicked"): STATUS_FORCE_LOGOUT,
}


def normalize_session_status(raw: Optional[str]) -> Optional[str]:
    """Return canonical session status value for comparisons."""
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text in {STATUS_ACTIVE, STATUS_LOGOUT, STATUS_FORCE_LOGOUT}:
        return text
    upper = text.upper()
    if upper == STATUS_FORCE_LOGOUT:
        return STATUS_FORCE_LOGOUT
    if upper == STATUS_LOGOUT:
        return STATUS_LOGOUT
    key = _canonical_key(text)
    if key in _ALIAS_MAP:
        return _ALIAS_MAP[key]
    if "force logout" in key or "kicked" in key:
        return STATUS_FORCE_LOGOUT
    if "finished" in key or "logout" in key:
        return STATUS_LOGOUT
    if "active" in key or "в работе" in key:
        return STATUS_ACTIVE
    return text


__all__ = [
    "STATUS_ACTIVE",
    "STATUS_LOGOUT",
    "STATUS_FORCE_LOGOUT",
    "normalize_session_status",
]
