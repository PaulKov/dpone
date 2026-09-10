"""Pinned CHECK catalog producer output, pending the first schema-v2 SQL run.

Empty references deliberately reject catalog admission. Root replaces these
values only through the controlled synthetic DDL round-trip producer, preserving
its original evidence and binding the complete canonical renderer SHA256.
Runtime must never learn expectations from the database it is inspecting.
"""

CHECK_DDL_SHA256: str = ""
CHECK_DEFINITIONS: dict[str, str] = {}
