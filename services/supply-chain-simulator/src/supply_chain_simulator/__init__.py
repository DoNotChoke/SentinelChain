"""Operational supply-chain simulator (PLAN §7.6, Milestone 1).

Writes and mutates operational data in PostgreSQL so Debezium can capture the changes as CDC
events. See ``README.md``.
"""

from __future__ import annotations

__version__ = "0.1.0"
