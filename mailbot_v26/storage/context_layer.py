from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from mailbot_v26.observability import get_logger

logger = logging.getLogger(__name__)
observability_logger = get_logger("mailbot")


def normalize_name(value: str) -> str:
    return "".join(ch for ch in (value or "").lower() if ch.isalnum())


def _to_timestamp(value: datetime | None) -> str:
    if value is None:
        return datetime.utcnow().isoformat()
    return value.isoformat()


@dataclass(frozen=True)
class EntityResolution:
    entity_id: str
    entity_type: str
    confidence: float


@dataclass(frozen=True)
class EntityRelationship:
    entity_from: str
    entity_to: str
    confidence: float
    heuristics: tuple[str, ...]
    name_similarity: float | None = None
    same_domain: bool | None = None
    email_match: bool | None = None


def _normalize_email(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = value.strip().lower()
    return cleaned or None


def _extract_domain(email: str | None) -> str | None:
    if not email or "@" not in email:
        return None
    domain = email.rsplit("@", 1)[-1].strip().lower()
    return domain or None


def _extract_metadata_email(metadata: str | None) -> str | None:
    if not metadata:
        return None
    try:
        payload = json.loads(metadata)
    except json.JSONDecodeError:
        return None
    email = payload.get("from_email")
    return _normalize_email(email)


class EntityResolver:
    def __init__(self, *, recent_window_days: int = 30, name_threshold: float = 0.84) -> None:
        self.recent_window_days = recent_window_days
        self.name_threshold = name_threshold

    def resolve(
        self,
        *,
        conn: sqlite3.Connection,
        entity_id: str,
        from_email: str | None,
        from_name: str | None,
        event_time: datetime | None,
    ) -> list[EntityRelationship]:
        if not entity_id:
            return []

        normalized_current = normalize_name((from_name or "").strip() or (from_email or "").strip())
        if not normalized_current:
            return []

        event_dt = event_time or datetime.utcnow()
        window_start = (event_dt - timedelta(days=self.recent_window_days)).isoformat()
        email_norm = _normalize_email(from_email)
        domain_norm = _extract_domain(email_norm)

        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, normalized_name, name, metadata, last_seen
            FROM entities
            WHERE id != ?
              AND last_seen >= ?;
            """,
            (entity_id, window_start),
        ).fetchall()

        relationships: list[EntityRelationship] = []
        for row in rows:
            candidate_id = str(row["id"])
            candidate_norm = str(row["normalized_name"] or "") or normalize_name(str(row["name"] or ""))
            if not candidate_norm:
                continue
            candidate_email = _extract_metadata_email(row["metadata"])
            candidate_domain = _extract_domain(candidate_email)
            email_match = bool(email_norm and candidate_email and email_norm == candidate_email)
            same_domain = bool(domain_norm and candidate_domain and domain_norm == candidate_domain)
            name_similarity = SequenceMatcher(None, normalized_current, candidate_norm).ratio()

            heuristics: list[str] = ["recent_window"]
            confidence: float | None = None

            if email_match:
                heuristics.append("email_exact")
                confidence = 1.0
            else:
                if not same_domain:
                    continue
                if name_similarity < self.name_threshold:
                    continue
                heuristics.extend(["same_domain", "name_similarity"])
                confidence = round(min(0.95, (0.7 * name_similarity) + 0.3), 3)

            if same_domain and "same_domain" not in heuristics:
                heuristics.append("same_domain")

            if name_similarity >= self.name_threshold and "name_similarity" not in heuristics:
                heuristics.append("name_similarity")

            relationship = EntityRelationship(
                entity_from=entity_id,
                entity_to=candidate_id,
                confidence=confidence,
                heuristics=tuple(sorted(set(heuristics))),
                name_similarity=round(name_similarity, 3),
                same_domain=same_domain,
                email_match=email_match,
            )
            relationships.append(relationship)

        return relationships


class ContextStore:
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path)

    def _ensure_schema(self) -> None:
        schema = """
        CREATE TABLE IF NOT EXISTS entities (
            id TEXT PRIMARY KEY,
            type TEXT NOT NULL,
            name TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            first_seen DATETIME,
            last_seen DATETIME,
            metadata JSON
        );

        CREATE INDEX IF NOT EXISTS idx_entities_norm_name
            ON entities(normalized_name);

        CREATE TABLE IF NOT EXISTS relationships (
            id TEXT PRIMARY KEY,
            entity_from TEXT NOT NULL,
            entity_to TEXT NOT NULL,
            type TEXT NOT NULL,
            strength REAL DEFAULT 1.0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS entity_baselines (
            entity_id TEXT NOT NULL,
            metric TEXT NOT NULL,
            baseline_value REAL,
            sample_size INTEGER,
            computed_at DATETIME,
            PRIMARY KEY (entity_id, metric)
        );

        CREATE TABLE IF NOT EXISTS interaction_events (
            id TEXT PRIMARY KEY,
            entity_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            event_time DATETIME,
            metadata JSON,
            deviation REAL,
            is_anomaly BOOLEAN
        );
        """
        with self._connect() as conn:
            conn.executescript(schema)

    def resolve_sender_entity(
        self,
        *,
        from_email: str | None,
        from_name: str | None,
        entity_type: str = "person",
        event_time: datetime | None = None,
    ) -> EntityResolution | None:
        display_name = (from_name or "").strip() or (from_email or "").strip()
        if not display_name:
            return None

        normalized = normalize_name(display_name)
        if not normalized:
            return None

        timestamp = _to_timestamp(event_time)
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT id, type FROM entities WHERE normalized_name = ? LIMIT 1;",
                (normalized,),
            ).fetchone()
            if row:
                conn.execute(
                    "UPDATE entities SET last_seen = ? WHERE id = ?;",
                    (timestamp, row["id"]),
                )
                return EntityResolution(
                    entity_id=str(row["id"]),
                    entity_type=str(row["type"] or entity_type),
                    confidence=1.0,
                )

            rows = conn.execute(
                "SELECT id, normalized_name, type FROM entities;",
            ).fetchall()
            best_row: sqlite3.Row | None = None
            best_ratio = 0.0
            for candidate in rows:
                candidate_norm = str(candidate["normalized_name"] or "")
                if not candidate_norm:
                    continue
                ratio = SequenceMatcher(None, normalized, candidate_norm).ratio()
                if ratio >= 0.9 and ratio > best_ratio:
                    best_ratio = ratio
                    best_row = candidate

            if best_row is not None:
                conn.execute(
                    "UPDATE entities SET last_seen = ? WHERE id = ?;",
                    (timestamp, best_row["id"]),
                )
                return EntityResolution(
                    entity_id=str(best_row["id"]),
                    entity_type=str(best_row["type"] or entity_type),
                    confidence=best_ratio,
                )

            entity_id = str(uuid.uuid4())
            metadata = {
                "from_email": (from_email or "").strip() or None,
                "from_name": (from_name or "").strip() or None,
            }
            conn.execute(
                """
                INSERT INTO entities (
                    id, type, name, normalized_name, first_seen, last_seen, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    entity_id,
                    entity_type,
                    display_name,
                    normalized,
                    timestamp,
                    timestamp,
                    json.dumps(metadata, ensure_ascii=False),
                ),
            )
            return EntityResolution(
                entity_id=entity_id,
                entity_type=entity_type,
                confidence=1.0,
            )

    def resolve_entity_relationships(
        self,
        *,
        entity_id: str,
        from_email: str | None,
        from_name: str | None,
        event_time: datetime | None = None,
    ) -> list[EntityRelationship]:
        if not entity_id:
            return []

        resolver = EntityResolver()
        created: list[EntityRelationship] = []
        with self._connect() as conn:
            relationships = resolver.resolve(
                conn=conn,
                entity_id=entity_id,
                from_email=from_email,
                from_name=from_name,
                event_time=event_time,
            )
            for relation in relationships:
                entity_from, entity_to = sorted([relation.entity_from, relation.entity_to])
                exists = conn.execute(
                    """
                    SELECT 1 FROM relationships
                    WHERE entity_from = ? AND entity_to = ? AND type = 'entity_resolution'
                    LIMIT 1;
                    """,
                    (entity_from, entity_to),
                ).fetchone()
                if exists:
                    continue
                conn.execute(
                    """
                    INSERT INTO relationships (
                        id, entity_from, entity_to, type, strength, created_at
                    ) VALUES (?, ?, ?, 'entity_resolution', ?, ?);
                    """,
                    (
                        str(uuid.uuid4()),
                        entity_from,
                        entity_to,
                        relation.confidence,
                        _to_timestamp(event_time),
                    ),
                )
                created.append(relation)
                observability_logger.info(
                    "entity_resolution",
                    entity_from=entity_from,
                    entity_to=entity_to,
                    confidence=relation.confidence,
                    heuristics=list(relation.heuristics),
                    name_similarity=relation.name_similarity,
                    same_domain=relation.same_domain,
                    email_match=relation.email_match,
                )
        return created

    def record_interaction_event(
        self,
        *,
        entity_id: str,
        event_type: str,
        event_time: datetime | None,
        metadata: dict[str, Any] | None = None,
    ) -> tuple[float | None, bool | None]:
        if not entity_id:
            return None, None

        event_dt = event_time or datetime.utcnow()
        event_timestamp = event_dt.isoformat()
        window_start = (event_dt - timedelta(days=30)).isoformat()
        deviation: float | None = None
        is_anomaly: bool | None = None

        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            baseline_row = conn.execute(
                """
                SELECT baseline_value
                FROM entity_baselines
                WHERE entity_id = ? AND metric = 'email_frequency';
                """,
                (entity_id,),
            ).fetchone()
            baseline_value = (
                float(baseline_row["baseline_value"])
                if baseline_row and baseline_row["baseline_value"] is not None
                else None
            )

            count_row = conn.execute(
                """
                SELECT COUNT(*) AS total
                FROM interaction_events
                WHERE entity_id = ?
                  AND event_type = ?
                  AND event_time >= ?;
                """,
                (entity_id, event_type, window_start),
            ).fetchone()
            recent_count = int(count_row["total"] if count_row else 0)
            current_frequency = (recent_count + 1) / 30.0

            if baseline_value is not None:
                deviation = abs(current_frequency - baseline_value)
                is_anomaly = deviation > (baseline_value * 2)

            conn.execute(
                """
                INSERT INTO interaction_events (
                    id, entity_id, event_type, event_time, metadata, deviation, is_anomaly
                ) VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    str(uuid.uuid4()),
                    entity_id,
                    event_type,
                    event_timestamp,
                    json.dumps(metadata or {}, ensure_ascii=False),
                    deviation,
                    is_anomaly,
                ),
            )

        return deviation, is_anomaly

    def latest_interaction_event_time(
        self,
        *,
        entity_id: str,
        event_type: str,
    ) -> datetime | None:
        if not entity_id:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT event_time
                FROM interaction_events
                WHERE entity_id = ?
                  AND event_type = ?
                ORDER BY event_time DESC
                LIMIT 1
                """,
                (entity_id, event_type),
            ).fetchone()
        if not row:
            return None
        value = row[0]
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value))
        except ValueError:
            return None

    def recompute_email_frequency(
        self,
        *,
        entity_id: str,
        now: datetime | None = None,
    ) -> tuple[float, int]:
        if not entity_id:
            return 0.0, 0

        current_time = now or datetime.utcnow()
        window_start = (current_time - timedelta(days=30)).isoformat()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS total
                FROM interaction_events
                WHERE entity_id = ?
                  AND event_type = 'email_received'
                  AND event_time >= ?;
                """,
                (entity_id, window_start),
            ).fetchone()
            total = int(row[0] if row else 0)
            baseline_value = total / 30.0
            conn.execute(
                """
                INSERT INTO entity_baselines (
                    entity_id, metric, baseline_value, sample_size, computed_at
                ) VALUES (?, 'email_frequency', ?, ?, ?)
                ON CONFLICT(entity_id, metric) DO UPDATE SET
                    baseline_value = excluded.baseline_value,
                    sample_size = excluded.sample_size,
                    computed_at = excluded.computed_at;
                """,
                (
                    entity_id,
                    baseline_value,
                    total,
                    current_time.isoformat(),
                ),
            )
        return baseline_value, total
