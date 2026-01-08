PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS emails (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_email TEXT NOT NULL,
    from_email TEXT,
    subject TEXT,
    received_at TEXT,
    priority TEXT,
    original_priority TEXT,
    priority_reason TEXT,
    shadow_priority TEXT,
    shadow_priority_reason TEXT,
    shadow_action_line TEXT,
    shadow_action_reason TEXT,
    confidence_score REAL,
    confidence_decision TEXT,
    proposed_action_type TEXT,
    proposed_action_text TEXT,
    proposed_action_confidence REAL,
    llm_provider TEXT,
    deferred_for_digest INTEGER DEFAULT 0,
    rfc_message_id TEXT,
    in_reply_to TEXT,
    "references" TEXT,
    thread_key TEXT,
    action_line TEXT,
    body_summary TEXT,
    raw_body_hash TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS digest_state (
    account_email TEXT PRIMARY KEY,
    last_digest_sent_at TEXT
);

CREATE TABLE IF NOT EXISTS weekly_digest_state (
    account_email TEXT PRIMARY KEY,
    last_week_key TEXT,
    last_sent_at TEXT
);

CREATE TABLE IF NOT EXISTS telegram_inbound_state (
    state_key TEXT PRIMARY KEY,
    last_update_id INTEGER,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS runtime_overrides (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_emails_account
    ON emails(account_email);

CREATE INDEX IF NOT EXISTS idx_emails_account_received_at
    ON emails(account_email, received_at);

CREATE INDEX IF NOT EXISTS idx_emails_received_at_id
    ON emails(received_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_emails_account_received_at_id
    ON emails(account_email, received_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_emails_from
    ON emails(from_email);

CREATE INDEX IF NOT EXISTS idx_emails_priority
    ON emails(priority);

CREATE TABLE IF NOT EXISTS attachments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email_id INTEGER NOT NULL,
    filename TEXT,
    summary TEXT,
    FOREIGN KEY(email_id) REFERENCES emails(id)
);

CREATE TABLE IF NOT EXISTS preview_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email_id INTEGER NOT NULL,
    proposed_action TEXT,
    confidence REAL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS action_feedback (
    id TEXT PRIMARY KEY,
    email_id TEXT,
    proposed_action TEXT,
    decision TEXT,
    user_note TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS priority_feedback (
    id TEXT PRIMARY KEY,
    email_id TEXT,
    kind TEXT,
    value TEXT,
    entity_id TEXT,
    sender_email TEXT,
    account_email TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_priority_feedback_email_kind_value
    ON priority_feedback(email_id, kind, value);

CREATE TABLE IF NOT EXISTS decision_traces (
    id TEXT PRIMARY KEY,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    email_id TEXT,
    account_email TEXT,
    signal_entropy REAL,
    signal_printable_ratio REAL,
    signal_quality_score REAL,
    signal_fallback_used BOOLEAN,
    llm_provider TEXT,
    llm_model TEXT,
    prompt_full TEXT,
    response_full TEXT,
    priority TEXT,
    action_line TEXT,
    confidence REAL,
    shadow_priority TEXT,
    compressed BOOLEAN DEFAULT FALSE
);

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

CREATE TABLE IF NOT EXISTS entity_signals (
    entity_id TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    score INTEGER,
    label TEXT,
    computed_at DATETIME,
    sample_size INTEGER,
    PRIMARY KEY (entity_id, signal_type)
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

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    entity_id TEXT,
    email_id TEXT,
    payload JSON
);

CREATE TABLE IF NOT EXISTS events_v1 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    ts_utc REAL NOT NULL,
    ts TEXT,
    account_id TEXT NOT NULL,
    entity_id TEXT,
    email_id INTEGER,
    payload JSON,
    payload_json JSON,
    schema_version INTEGER NOT NULL,
    fingerprint TEXT NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_events_v1_event_type_ts
    ON events_v1(event_type, ts_utc);

CREATE INDEX IF NOT EXISTS idx_events_v1_account_ts
    ON events_v1(account_id, ts_utc);

CREATE INDEX IF NOT EXISTS idx_events_v1_account_ts_id
    ON events_v1(account_id, ts_utc DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_events_v1_entity_ts
    ON events_v1(entity_id, ts_utc);

CREATE INDEX IF NOT EXISTS idx_events_v1_email_id
    ON events_v1(email_id);

CREATE INDEX IF NOT EXISTS idx_events_v1_email_event_ts
    ON events_v1(email_id, event_type, ts_utc DESC);

CREATE INDEX IF NOT EXISTS idx_events_v1_email_ts_id
    ON events_v1(email_id, ts_utc DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_events_v1_event_type_ts_iso
    ON events_v1(event_type, ts);

CREATE INDEX IF NOT EXISTS idx_events_v1_account_ts_iso
    ON events_v1(account_id, ts);

CREATE INDEX IF NOT EXISTS idx_events_v1_entity_ts_iso
    ON events_v1(entity_id, ts);

CREATE TABLE IF NOT EXISTS auto_priority_gate_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_disabled_at_utc REAL,
    last_disabled_reason TEXT,
    last_eval_at_utc REAL
);

CREATE TABLE IF NOT EXISTS events_backfill_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    status TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS commitments (
    id INTEGER PRIMARY KEY,
    email_row_id INTEGER NOT NULL,
    source TEXT NOT NULL,
    commitment_text TEXT NOT NULL,
    deadline_iso TEXT,
    status TEXT NOT NULL,
    confidence REAL NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(email_row_id) REFERENCES emails(id)
);

CREATE INDEX IF NOT EXISTS idx_commitments_email_row_id
    ON commitments(email_row_id);

CREATE INDEX IF NOT EXISTS idx_commitments_status
    ON commitments(status);

CREATE INDEX IF NOT EXISTS idx_commitments_deadline_iso
    ON commitments(deadline_iso);

CREATE INDEX IF NOT EXISTS idx_commitments_email_status_created_id
    ON commitments(email_row_id, status, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS processing_spans (
    span_id TEXT PRIMARY KEY,
    ts_start_utc REAL NOT NULL,
    ts_end_utc REAL NOT NULL,
    total_duration_ms INTEGER NOT NULL,
    account_id TEXT NOT NULL,
    email_id INTEGER,
    stage_durations_json TEXT NOT NULL DEFAULT '{}',
    llm_provider TEXT,
    llm_model TEXT,
    llm_latency_ms INTEGER,
    llm_quality_score REAL,
    fallback_used INTEGER NOT NULL DEFAULT 0,
    outcome TEXT NOT NULL,
    error_code TEXT NOT NULL DEFAULT '',
    health_snapshot_id TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS system_health_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    ts_utc REAL NOT NULL,
    payload_json TEXT NOT NULL,
    gates_state TEXT,
    metrics_brief TEXT,
    system_mode TEXT
);

CREATE INDEX IF NOT EXISTS idx_processing_spans_account_ts
    ON processing_spans(account_id, ts_start_utc);

CREATE INDEX IF NOT EXISTS idx_processing_spans_email_id
    ON processing_spans(email_id);

CREATE INDEX IF NOT EXISTS idx_processing_spans_email_ts
    ON processing_spans(email_id, ts_start_utc, span_id);

CREATE INDEX IF NOT EXISTS idx_processing_spans_ts_start
    ON processing_spans(ts_start_utc);

CREATE INDEX IF NOT EXISTS idx_system_health_snapshots_ts
    ON system_health_snapshots(ts_utc);

CREATE INDEX IF NOT EXISTS idx_system_health_snapshots_ts_snapshot
    ON system_health_snapshots(ts_utc DESC, snapshot_id);
