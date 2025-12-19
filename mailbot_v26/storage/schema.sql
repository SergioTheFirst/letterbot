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
    action_line TEXT,
    body_summary TEXT,
    raw_body_hash TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_emails_account
    ON emails(account_email);

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
