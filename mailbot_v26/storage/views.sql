CREATE VIEW IF NOT EXISTS v_sender_stats AS
SELECT
    LOWER(TRIM(COALESCE(from_email, ''))) AS sender_email,
    COUNT(*) AS emails_total,
    COUNT(DISTINCT account_email) AS account_count,
    SUM(CASE WHEN priority = '🔴' THEN 1 ELSE 0 END) AS red_count,
    SUM(CASE WHEN priority = '🟡' THEN 1 ELSE 0 END) AS yellow_count,
    SUM(CASE WHEN priority = '🔵' THEN 1 ELSE 0 END) AS blue_count,
    SUM(CASE WHEN TRIM(COALESCE(priority_reason, '')) != '' THEN 1 ELSE 0 END) AS escalations,
    MIN(received_at) AS first_received_at,
    MAX(received_at) AS last_received_at
FROM emails
WHERE TRIM(COALESCE(from_email, '')) != ''
GROUP BY LOWER(TRIM(COALESCE(from_email, '')))
HAVING COUNT(*) > 0;

CREATE VIEW IF NOT EXISTS v_account_stats AS
SELECT
    account_email,
    COUNT(*) AS emails_total,
    COUNT(DISTINCT LOWER(TRIM(COALESCE(from_email, '')))) AS sender_count,
    SUM(CASE WHEN priority = '🔴' THEN 1 ELSE 0 END) AS red_count,
    SUM(CASE WHEN priority = '🟡' THEN 1 ELSE 0 END) AS yellow_count,
    SUM(CASE WHEN priority = '🔵' THEN 1 ELSE 0 END) AS blue_count,
    SUM(CASE WHEN TRIM(COALESCE(priority_reason, '')) != '' THEN 1 ELSE 0 END) AS escalations,
    MIN(received_at) AS first_received_at,
    MAX(received_at) AS last_received_at
FROM emails
GROUP BY account_email
HAVING COUNT(*) > 0;

CREATE VIEW IF NOT EXISTS v_priority_escalations AS
SELECT
    id AS email_id,
    account_email,
    from_email,
    subject,
    received_at,
    priority,
    priority_reason,
    created_at
FROM emails
WHERE TRIM(COALESCE(priority_reason, '')) != '';
