-- Event aggregates: 5-minute tumbling window counts per repo
CREATE TABLE IF NOT EXISTS github_pulse.event_aggregates
(
    window_start     DateTime,
    window_end       DateTime,
    repo_name        String,
    event_type       String,
    event_count      UInt32,
    inserted_at      DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (window_start, repo_name, event_type);

-- Stale issue alerts: issues with no maintainer response past SLA
CREATE TABLE IF NOT EXISTS github_pulse.stale_issue_alerts
(
    alert_id         String,
    repo_name        String,
    issue_number     UInt32,
    issue_title      String,
    author_login     String,
    created_at       DateTime,
    days_open        UInt32,
    sla_threshold    UInt32,
    detected_at      DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (detected_at, repo_name);
