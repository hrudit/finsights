CREATE TABLE documents (
    transcript_uuid TEXT PRIMARY KEY,
    company_name    TEXT NOT NULL,
    script_code     TEXT NOT NULL,
    pdf_url         TEXT NOT NULL,
    pdf_url_sha256  TEXT NOT NULL UNIQUE,
    json_text       TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    announcement_date TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    processing_status TEXT NOT NULL 
        DEFAULT 'discovered'
        CHECK (processing_status IN (
            'discovered',
            'downloaded',
            'parsed',
            'failed'
        )),
    pdf_file_name   TEXT DEFAULT NULL,
    pdf_created_at  TEXT DEFAULT NULL,
    text_file_name  TEXT DEFAULT NULL,
    text_file_created_at TEXT DEFAULT NULL,
    insights_file_name TEXT DEFAULT NULL,
    insights_created_at TEXT DEFAULT NULL,
    error_message TEXT DEFAULT NULL
);