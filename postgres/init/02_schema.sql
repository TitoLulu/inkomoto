\c analytics_db

CREATE TABLE IF NOT EXISTS loans (
    project_id              VARCHAR(20) PRIMARY KEY,
    project_name            VARCHAR(500),
    country                 VARCHAR(200),
    country_code            CHAR(2),
    region                  VARCHAR(100),
    status                  VARCHAR(50),
    lending_instrument      VARCHAR(150),
    total_commitment_usd    DECIMAL(20, 2),
    ibrd_commitment_usd     DECIMAL(20, 2),
    ida_commitment_usd      DECIMAL(20, 2),
    total_project_cost_usd  DECIMAL(20, 2),
    board_approval_date     DATE,
    closing_date            DATE,
    approval_fy             SMALLINT,
    source                  VARCHAR(20),
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE PUBLICATION dbz_publication FOR TABLE loans;
