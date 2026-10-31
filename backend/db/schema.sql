-- UWB Indoor Positioning — Database Schema
-- Requires TimescaleDB extension

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============ HARDWARE TABLES ============

CREATE TABLE anchors (
    anchor_id   SERIAL PRIMARY KEY,
    label       VARCHAR(32) NOT NULL,
    hardware_id TEXT UNIQUE,
    is_active   BOOLEAN DEFAULT true,
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE anchor_calibrations (
    calibration_id  SERIAL PRIMARY KEY,
    anchor_id       INT NOT NULL REFERENCES anchors(anchor_id),
    x_m             DOUBLE PRECISION NOT NULL,
    y_m             DOUBLE PRECISION NOT NULL,
    z_m             DOUBLE PRECISION NOT NULL,
    calibrated_at   TIMESTAMPTZ DEFAULT now(),
    calibrated_by   VARCHAR(32),
    method          VARCHAR(32),
    notes           TEXT,
    is_active       BOOLEAN DEFAULT true
);

CREATE TABLE tags (
    tag_id      SERIAL PRIMARY KEY,
    label       VARCHAR(64),
    hardware_id TEXT UNIQUE,
    source      VARCHAR(16) NOT NULL DEFAULT 'real',
    is_active   BOOLEAN DEFAULT true,
    created_at  TIMESTAMPTZ DEFAULT now()
);

-- ============ CONFIG TABLES ============

CREATE TABLE calc_config (
    calc_config_id      SERIAL PRIMARY KEY,
    name                VARCHAR(32) NOT NULL,
    window_size_ms      INT NOT NULL DEFAULT 2000,
    min_anchors         SMALLINT NOT NULL DEFAULT 3,
    max_range_m         DOUBLE PRECISION NOT NULL DEFAULT 50.0,
    update_interval_ms  INT NOT NULL DEFAULT 67,
    safety_margin_ms    INT NOT NULL DEFAULT 50,
    is_active           BOOLEAN DEFAULT true
);

CREATE TABLE sim_config (
    sim_config_id       SERIAL PRIMARY KEY,
    name                VARCHAR(32) NOT NULL,
    tick_rate_hz        INT NOT NULL DEFAULT 15,
    noise_std_m         DOUBLE PRECISION NOT NULL DEFAULT 0.02,
    flush_size          INT NOT NULL DEFAULT 12,
    flush_interval_ms   INT NOT NULL DEFAULT 200,
    is_active           BOOLEAN DEFAULT true
);

-- ============ HIGH-THROUGHPUT DATA TABLES ============

CREATE TABLE measurements (
    id              BIGINT GENERATED ALWAYS AS IDENTITY,
    measured_at     TIMESTAMPTZ(6) NOT NULL,
    anchor_id       INT NOT NULL,
    tag_id          INT NOT NULL,
    t1_ticks        BIGINT,
    t2_ticks        BIGINT,
    t3_ticks        BIGINT,
    t4_ticks        BIGINT,
    t5_ticks        BIGINT,
    t6_ticks        BIGINT,
    range_m         DOUBLE PRECISION,
    fp_power        DOUBLE PRECISION,
    rx_power        DOUBLE PRECISION,
    is_valid        BOOLEAN DEFAULT true
);

SELECT create_hypertable('measurements', 'measured_at',
    chunk_time_interval => INTERVAL '1 hour'
);

CREATE TABLE positions (
    id              BIGINT GENERATED ALWAYS AS IDENTITY,
    calculated_at   TIMESTAMPTZ(6) NOT NULL,
    tag_id          INT NOT NULL,
    x_m             DOUBLE PRECISION NOT NULL,
    y_m             DOUBLE PRECISION NOT NULL,
    z_m             DOUBLE PRECISION NOT NULL,
    error_est_m     DOUBLE PRECISION,
    anchors_used    SMALLINT NOT NULL,
    method          VARCHAR(32) NOT NULL DEFAULT 'least_squares',
    calc_config_id  INT NOT NULL
);

SELECT create_hypertable('positions', 'calculated_at',
    chunk_time_interval => INTERVAL '1 hour'
);

-- ============ AUDIT LOG ============

CREATE TABLE audit_log (
    log_id      BIGINT GENERATED ALWAYS AS IDENTITY,
    logged_at   TIMESTAMPTZ DEFAULT now(),
    action      VARCHAR(32),
    user_id     VARCHAR(32),
    ip_address  VARCHAR(45),
    details     TEXT
);

SELECT create_hypertable('audit_log', 'logged_at',
    chunk_time_interval => INTERVAL '1 day'
);

-- ============ INDEXES ============

-- Position calculator queries measurements by tag + time range
CREATE INDEX idx_meas_tag_time ON measurements (tag_id, measured_at DESC);

-- Frontend queries latest position per tag
CREATE INDEX idx_pos_tag_time ON positions (tag_id, calculated_at DESC);

-- ============ RETENTION POLICIES ============

SELECT add_retention_policy('measurements', INTERVAL '7 days');
SELECT add_retention_policy('positions', INTERVAL '7 days');
