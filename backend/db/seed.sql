-- UWB Indoor Positioning — Seed Data
-- 4 anchors in a 10m x 8m room, 1 tag, default config

-- Anchors
INSERT INTO anchors (label) VALUES
    ('Anchor_A'),
    ('Anchor_B'),
    ('Anchor_C'),
    ('Anchor_D');

-- Calibrated positions (corners of the room, mounted at 2.5m height)
INSERT INTO anchor_calibrations (anchor_id, x_m, y_m, z_m, calibrated_by, method, is_active) VALUES
    (1,  0.0, 0.0, 2.5, 'setup', 'manual', true),   -- bottom-left
    (2, 10.0, 0.0, 2.5, 'setup', 'manual', true),   -- bottom-right
    (3, 10.0, 8.0, 2.5, 'setup', 'manual', true),   -- top-right
    (4,  0.0, 8.0, 2.5, 'setup', 'manual', true);   -- top-left

-- Tag (the tracked object)
INSERT INTO tags (label, source) VALUES ('Tag_1', 'virtual');

-- Default calculation config
INSERT INTO calc_config (name, window_size_ms, min_anchors, max_range_m, update_interval_ms, safety_margin_ms, is_active)
VALUES ('default', 2000, 3, 50.0, 67, 50, true);

-- Default simulation config
INSERT INTO sim_config (name, tick_rate_hz, noise_std_m, flush_size, flush_interval_ms, is_active)
VALUES ('default', 15, 0.02, 14, 200, true);
