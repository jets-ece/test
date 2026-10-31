-- Migration 001: Add hardware_id column to anchors and tags
-- Allows DW1000 hex addresses to be mapped to integer IDs

ALTER TABLE anchors ADD COLUMN IF NOT EXISTS hardware_id TEXT UNIQUE;
ALTER TABLE tags ADD COLUMN IF NOT EXISTS hardware_id TEXT UNIQUE;
