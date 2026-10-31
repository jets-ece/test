require("dotenv").config();
const express = require("express");
const compression = require("compression");
const cors = require("cors");
const { Pool } = require("pg");
const swaggerUi = require("swagger-ui-express");
const swaggerSpec = require("./swagger.json");

const app = express();
app.use(compression());
app.use(cors());
app.use(express.json({ limit: "1mb" }));

// ---------- Request Logger ----------

let reqCounter = 0;
const SLOW_MS = 200;
// Only log these routes (plus all errors and slow responses)
const ALWAYS_LOG_ROUTES = new Set([
  "/api/measurements/hardware",
]);

function extractDetail(body) {
  if (!body || typeof body !== "object") return "";
  if (body.error) return body.error;
  if (body.inserted != null) {
    let s = `inserted=${body.inserted}`;
    if (body.skipped) s += ` skipped=${body.skipped}`;
    if (body.anchor_ids?.length) s += ` A:${body.anchor_ids}`;
    if (body.tag_ids?.length) s += ` T:${body.tag_ids}`;
    return s;
  }
  if (body.anchor_id != null) return `anchor_id=${body.anchor_id}`;
  if (Array.isArray(body.measurements)) return `${body.measurements.length} rows`;
  if (Array.isArray(body)) return `${body.length} rows`;
  return "";
}

app.use((req, res, next) => {
  // Skip logging for Swagger UI static assets
  if (req.originalUrl.startsWith("/api/docs")) return next();

  const start = Date.now();
  const id = ++reqCounter;
  let capturedBody = undefined;

  const origJson = res.json.bind(res);
  res.json = (body) => {
    capturedBody = body;
    return origJson(body);
  };

  res.on("finish", () => {
    const ms = Date.now() - start;
    const status = res.statusCode;
    const isErr = status >= 400;
    const isSlow = !isErr && ms >= SLOW_MS;
    const tag = isErr ? "ERR " : isSlow ? "SLOW" : "OK  ";
    const method = req.method.padEnd(4);
    const url = req.originalUrl;
    const num = String(id).padStart(3);
    // Only log hardware/measurement routes by default; always log errors and slow responses
    if (!isErr && !isSlow && !ALWAYS_LOG_ROUTES.has(req.path)) {
      return;
    }

    const detail = extractDetail(capturedBody);
    const suffix = detail ? `  | ${detail}` : "";
    console.log(`[${num}] ${tag} ${method} ${url} → ${status} ${String(ms).padStart(3)}ms${suffix}`);
  });

  next();
});

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: parseInt(process.env.DB_POOL_MAX) || 50,
  idleTimeoutMillis: parseInt(process.env.DB_POOL_IDLE_TIMEOUT_MS) || 30000,
  connectionTimeoutMillis: parseInt(process.env.DB_POOL_CONNECTION_TIMEOUT_MS) || 5000,
  statement_timeout: parseInt(process.env.DB_STATEMENT_TIMEOUT_MS) || 30000,
});

// ---------- DB Error Enrichment ----------

const PG_ERROR_NAMES = {
  "42P01": "undefined_table",
  "23505": "unique_violation",
  "23503": "foreign_key_violation",
  "23502": "not_null_violation",
  "42703": "undefined_column",
};

function enrichPgError(err) {
  if (!err || !err.code) return err;
  const name = PG_ERROR_NAMES[err.code];
  if (name) {
    let msg = `DB ${name}: ${err.message}`;
    if (err.detail) msg += ` (${err.detail})`;
    if (err.constraint) msg += ` [${err.constraint}]`;
    err.message = msg;
  }
  return err;
}

function wrapClientQuery(client) {
  if (client._queryWrapped) return;
  client._queryWrapped = true;
  const origClientQuery = client.query.bind(client);
  client.query = (...args) => {
    const hasCb = typeof args[args.length - 1] === 'function';
    if (hasCb) {
      const cb = args[args.length - 1];
      args[args.length - 1] = (err, res) => cb(err ? enrichPgError(err) : null, res);
      return origClientQuery(...args);
    }
    return origClientQuery(...args).catch((err) => Promise.reject(enrichPgError(err)));
  };
}

const origPoolQuery = pool.query.bind(pool);
pool.query = (...args) => {
  const result = origPoolQuery(...args);
  return result && typeof result.catch === 'function'
    ? result.catch((err) => Promise.reject(enrichPgError(err)))
    : result;
};

const origPoolConnect = pool.connect.bind(pool);
pool.connect = function(cb) {
  if (typeof cb === 'function') {
    return origPoolConnect((err, client, done) => {
      if (err) return cb(enrichPgError(err), client, done);
      wrapClientQuery(client);
      cb(null, client, done);
    });
  }
  return origPoolConnect().then((client) => {
    wrapClientQuery(client);
    return client;
  });
};

let hardwareActive = true;

const MAX_BATCH_SIZE = 1000;

function isPoolExhausted(err) {
  return err && err.message && err.message.includes("timeout exceeded when trying to connect");
}

// ---------- Hardware ID Cache ----------

const anchorHwCache = new Map(); // hardware_id string → anchor_id int
const tagHwCache = new Map();    // hardware_id string → tag_id int

async function resolveHardwareId(hwId, type) {
  const cache = type === "anchor" ? anchorHwCache : tagHwCache;
  const table = type === "anchor" ? "anchors" : "tags";
  const idCol = type === "anchor" ? "anchor_id" : "tag_id";
  const labelPrefix = type === "anchor" ? "Anchor" : "Tag";

  // 1. Check cache
  if (cache.has(hwId)) return cache.get(hwId);

  // 2. Check DB
  const { rows: existing } = await pool.query(
    `SELECT ${idCol} FROM ${table} WHERE hardware_id = $1`, [hwId]
  );
  if (existing.length > 0) {
    cache.set(hwId, existing[0][idCol]);
    return existing[0][idCol];
  }

  // 3. Auto-register with upsert (handles race conditions)
  const insertSql = type === "anchor"
    ? `INSERT INTO anchors (label, hardware_id) VALUES ($1, $2)
       ON CONFLICT (hardware_id) DO UPDATE SET hardware_id = EXCLUDED.hardware_id
       RETURNING anchor_id`
    : `INSERT INTO tags (label, hardware_id, source) VALUES ($1, $2, 'real')
       ON CONFLICT (hardware_id) DO UPDATE SET hardware_id = EXCLUDED.hardware_id
       RETURNING tag_id`;

  const { rows: [created] } = await pool.query(insertSql, [`${labelPrefix}_${hwId}`, hwId]);
  const newId = created[idCol];
  cache.set(hwId, newId);
  console.log(`[HW] Auto-registered ${type} hardware_id=${hwId} → ${idCol}=${newId}`);
  return newId;
}

async function warmHardwareCache() {
  const { rows: anchors } = await pool.query(
    `SELECT anchor_id, hardware_id FROM anchors WHERE hardware_id IS NOT NULL`
  );
  for (const r of anchors) anchorHwCache.set(r.hardware_id, r.anchor_id);

  const { rows: tags } = await pool.query(
    `SELECT tag_id, hardware_id FROM tags WHERE hardware_id IS NOT NULL`
  );
  for (const r of tags) tagHwCache.set(r.hardware_id, r.tag_id);

  if (anchorHwCache.size + tagHwCache.size > 0) {
    console.log(`[HW] Cache warmed: ${anchorHwCache.size} anchors, ${tagHwCache.size} tags`);
  }
}

// ---------- Swagger ----------

app.use("/api/docs", swaggerUi.serve, swaggerUi.setup(swaggerSpec));

// ---------- Health ----------

app.get("/api/health", async (req, res) => {
  try {
    const { rows } = await pool.query("SELECT NOW() AS server_time");
    res.json({ status: "ok", server_time: rows[0].server_time });
  } catch (err) {
    const status = isPoolExhausted(err) ? 503 : 500;
    res.status(status).json({ status: "error", message: err.message });
  }
});

// ---------- Hardware Status ----------

app.get("/api/hardware/status", (req, res) => {
  res.json({ active: hardwareActive });
});

app.put("/api/hardware/status", (req, res) => {
  const { active } = req.body;
  if (typeof active !== "boolean") {
    return res.status(400).json({ error: "active (boolean) is required" });
  }
  hardwareActive = active;
  console.log(`[HW] Hardware status set to ${active ? "ON" : "OFF"}`);
  res.json({ active: hardwareActive });
});

// ---------- Pipeline Status ----------

app.get("/api/status/pipeline", async (req, res) => {
  try {
    const [
      lastMeas,
      measCount,
      lastPos,
      posCount,
      anchorStats,
      calcConfig,
    ] = await Promise.all([
      pool.query(
        `SELECT measured_at AS last_at, NOW() AS server_time
         FROM measurements ORDER BY measured_at DESC LIMIT 1`
      ),
      pool.query(
        `SELECT COUNT(*)::int AS count_60s
         FROM measurements WHERE measured_at > NOW() - INTERVAL '60 seconds'`
      ),
      pool.query(
        `SELECT calculated_at AS last_at
         FROM positions ORDER BY calculated_at DESC LIMIT 1`
      ),
      pool.query(
        `SELECT COUNT(*)::int AS count_60s
         FROM positions WHERE calculated_at > NOW() - INTERVAL '60 seconds'`
      ),
      pool.query(
        `WITH recent AS (
           SELECT anchor_id, MAX(measured_at) AS last_at, COUNT(*)::int AS count_60s
           FROM measurements WHERE measured_at > NOW() - INTERVAL '60 seconds'
           GROUP BY anchor_id
         )
         SELECT a.anchor_id, r.last_at, COALESCE(r.count_60s, 0) AS count_60s
         FROM anchors a LEFT JOIN recent r ON r.anchor_id = a.anchor_id
         WHERE a.is_active = true
         ORDER BY a.anchor_id`
      ),
      pool.query(
        `SELECT window_size_ms, update_interval_ms, safety_margin_ms
         FROM calc_config WHERE is_active = true
         ORDER BY calc_config_id DESC LIMIT 1`
      ),
    ]);

    const serverTime = lastMeas.rows[0]?.server_time || new Date();

    const measLastAt = lastMeas.rows[0]?.last_at || null;
    const measAge = measLastAt
      ? new Date(serverTime).getTime() - new Date(measLastAt).getTime()
      : null;
    const measCount60 = measCount.rows[0]?.count_60s || 0;

    const posLastAt = lastPos.rows[0]?.last_at || null;
    const posAge = posLastAt
      ? new Date(serverTime).getTime() - new Date(posLastAt).getTime()
      : null;
    const posCount60 = posCount.rows[0]?.count_60s || 0;

    const anchors = anchorStats.rows.map((r) => ({
      anchor_id: r.anchor_id,
      last_at: r.last_at || null,
      age_ms: r.last_at
        ? new Date(serverTime).getTime() - new Date(r.last_at).getTime()
        : null,
      count_60s: r.count_60s,
    }));

    const defaultThresholds = { measurement_stale_ms: 5000, position_stale_ms: 5000 };

    res.json({
      server_time: serverTime,
      measurements: {
        last_at: measLastAt,
        age_ms: measAge,
        count_60s: measCount60,
        rate_per_s: +(measCount60 / 60).toFixed(1),
      },
      positions: {
        last_at: posLastAt,
        age_ms: posAge,
        count_60s: posCount60,
        rate_per_s: +(posCount60 / 60).toFixed(1),
      },
      anchors,
      thresholds: defaultThresholds,
    });
  } catch (err) {
    const status = isPoolExhausted(err) ? 503 : 500;
    res.status(status).json({ error: err.message });
  }
});

// ---------- Anchors ----------

app.get("/api/anchors", async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT a.anchor_id, a.label, a.hardware_id, c.x_m, c.y_m, c.z_m
      FROM anchors a
      LEFT JOIN anchor_calibrations c ON a.anchor_id = c.anchor_id AND c.is_active = true
      WHERE a.is_active = true
      ORDER BY a.anchor_id
    `);
    res.json(rows);
  } catch (err) {
    const status = isPoolExhausted(err) ? 503 : 500;
    res.status(status).json({ error: err.message });
  }
});

app.post("/api/anchors", async (req, res) => {
  let client;
  try {
    const { label, hardware_id, x_m, y_m, z_m } = req.body;
    if (!label || x_m == null || y_m == null || z_m == null) {
      return res.status(400).json({ error: "label, x_m, y_m, and z_m are required" });
    }

    client = await pool.connect();
    await client.query("BEGIN");
    const { rows: [anchor] } = await client.query(
      `INSERT INTO anchors (label, hardware_id) VALUES ($1, $2) RETURNING *`,
      [label, hardware_id || null]
    );
    const { rows: [calibration] } = await client.query(
      `INSERT INTO anchor_calibrations (anchor_id, x_m, y_m, z_m, calibrated_by, method)
       VALUES ($1, $2, $3, $4, 'api', 'manual') RETURNING *`,
      [anchor.anchor_id, x_m, y_m, z_m]
    );
    await client.query("COMMIT");

    res.status(201).json({ ...anchor, x_m: calibration.x_m, y_m: calibration.y_m, z_m: calibration.z_m });
  } catch (err) {
    if (client) await client.query("ROLLBACK").catch(() => {});
    const status = isPoolExhausted(err) ? 503 : 500;
    res.status(status).json({ error: err.message });
  } finally {
    if (client) client.release();
  }
});

app.put("/api/anchors/:id", async (req, res) => {
  let client;
  try {
    const id = parseInt(req.params.id);
    const { label, hardware_id, is_active, x_m, y_m, z_m } = req.body;

    client = await pool.connect();
    await client.query("BEGIN");

    // Update anchor fields if provided
    const anchorUpdates = [];
    const anchorValues = [];
    let idx = 1;
    if (label !== undefined) { anchorUpdates.push(`label = $${idx}`); anchorValues.push(label); idx++; }
    if (hardware_id !== undefined) { anchorUpdates.push(`hardware_id = $${idx}`); anchorValues.push(hardware_id); idx++; }
    if (is_active !== undefined) { anchorUpdates.push(`is_active = $${idx}`); anchorValues.push(is_active); idx++; }

    let anchor;
    if (anchorUpdates.length > 0) {
      anchorValues.push(id);
      const sql = `UPDATE anchors SET ${anchorUpdates.join(", ")} WHERE anchor_id = $${idx} RETURNING *`;
      const result = await client.query(sql, anchorValues);
      if (result.rows.length === 0) {
        await client.query("ROLLBACK");
        return res.status(404).json({ error: "Anchor not found" });
      }
      anchor = result.rows[0];
    } else {
      const result = await client.query("SELECT * FROM anchors WHERE anchor_id = $1", [id]);
      if (result.rows.length === 0) {
        await client.query("ROLLBACK");
        return res.status(404).json({ error: "Anchor not found" });
      }
      anchor = result.rows[0];
    }

    // Recalibrate position if coordinates provided
    let calibration;
    if (x_m != null && y_m != null && z_m != null) {
      await client.query(
        `UPDATE anchor_calibrations SET is_active = false WHERE anchor_id = $1 AND is_active = true`,
        [id]
      );
      const { rows: [cal] } = await client.query(
        `INSERT INTO anchor_calibrations (anchor_id, x_m, y_m, z_m, calibrated_by, method)
         VALUES ($1, $2, $3, $4, 'api', 'manual') RETURNING *`,
        [id, x_m, y_m, z_m]
      );
      calibration = cal;
    } else {
      const { rows } = await client.query(
        `SELECT * FROM anchor_calibrations WHERE anchor_id = $1 AND is_active = true LIMIT 1`,
        [id]
      );
      calibration = rows[0];
    }

    await client.query("COMMIT");
    res.json({
      ...anchor,
      x_m: calibration?.x_m,
      y_m: calibration?.y_m,
      z_m: calibration?.z_m,
    });
  } catch (err) {
    if (client) await client.query("ROLLBACK").catch(() => {});
    const status = isPoolExhausted(err) ? 503 : 500;
    res.status(status).json({ error: err.message });
  } finally {
    if (client) client.release();
  }
});

// ---------- Tags ----------

app.get("/api/tags", async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT tag_id, label, hardware_id, source, is_active, created_at
      FROM tags
      ORDER BY tag_id
    `);
    res.json(rows);
  } catch (err) {
    const status = isPoolExhausted(err) ? 503 : 500;
    res.status(status).json({ error: err.message });
  }
});

app.post("/api/tags", async (req, res) => {
  try {
    const { label, hardware_id, source } = req.body;
    if (!label) {
      return res.status(400).json({ error: "label is required" });
    }
    const validSources = ["virtual", "real"];
    const tagSource = validSources.includes(source) ? source : "real";

    const { rows } = await pool.query(
      `INSERT INTO tags (label, hardware_id, source) VALUES ($1, $2, $3) RETURNING *`,
      [label, hardware_id || null, tagSource]
    );
    res.status(201).json(rows[0]);
  } catch (err) {
    const status = isPoolExhausted(err) ? 503 : 500;
    res.status(status).json({ error: err.message });
  }
});

app.put("/api/tags/:id", async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    const allowedFields = ["label", "hardware_id", "source", "is_active"];

    const updates = [];
    const values = [];
    let paramIndex = 1;

    for (const field of allowedFields) {
      if (req.body[field] !== undefined) {
        if (field === "source" && !["virtual", "real"].includes(req.body[field])) {
          return res.status(400).json({ error: "source must be 'virtual' or 'real'" });
        }
        updates.push(`${field} = $${paramIndex}`);
        values.push(req.body[field]);
        paramIndex++;
      }
    }

    if (updates.length === 0) {
      return res.status(400).json({ error: "No valid fields to update" });
    }

    values.push(id);
    const sql = `UPDATE tags SET ${updates.join(", ")} WHERE tag_id = $${paramIndex} RETURNING *`;
    const { rows } = await pool.query(sql, values);

    if (rows.length === 0) {
      return res.status(404).json({ error: "Tag not found" });
    }
    res.json(rows[0]);
  } catch (err) {
    const status = isPoolExhausted(err) ? 503 : 500;
    res.status(status).json({ error: err.message });
  }
});

// ---------- Config: Calculation ----------

app.get("/api/config/calc", async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT calc_config_id, name, window_size_ms, min_anchors,
             max_range_m, update_interval_ms, safety_margin_ms, is_active
      FROM calc_config
      WHERE is_active = true
      ORDER BY calc_config_id DESC
      LIMIT 1
    `);
    if (rows.length === 0) {
      return res.status(404).json({ error: "No active calculation config found" });
    }
    res.json(rows[0]);
  } catch (err) {
    const status = isPoolExhausted(err) ? 503 : 500;
    res.status(status).json({ error: err.message });
  }
});

app.put("/api/config/calc/:id", async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    const allowedFields = [
      "name", "window_size_ms", "min_anchors", "max_range_m",
      "update_interval_ms", "safety_margin_ms", "is_active",
    ];

    const updates = [];
    const values = [];
    let paramIndex = 1;

    for (const field of allowedFields) {
      if (req.body[field] !== undefined) {
        updates.push(`${field} = $${paramIndex}`);
        values.push(req.body[field]);
        paramIndex++;
      }
    }

    if (updates.length === 0) {
      return res.status(400).json({ error: "No valid fields to update" });
    }

    values.push(id);
    const sql = `UPDATE calc_config SET ${updates.join(", ")} WHERE calc_config_id = $${paramIndex} RETURNING *`;
    const { rows } = await pool.query(sql, values);

    if (rows.length === 0) {
      return res.status(404).json({ error: "Config not found" });
    }
    res.json(rows[0]);
  } catch (err) {
    const status = isPoolExhausted(err) ? 503 : 500;
    res.status(status).json({ error: err.message });
  }
});

// ---------- Config: Simulation ----------

app.get("/api/config/sim", async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT sim_config_id, name, tick_rate_hz, noise_std_m,
             flush_size, flush_interval_ms, is_active
      FROM sim_config
      WHERE is_active = true
      ORDER BY sim_config_id DESC
      LIMIT 1
    `);
    if (rows.length === 0) {
      return res.status(404).json({ error: "No active simulation config found" });
    }
    res.json(rows[0]);
  } catch (err) {
    const status = isPoolExhausted(err) ? 503 : 500;
    res.status(status).json({ error: err.message });
  }
});

app.put("/api/config/sim/:id", async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    const allowedFields = [
      "name", "tick_rate_hz", "noise_std_m",
      "flush_size", "flush_interval_ms", "is_active",
    ];

    const updates = [];
    const values = [];
    let paramIndex = 1;

    for (const field of allowedFields) {
      if (req.body[field] !== undefined) {
        updates.push(`${field} = $${paramIndex}`);
        values.push(req.body[field]);
        paramIndex++;
      }
    }

    if (updates.length === 0) {
      return res.status(400).json({ error: "No valid fields to update" });
    }

    values.push(id);
    const sql = `UPDATE sim_config SET ${updates.join(", ")} WHERE sim_config_id = $${paramIndex} RETURNING *`;
    const { rows } = await pool.query(sql, values);

    if (rows.length === 0) {
      return res.status(404).json({ error: "Config not found" });
    }
    res.json(rows[0]);
  } catch (err) {
    const status = isPoolExhausted(err) ? 503 : 500;
    res.status(status).json({ error: err.message });
  }
});

// ---------- DS-TWR Range Computation ----------

const SPEED_OF_LIGHT = 299_792_458;
const DW_TICK_FREQ = 499.2e6 * 128;

function computeRange(t1, t2, t3, t4, t5, t6) {
  if (t1 == null || t2 == null || t3 == null || t4 == null) return null;
  t1 = Number(t1); t2 = Number(t2); t3 = Number(t3); t4 = Number(t4);
  const tRound1 = t4 - t1;
  const tReply1 = t3 - t2;

  if (t5 != null && t6 != null) {
    // DS-TWR (6 timestamps)
    t5 = Number(t5); t6 = Number(t6);
    const tRound2 = t6 - t3;
    const tReply2 = t5 - t4;
    const denom = tRound1 + tRound2 + tReply1 + tReply2;
    if (denom === 0) return 0;
    const tofTicks = (tRound1 * tRound2 - tReply1 * tReply2) / denom;
    return Math.max(0, (tofTicks / DW_TICK_FREQ) * SPEED_OF_LIGHT);
  }

  // SS-TWR (4 timestamps)
  const tofTicks = (tRound1 - tReply1) / 2;
  if (tofTicks <= 0) return 0;
  return (tofTicks / DW_TICK_FREQ) * SPEED_OF_LIGHT;
}

// ---------- Ranges: Latest per (tag, anchor) pair ----------

app.get("/api/ranges/latest", async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT DISTINCT ON (tag_id, anchor_id)
        tag_id, anchor_id, range_m,
        t1_ticks, t2_ticks, t3_ticks, t4_ticks, t5_ticks, t6_ticks,
        measured_at
      FROM measurements
      WHERE measured_at > NOW() - INTERVAL '30 seconds'
        AND is_valid = true
      ORDER BY tag_id, anchor_id, measured_at DESC
    `);

    const result = rows.map((r) => ({
      tag_id: r.tag_id,
      anchor_id: r.anchor_id,
      range_m:
        r.range_m != null
          ? r.range_m
          : r.t1_ticks != null
            ? computeRange(r.t1_ticks, r.t2_ticks, r.t3_ticks, r.t4_ticks, r.t5_ticks, r.t6_ticks)
            : null,
      measured_at: r.measured_at,
    }));

    res.json(result);
  } catch (err) {
    const status = isPoolExhausted(err) ? 503 : 500;
    res.status(status).json({ error: err.message });
  }
});

// ---------- Ranges: History for a (tag, anchor) pair ----------

app.get("/api/ranges/history", async (req, res) => {
  try {
    const tagId = parseInt(req.query.tag_id);
    const anchorId = parseInt(req.query.anchor_id);
    if (isNaN(tagId) || isNaN(anchorId)) {
      return res.status(400).json({ error: "tag_id and anchor_id query params required" });
    }
    const limit = Math.min(parseInt(req.query.limit) || 200, 2000);

    const { rows } = await pool.query(`
      SELECT range_m,
        t1_ticks, t2_ticks, t3_ticks, t4_ticks, t5_ticks, t6_ticks,
        measured_at
      FROM measurements
      WHERE tag_id = $1 AND anchor_id = $2 AND is_valid = true
      ORDER BY measured_at DESC
      LIMIT $3
    `, [tagId, anchorId, limit]);

    const result = rows.map((r) => ({
      range_m:
        r.range_m != null
          ? r.range_m
          : r.t1_ticks != null
            ? computeRange(r.t1_ticks, r.t2_ticks, r.t3_ticks, r.t4_ticks, r.t5_ticks, r.t6_ticks)
            : null,
      measured_at: r.measured_at,
    }));

    res.json({ tag_id: tagId, anchor_id: anchorId, ranges: result });
  } catch (err) {
    const status = isPoolExhausted(err) ? 503 : 500;
    res.status(status).json({ error: err.message });
  }
});

// ---------- Measurements: Batch insert from hardware ----------

app.post("/api/measurements/hardware", async (req, res) => {
  try {
    const rows = Array.isArray(req.body) ? req.body : [req.body];
    if (rows.length === 0) {
      return res.status(400).json({ error: "Expected a JSON object or array of measurements" });
    }
    if (rows.length > MAX_BATCH_SIZE) {
      return res.status(400).json({ error: `Batch too large (max ${MAX_BATCH_SIZE})` });
    }

    // Collect unique hardware IDs from explicit fields and resolve in parallel
    const anchorHwIds = new Set();
    const tagHwIds = new Set();
    for (const r of rows) {
      if (r.anchor_hardware_id) anchorHwIds.add(r.anchor_hardware_id);
      if (r.tag_hardware_id) tagHwIds.add(r.tag_hardware_id);
    }

    const resolvedAnchors = new Map();
    const resolvedTags = new Map();
    await Promise.all([
      ...Array.from(anchorHwIds).map(async (hwId) => {
        resolvedAnchors.set(hwId, await resolveHardwareId(hwId, "anchor"));
      }),
      ...Array.from(tagHwIds).map(async (hwId) => {
        resolvedTags.set(hwId, await resolveHardwareId(hwId, "tag"));
      }),
    ]);

    const columns = [
      "measured_at", "anchor_id", "tag_id",
      "t1_ticks", "t2_ticks", "t3_ticks",
      "t4_ticks", "t5_ticks", "t6_ticks",
      "range_m", "fp_power", "rx_power", "is_valid",
    ];
    const colCount = columns.length;
    const valueClauses = [];
    const params = [];
    let accepted = 0;
    const usedAnchors = new Set();
    const usedTags = new Set();

    for (const r of rows) {
      const t1 = r.timePollSent ?? null;
      const t2 = r.timePollReceived ?? null;
      const t3 = r.timePollAckSent ?? null;
      const t4 = r.timePollAckReceived ?? null;
      const t5 = r.timeRangeSent ?? null;
      const t6 = r.timeRangeReceived ?? null;
      const rangeM = r.range_m ?? null;

      // Need either range_m or at least t1-t4
      if (rangeM == null && (t1 == null || t2 == null || t3 == null || t4 == null || t5 == null || t6 == null)) {
        continue;
      }

      // Resolve anchor: require anchor_hardware_id or anchor_id
      let anchorId;
      if (r.anchor_hardware_id) {
        anchorId = resolvedAnchors.get(r.anchor_hardware_id);
      } else if (r.anchor_id != null) {
        anchorId = parseInt(r.anchor_id);
        if (isNaN(anchorId)) continue; // skip — invalid anchor_id
      } else {
        continue; // skip row — no anchor identifier
      }

      // Resolve tag: require tag_hardware_id or tag_id
      let tagId;
      if (r.tag_hardware_id) {
        tagId = resolvedTags.get(r.tag_hardware_id);
      } else if (r.tag_id != null) {
        tagId = parseInt(r.tag_id);
        if (isNaN(tagId)) continue; // skip — invalid tag_id
      } else {
        continue; // skip row — no tag identifier
      }

      const offset = accepted * colCount;
      const placeholders = columns.map((_, j) => `$${offset + j + 1}`);
      valueClauses.push(`(${placeholders.join(", ")})`);

      params.push(
        new Date(),                          // measured_at — server timestamp
        anchorId,                            // anchor_id (resolved)
        tagId,                               // tag_id (resolved)
        t1, t2, t3, t4, t5, t6,           // t5/t6 not used by current hardware
        rangeM,                              // range_m (use hardware value if provided)
        r.fp_power ?? null,                  // fp_power (optional)
        r.rx_power ?? null,                  // rx_power (optional)
        true,                                // is_valid
      );
      usedAnchors.add(anchorId);
      usedTags.add(tagId);
      accepted++;
    }

    if (accepted === 0) {
      return res.status(400).json({ error: "No valid rows (each row needs range_m or at least t1-t4)" });
    }

    const sql = `INSERT INTO measurements (${columns.join(", ")}) VALUES ${valueClauses.join(", ")}`;
    await pool.query(sql, params);

    res.status(201).json({
      inserted: accepted,
      skipped: rows.length - accepted,
      anchor_ids: [...usedAnchors],
      tag_ids: [...usedTags],
    });
  } catch (err) {
    const status = isPoolExhausted(err) ? 503 : 500;
    res.status(status).json({ error: err.message });
  }
});

// ---------- Measurements: Batch insert (Simulator → API) ----------

app.post("/api/measurements", async (req, res) => {
  try {
    const { measurements } = req.body;
    if (!Array.isArray(measurements) || measurements.length === 0) {
      return res.status(400).json({ error: "measurements array required" });
    }
    if (measurements.length > MAX_BATCH_SIZE) {
      return res.status(400).json({ error: `Batch too large (max ${MAX_BATCH_SIZE})` });
    }

    const columns = [
      "measured_at", "anchor_id", "tag_id",
      "t1_ticks", "t2_ticks", "t3_ticks",
      "t4_ticks", "t5_ticks", "t6_ticks",
      "range_m", "fp_power", "rx_power", "is_valid",
    ];
    const colCount = columns.length;
    const valueClauses = [];
    const params = [];
    const errors = [];
    let accepted = 0;

    measurements.forEach((m, i) => {
      const ticks = [m.t1_ticks, m.t2_ticks, m.t3_ticks, m.t4_ticks, m.t5_ticks, m.t6_ticks];
      const hasAllTicks = ticks.every((t) => t != null);
      const hasAnyTick = ticks.some((t) => t != null);
      const hasRange = m.range_m != null;

      // Must have either all 6 ticks or range_m
      if (!hasAllTicks && !hasRange) {
        errors.push({ index: i, error: "requires either all 6 tick fields (t1-t6) or range_m" });
        return;
      }

      // Partial ticks without range_m is invalid
      if (hasAnyTick && !hasAllTicks && !hasRange) {
        errors.push({ index: i, error: "partial tick fields provided without range_m" });
        return;
      }

      const range = m.range_m ?? null;

      const offset = accepted * colCount;
      const placeholders = columns.map((_, j) => `$${offset + j + 1}`);
      valueClauses.push(`(${placeholders.join(", ")})`);
      params.push(
        m.measured_at, m.anchor_id, m.tag_id,
        m.t1_ticks ?? null, m.t2_ticks ?? null, m.t3_ticks ?? null,
        m.t4_ticks ?? null, m.t5_ticks ?? null, m.t6_ticks ?? null,
        range, m.fp_power ?? null, m.rx_power ?? null,
        true
      );
      accepted++;
    });

    if (accepted === 0) {
      return res.status(400).json({ error: "No valid rows", details: errors });
    }

    const sql = `INSERT INTO measurements (${columns.join(", ")}) VALUES ${valueClauses.join(", ")}`;
    await pool.query(sql, params);

    const result = { inserted: accepted };
    if (errors.length > 0) {
      result.skipped = errors.length;
      result.errors = errors;
    }
    res.status(201).json(result);
  } catch (err) {
    const status = isPoolExhausted(err) ? 503 : 500;
    res.status(status).json({ error: err.message });
  }
});

// ---------- Measurements: Time-windowed fetch ----------

app.get("/api/measurements", async (req, res) => {
  try {
    const { since, until, tag_id } = req.query;
    if (!since || !until) {
      return res.status(400).json({ error: "since and until query params required" });
    }
    const limit = Math.min(parseInt(req.query.limit) || 10000, 50000);

    let sql = `SELECT measured_at, anchor_id, tag_id,
              t1_ticks, t2_ticks, t3_ticks, t4_ticks, t5_ticks, t6_ticks,
              range_m, fp_power, rx_power
       FROM measurements
       WHERE measured_at > $1
         AND measured_at <= $2
         AND is_valid = true`;
    const params = [since, until];

    if (tag_id) {
      params.push(parseInt(tag_id));
      sql += ` AND tag_id = $${params.length}`;
    }

    sql += ` ORDER BY measured_at ASC LIMIT $${params.length + 1}`;
    params.push(limit);

    const { rows } = await pool.query(sql, params);
    res.json({ measurements: rows });
  } catch (err) {
    const status = isPoolExhausted(err) ? 503 : 500;
    res.status(status).json({ error: err.message });
  }
});

// ---------- Positions: Batch insert (Calculator → API) ----------

app.post("/api/positions", async (req, res) => {
  try {
    const { positions } = req.body;
    if (!Array.isArray(positions) || positions.length === 0) {
      return res.status(400).json({ error: "positions array required" });
    }
    if (positions.length > MAX_BATCH_SIZE) {
      return res.status(400).json({ error: `Batch too large (max ${MAX_BATCH_SIZE})` });
    }

    const columns = [
      "calculated_at", "tag_id", "x_m", "y_m", "z_m",
      "error_est_m", "anchors_used", "method", "calc_config_id",
    ];
    const colCount = columns.length;
    const valueClauses = [];
    const params = [];

    positions.forEach((p, i) => {
      const offset = i * colCount;
      const placeholders = columns.map((_, j) => `$${offset + j + 1}`);
      valueClauses.push(`(${placeholders.join(", ")})`);
      params.push(
        p.calculated_at, p.tag_id, p.x_m, p.y_m, p.z_m,
        p.error_est_m, p.anchors_used, p.method, p.calc_config_id
      );
    });

    const sql = `INSERT INTO positions (${columns.join(", ")}) VALUES ${valueClauses.join(", ")}`;
    await pool.query(sql, params);

    res.status(201).json({ inserted: positions.length });
  } catch (err) {
    const status = isPoolExhausted(err) ? 503 : 500;
    res.status(status).json({ error: err.message });
  }
});

// ---------- Positions: Latest per tag ----------

app.get("/api/positions/latest", async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT DISTINCT ON (tag_id)
        tag_id, x_m, y_m, z_m, error_est_m, anchors_used, calculated_at
      FROM positions
      WHERE calculated_at > now() - INTERVAL '30 seconds'
      ORDER BY tag_id, calculated_at DESC
    `);
    res.json(rows);
  } catch (err) {
    const status = isPoolExhausted(err) ? 503 : 500;
    res.status(status).json({ error: err.message });
  }
});

// ---------- Positions: History ----------

app.get("/api/positions", async (req, res) => {
  try {
    const tagId = parseInt(req.query.tag_id) || 1;
    const limit = Math.min(parseInt(req.query.limit) || 50, 1000);
    const { since, until } = req.query;

    let sql = `SELECT tag_id, x_m, y_m, z_m, error_est_m, anchors_used, calculated_at
       FROM positions
       WHERE tag_id = $1`;
    const params = [tagId];

    if (since) {
      params.push(since);
      sql += ` AND calculated_at > $${params.length}`;
    }
    if (until) {
      params.push(until);
      sql += ` AND calculated_at <= $${params.length}`;
    }

    sql += ` ORDER BY calculated_at DESC LIMIT $${params.length + 1}`;
    params.push(limit);

    const { rows } = await pool.query(sql, params);
    res.json(rows);
  } catch (err) {
    const status = isPoolExhausted(err) ? 503 : 500;
    res.status(status).json({ error: err.message });
  }
});

// ---------- 404 + Error Handlers ----------

app.use((req, res) => {
  res.status(404).json({ error: "Not found" });
});

app.use((err, req, res, _next) => {
  if (err.type === "entity.parse.failed") {
    return res.status(400).json({ error: `Invalid JSON: ${err.message.split(":")[0]}` });
  }
  if (err.type === "entity.too.large") {
    return res.status(413).json({ error: "Payload too large" });
  }
  const status = isPoolExhausted(err) ? 503 : 500;
  res.status(status).json({ error: err.message });
});

// ---------- Start ----------

const PORT = process.env.PORT || 3000;
let server;

warmHardwareCache()
  .catch((err) => console.warn("[HW] Cache warm failed (will resolve on demand):", err.message))
  .then(() => {
    server = app.listen(PORT, "0.0.0.0", () => {
      console.log(`UWB backend listening on http://0.0.0.0:${PORT} (pool: ${pool.options.max || 50} connections)`);
    });
  });

async function shutdown() {
  console.log("Shutting down gracefully...");
  if (server) server.close();
  await pool.end();
  process.exit(0);
}

// Force exit after 10s if graceful shutdown stalls
const forceExit = () => { process.exit(1); };

process.on("SIGTERM", () => {
  console.log("SIGTERM received");
  setTimeout(forceExit, 10000).unref();
  shutdown();
});

process.on("SIGINT", () => {
  console.log("SIGINT received");
  setTimeout(forceExit, 10000).unref();
  shutdown();
});

process.on("unhandledRejection", (reason) => {
  console.error("[FATAL] Unhandled rejection:", reason);
});

process.on("uncaughtException", (err) => {
  console.error("[FATAL] Uncaught exception:", err.stack || err);
  process.exit(1);
});
