require("dotenv").config();
const fs = require("fs");
const path = require("path");
const { Client } = require("pg");

async function main() {
  const url = process.env.DATABASE_URL;
  if (!url) {
    console.error("DATABASE_URL is not set. Copy .env.example to .env and fill in your credentials.");
    process.exit(1);
  }

  const client = new Client({ connectionString: url });
  await client.connect();

  const schemaPath = path.join(__dirname, "..", "db", "schema.sql");
  const seedPath = path.join(__dirname, "..", "db", "seed.sql");

  console.log("Running schema.sql ...");
  await client.query(fs.readFileSync(schemaPath, "utf8"));
  console.log("Schema created.");

  console.log("Running seed.sql ...");
  await client.query(fs.readFileSync(seedPath, "utf8"));
  console.log("Seed data inserted.");

  await client.end();
  console.log("Done.");
}

main().catch((err) => {
  console.error("DB init failed:", err.message);
  process.exit(1);
});
