const pool = require("./pool");

const migrations = [
  {
    name: "001_create_tasks_table",
    sql: `
      CREATE TABLE IF NOT EXISTS tasks (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        title TEXT NOT NULL,
        done BOOLEAN NOT NULL DEFAULT false,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );

      CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks (created_at);
      CREATE INDEX IF NOT EXISTS idx_tasks_done ON tasks (done);
    `,
  },
  {
    name: "002_create_analytics_tables",
    sql: `
      CREATE TABLE IF NOT EXISTS analytics_daily_summary (
        date DATE PRIMARY KEY,
        total_tasks INTEGER NOT NULL DEFAULT 0,
        completed_tasks INTEGER NOT NULL DEFAULT 0,
        completion_rate NUMERIC(5,2) NOT NULL DEFAULT 0,
        avg_lifetime_hours NUMERIC(10,2),
        computed_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `,
  },
  {
    name: "003_create_migrations_table",
    sql: `
      CREATE TABLE IF NOT EXISTS schema_migrations (
        name TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `,
  },
];

async function migrate() {
  const client = await pool.connect();
  try {
    // Ensure migrations table exists first
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        name TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);

    for (const migration of migrations) {
      const { rows } = await client.query(
        "SELECT 1 FROM schema_migrations WHERE name = $1",
        [migration.name]
      );

      if (rows.length === 0) {
        console.log(`Applying migration: ${migration.name}`);
        await client.query("BEGIN");
        try {
          await client.query(migration.sql);
          await client.query(
            "INSERT INTO schema_migrations (name) VALUES ($1)",
            [migration.name]
          );
          await client.query("COMMIT");
          console.log(`  ✓ ${migration.name} applied`);
        } catch (err) {
          await client.query("ROLLBACK");
          throw err;
        }
      } else {
        console.log(`  ⏭ ${migration.name} already applied`);
      }
    }

    console.log("All migrations complete.");
  } finally {
    client.release();
    await pool.end();
  }
}

migrate().catch((err) => {
  console.error("Migration failed:", err);
  process.exit(1);
});
