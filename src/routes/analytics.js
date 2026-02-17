const express = require("express");
const pool = require("../db/pool");

const router = express.Router();

// Get daily summary — populated by Spark analytics jobs
router.get("/summary", async (req, res, next) => {
  try {
    const { rows } = await pool.query(
      `SELECT date, total_tasks, completed_tasks, completion_rate, avg_lifetime_hours, computed_at
       FROM analytics_daily_summary
       ORDER BY date DESC
       LIMIT 30`
    );
    res.json(rows);
  } catch (err) {
    next(err);
  }
});

// Get real-time stats directly from tasks table
router.get("/realtime", async (req, res, next) => {
  try {
    const { rows } = await pool.query(`
      SELECT
        COUNT(*) AS total_tasks,
        COUNT(*) FILTER (WHERE done = true) AS completed_tasks,
        COUNT(*) FILTER (WHERE done = false) AS pending_tasks,
        ROUND(
          COUNT(*) FILTER (WHERE done = true)::numeric / NULLIF(COUNT(*), 0) * 100, 2
        ) AS completion_rate
      FROM tasks
    `);
    res.json(rows[0]);
  } catch (err) {
    next(err);
  }
});

module.exports = router;
