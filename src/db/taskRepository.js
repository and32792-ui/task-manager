const pool = require("./pool");

const taskRepository = {
  async findAll() {
    const { rows } = await pool.query(
      "SELECT id, title, done, created_at, updated_at FROM tasks ORDER BY created_at DESC"
    );
    return rows.map(mapRow);
  },

  async findById(id) {
    const { rows } = await pool.query(
      "SELECT id, title, done, created_at, updated_at FROM tasks WHERE id = $1",
      [id]
    );
    return rows.length > 0 ? mapRow(rows[0]) : null;
  },

  async create(title) {
    const { rows } = await pool.query(
      "INSERT INTO tasks (title) VALUES ($1) RETURNING id, title, done, created_at, updated_at",
      [title]
    );
    return mapRow(rows[0]);
  },

  async update(id, fields) {
    const setClauses = [];
    const values = [];
    let paramIndex = 1;

    if (fields.title !== undefined) {
      setClauses.push(`title = $${paramIndex++}`);
      values.push(fields.title);
    }
    if (fields.done !== undefined) {
      setClauses.push(`done = $${paramIndex++}`);
      values.push(fields.done);
    }

    if (setClauses.length === 0) return null;

    setClauses.push(`updated_at = now()`);
    values.push(id);

    const { rows } = await pool.query(
      `UPDATE tasks SET ${setClauses.join(", ")} WHERE id = $${paramIndex} RETURNING id, title, done, created_at, updated_at`,
      values
    );
    return rows.length > 0 ? mapRow(rows[0]) : null;
  },

  async delete(id) {
    const { rowCount } = await pool.query("DELETE FROM tasks WHERE id = $1", [
      id,
    ]);
    return rowCount > 0;
  },
};

function mapRow(row) {
  return {
    id: row.id,
    title: row.title,
    done: row.done,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

module.exports = taskRepository;
