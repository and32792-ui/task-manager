const express = require("express");
const crypto = require("crypto");
const path = require("path");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

let tasks = [];

// List all tasks
app.get("/api/tasks", (req, res) => {
  res.json(tasks);
});

// Create a task
app.post("/api/tasks", (req, res) => {
  const { title } = req.body;
  if (!title || !title.trim()) {
    return res.status(400).json({ error: "Title is required" });
  }
  const task = {
    id: crypto.randomUUID(),
    title: title.trim(),
    done: false,
    createdAt: new Date().toISOString(),
  };
  tasks.push(task);
  res.status(201).json(task);
});

// Update a task
app.patch("/api/tasks/:id", (req, res) => {
  const task = tasks.find((t) => t.id === req.params.id);
  if (!task) {
    return res.status(404).json({ error: "Task not found" });
  }
  if (req.body.title !== undefined) task.title = req.body.title.trim();
  if (req.body.done !== undefined) task.done = req.body.done;
  res.json(task);
});

// Delete a task
app.delete("/api/tasks/:id", (req, res) => {
  const index = tasks.findIndex((t) => t.id === req.params.id);
  if (index === -1) {
    return res.status(404).json({ error: "Task not found" });
  }
  tasks.splice(index, 1);
  res.status(204).end();
});

app.listen(PORT, () => {
  console.log(`Task Manager running at http://localhost:${PORT}`);
});
