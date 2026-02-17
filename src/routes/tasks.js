const express = require("express");
const taskRepository = require("../db/taskRepository");
const kafkaProducer = require("../kafka/producer");

const router = express.Router();

// List all tasks
router.get("/", async (req, res, next) => {
  try {
    const tasks = await taskRepository.findAll();
    res.json(tasks);
  } catch (err) {
    next(err);
  }
});

// Create a task
router.post("/", async (req, res, next) => {
  try {
    const { title } = req.body;
    if (!title || !title.trim()) {
      return res.status(400).json({ error: "Title is required" });
    }

    const task = await taskRepository.create(title.trim());
    await kafkaProducer.emit("CREATE", task);

    res.status(201).json(task);
  } catch (err) {
    next(err);
  }
});

// Update a task
router.patch("/:id", async (req, res, next) => {
  try {
    const previousState = await taskRepository.findById(req.params.id);
    if (!previousState) {
      return res.status(404).json({ error: "Task not found" });
    }

    const fields = {};
    if (req.body.title !== undefined) fields.title = req.body.title.trim();
    if (req.body.done !== undefined) fields.done = req.body.done;

    const task = await taskRepository.update(req.params.id, fields);
    await kafkaProducer.emit("UPDATE", task, previousState);

    res.json(task);
  } catch (err) {
    next(err);
  }
});

// Delete a task
router.delete("/:id", async (req, res, next) => {
  try {
    const task = await taskRepository.findById(req.params.id);
    if (!task) {
      return res.status(404).json({ error: "Task not found" });
    }

    await taskRepository.delete(req.params.id);
    await kafkaProducer.emit("DELETE", task);

    res.status(204).end();
  } catch (err) {
    next(err);
  }
});

module.exports = router;
