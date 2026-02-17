const express = require("express");
const ozoneClient = require("../ozone/client");
const taskRepository = require("../db/taskRepository");

const router = express.Router();

// List attachments for a task
router.get("/:taskId/attachments", async (req, res, next) => {
  try {
    const task = await taskRepository.findById(req.params.taskId);
    if (!task) {
      return res.status(404).json({ error: "Task not found" });
    }

    const attachments = await ozoneClient.listAttachments(req.params.taskId);
    res.json(attachments);
  } catch (err) {
    next(err);
  }
});

// Upload an attachment to a task
router.post("/:taskId/attachments", express.raw({ type: "*/*", limit: "50mb" }), async (req, res, next) => {
  try {
    const task = await taskRepository.findById(req.params.taskId);
    if (!task) {
      return res.status(404).json({ error: "Task not found" });
    }

    const filename = req.headers["x-filename"] || `upload-${Date.now()}`;
    const contentType = req.headers["content-type"] || "application/octet-stream";

    const result = await ozoneClient.uploadAttachment(
      req.params.taskId,
      filename,
      req.body,
      contentType
    );

    res.status(201).json({
      message: "Attachment uploaded",
      bucket: result.bucket,
      key: result.key,
      filename,
    });
  } catch (err) {
    next(err);
  }
});

// Download an attachment
router.get("/:taskId/attachments/:filename", async (req, res, next) => {
  try {
    const { body, contentType } = await ozoneClient.getAttachment(
      req.params.taskId,
      req.params.filename
    );

    res.setHeader("Content-Type", contentType || "application/octet-stream");
    res.setHeader("Content-Disposition", `attachment; filename="${req.params.filename}"`);
    body.pipe(res);
  } catch (err) {
    if (err.name === "NoSuchKey" || err.$metadata?.httpStatusCode === 404) {
      return res.status(404).json({ error: "Attachment not found" });
    }
    next(err);
  }
});

// Delete an attachment
router.delete("/:taskId/attachments/:filename", async (req, res, next) => {
  try {
    await ozoneClient.deleteAttachment(req.params.taskId, req.params.filename);
    res.status(204).end();
  } catch (err) {
    next(err);
  }
});

module.exports = router;
