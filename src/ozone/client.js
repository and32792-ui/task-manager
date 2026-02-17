const {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
} = require("@aws-sdk/client-s3");
const config = require("../config");

/**
 * Ozone S3-compatible client for file attachments and direct data lake access.
 */
const s3Client = new S3Client({
  endpoint: config.ozone.endpoint,
  region: config.ozone.region,
  credentials: {
    accessKeyId: config.ozone.accessKeyId,
    secretAccessKey: config.ozone.secretAccessKey,
  },
  forcePathStyle: true, // Required for Ozone S3 Gateway
});

const ozoneClient = {
  /**
   * Upload a file attachment for a task.
   * @param {string} taskId - Task UUID
   * @param {string} filename - Original filename
   * @param {Buffer|ReadableStream} body - File content
   * @param {string} contentType - MIME type
   * @returns {Promise<{key: string, bucket: string}>}
   */
  async uploadAttachment(taskId, filename, body, contentType) {
    const key = `${taskId}/${filename}`;
    const bucket = config.ozone.buckets.attachments;

    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: body,
        ContentType: contentType,
      })
    );

    return { bucket, key };
  },

  /**
   * Download a file attachment.
   * @param {string} taskId
   * @param {string} filename
   * @returns {Promise<{body: ReadableStream, contentType: string}>}
   */
  async getAttachment(taskId, filename) {
    const key = `${taskId}/${filename}`;
    const response = await s3Client.send(
      new GetObjectCommand({
        Bucket: config.ozone.buckets.attachments,
        Key: key,
      })
    );

    return {
      body: response.Body,
      contentType: response.ContentType,
    };
  },

  /**
   * Delete a file attachment.
   * @param {string} taskId
   * @param {string} filename
   */
  async deleteAttachment(taskId, filename) {
    const key = `${taskId}/${filename}`;
    await s3Client.send(
      new DeleteObjectCommand({
        Bucket: config.ozone.buckets.attachments,
        Key: key,
      })
    );
  },

  /**
   * List all attachments for a task.
   * @param {string} taskId
   * @returns {Promise<Array<{key: string, size: number, lastModified: Date}>>}
   */
  async listAttachments(taskId) {
    const response = await s3Client.send(
      new ListObjectsV2Command({
        Bucket: config.ozone.buckets.attachments,
        Prefix: `${taskId}/`,
      })
    );

    return (response.Contents || []).map((obj) => ({
      key: obj.Key,
      filename: obj.Key.split("/").pop(),
      size: obj.Size,
      lastModified: obj.LastModified,
    }));
  },

  /**
   * Write raw data to the data lake (for direct API writes if needed).
   * @param {string} bucket - Ozone bucket name
   * @param {string} key - Object key
   * @param {Buffer|string} body - Content
   * @param {string} contentType
   */
  async putObject(bucket, key, body, contentType = "application/octet-stream") {
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: body,
        ContentType: contentType,
      })
    );
  },
};

module.exports = ozoneClient;
