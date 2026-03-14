import {
    S3Client,
    GetObjectCommand,
    ListObjectsV2Command
} from "@aws-sdk/client-s3";
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");
import { createReadStream } from "fs";
import * as crypto from "crypto";
import { DownloadOptions } from "@actions/cache/lib/options";
import { CompressionMethod } from "@actions/cache/lib/internal/constants";
import * as core from "@actions/core";
import * as utils from "@actions/cache/lib/internal/cacheUtils";
import { Upload } from "@aws-sdk/lib-storage";
import { downloadCacheHttpClientConcurrent } from "./downloadUtils";

export interface ArtifactCacheEntry {
    cacheKey?: string;
    scope?: string;
    cacheVersion?: string;
    creationTime?: string;
    archiveLocation?: string;
}

// if executing from RunsOn, unset any existing AWS credential env variables so that we can use the IAM instance profile for credentials
// see unsetCredentials() in https://github.com/aws-actions/configure-aws-credentials/blob/v4.0.2/src/helpers.ts#L44
// Note: we preserve AWS_REGION and AWS_DEFAULT_REGION as they are needed for SDK initialization
if (process.env.RUNS_ON_RUNNER_NAME && process.env.RUNS_ON_RUNNER_NAME !== "") {
    core.debug("RunsOn runner detected — clearing AWS credential env vars to use IAM instance profile");
    delete process.env.AWS_ACCESS_KEY_ID;
    delete process.env.AWS_SECRET_ACCESS_KEY;
    delete process.env.AWS_SESSION_TOKEN;
}

const versionSalt = "1.0";
const bucketName = process.env.RUNS_ON_S3_BUCKET_CACHE;
const endpoint = process.env.RUNS_ON_S3_BUCKET_ENDPOINT;
const region =
    process.env.RUNS_ON_AWS_REGION ||
    process.env.AWS_REGION ||
    process.env.AWS_DEFAULT_REGION;
const forcePathStyle =
    process.env.RUNS_ON_S3_FORCE_PATH_STYLE === "true" ||
    process.env.AWS_S3_FORCE_PATH_STYLE === "true";

const uploadQueueSize = Number(process.env.UPLOAD_QUEUE_SIZE || "4");
const uploadPartSize =
    Number(process.env.UPLOAD_PART_SIZE || "32") * 1024 * 1024;
const downloadQueueSize = Number(process.env.DOWNLOAD_QUEUE_SIZE || "8");
const downloadPartSize =
    Number(process.env.DOWNLOAD_PART_SIZE || "16") * 1024 * 1024;

core.debug(`S3 cache config: bucket=${bucketName}, region=${region}, endpoint=${endpoint || "default"}, forcePathStyle=${forcePathStyle}`);
core.debug(`S3 upload config: partSize=${uploadPartSize / (1024 * 1024)}MB, queueSize=${uploadQueueSize}`);
core.debug(`S3 download config: partSize=${downloadPartSize / (1024 * 1024)}MB, queueSize=${downloadQueueSize}`);

const s3Client = new S3Client({ region, forcePathStyle, endpoint });

export function getCacheVersion(
    paths: string[],
    compressionMethod?: CompressionMethod,
    enableCrossOsArchive = false
): string {
    // don't pass changes upstream
    const components = paths.slice();

    // Add compression method to cache version to restore
    // compressed cache as per compression method
    if (compressionMethod) {
        components.push(compressionMethod);
    }

    // Only check for windows platforms if enableCrossOsArchive is false
    if (process.platform === "win32" && !enableCrossOsArchive) {
        components.push("windows-only");
    }

    // Add salt to cache version to support breaking changes in cache entry
    components.push(versionSalt);

    return crypto
        .createHash("sha256")
        .update(components.join("|"))
        .digest("hex");
}

function getS3Prefix(
    paths: string[],
    { compressionMethod, enableCrossOsArchive }: { compressionMethod?: CompressionMethod; enableCrossOsArchive?: boolean }
): string {
    const repository = process.env.GITHUB_REPOSITORY;
    const version = getCacheVersion(
        paths,
        compressionMethod,
        enableCrossOsArchive
    );

    return ["cache", repository, version].join("/");
}

export async function getCacheEntry(
    keys: string[],
    paths: string[],
    { compressionMethod, enableCrossOsArchive }: { compressionMethod?: CompressionMethod; enableCrossOsArchive?: boolean }
) {
    const cacheEntry: ArtifactCacheEntry = {};
    const s3Prefix = getS3Prefix(paths, { compressionMethod, enableCrossOsArchive });

    core.debug(`S3 cache lookup: bucket=${bucketName}, prefix=${s3Prefix}`);
    core.debug(`S3 restore keys: ${JSON.stringify(keys)}`);

    // Find the most recent key matching one of the restoreKeys prefixes
    for (const restoreKey of keys) {
        const lookupPrefix = [s3Prefix, restoreKey].join("/");
        core.debug(`S3 listing objects with prefix: ${lookupPrefix}`);

        const listObjectsParams = {
            Bucket: bucketName,
            Prefix: lookupPrefix
        };

        try {
            const { Contents = [] } = await s3Client.send(
                new ListObjectsV2Command(listObjectsParams)
            );
            core.debug(`S3 found ${Contents.length} object(s) for key prefix '${restoreKey}'`);

            if (Contents.length > 0) {
                // Sort keys by LastModified time in descending order
                const sortedKeys = Contents.sort(
                    (a, b) => Number(b.LastModified) - Number(a.LastModified)
                );
                const s3Path = sortedKeys[0].Key;
                cacheEntry.cacheKey = s3Path?.replace(`${s3Prefix}/`, "");
                cacheEntry.archiveLocation = `s3://${bucketName}/${s3Path}`;
                core.debug(`S3 cache hit: key='${cacheEntry.cacheKey}', location='${cacheEntry.archiveLocation}'`);
                return cacheEntry;
            }
        } catch (error) {
            core.debug(`S3 error listing objects for prefix '${restoreKey}': ${(error as Error).message}`);
            core.warning(
                `Error listing objects with prefix ${restoreKey} in bucket ${bucketName}: ${(error as Error).message}`
            );
        }
    }

    core.debug("S3 cache miss — no matching objects found for any restore key");
    return cacheEntry; // No keys found
}

export async function downloadCache(
    archiveLocation: string,
    archivePath: string,
    options?: DownloadOptions
): Promise<void> {
    if (!bucketName) {
        throw new Error("Environment variable RUNS_ON_S3_BUCKET_CACHE not set");
    }

    if (!region) {
        throw new Error("Environment variable RUNS_ON_AWS_REGION not set");
    }

    const archiveUrl = new URL(archiveLocation);
    const objectKey = archiveUrl.pathname.slice(1);

    core.debug(`S3 downloading: bucket=${bucketName}, key=${objectKey}`);
    core.debug(`S3 download settings: concurrency=${downloadQueueSize}, partSize=${downloadPartSize / (1024 * 1024)}MB`);

    // Retry logic for download validation failures
    const maxRetries = 3;
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            core.debug(`S3 generating presigned URL (attempt ${attempt}/${maxRetries}, expires in 3600s)`);
            const command = new GetObjectCommand({
                Bucket: bucketName,
                Key: objectKey
            });
            const url = await getSignedUrl(s3Client, command, {
                expiresIn: 3600
            });
            core.debug("S3 presigned URL generated, starting concurrent download");

            await downloadCacheHttpClientConcurrent(url, archivePath, {
                ...options,
                downloadConcurrency: downloadQueueSize,
                concurrentBlobDownloads: true,
                partSize: downloadPartSize
            });

            core.debug(`S3 download complete: ${archivePath}`);
            // If we get here, download succeeded
            return;
        } catch (error) {
            const errorMessage = (error as Error).message;
            lastError = error as Error;

            core.debug(`S3 download attempt ${attempt} failed: ${errorMessage}`);

            // Only retry on validation failures, not on other errors
            if (
                errorMessage.includes("Download validation failed") ||
                errorMessage.includes("Range request not supported") ||
                errorMessage.includes("Content-Range header")
            ) {
                if (attempt < maxRetries) {
                    const delayMs = Math.pow(2, attempt - 1) * 1000; // exponential backoff
                    core.warning(
                        `Download attempt ${attempt} failed: ${errorMessage}. Retrying in ${delayMs}ms...`
                    );
                    await new Promise(resolve => setTimeout(resolve, delayMs));
                    continue;
                }
            }

            // For non-retryable errors or max retries reached, throw the error
            throw error;
        }
    }

    // This should never be reached, but just in case
    throw lastError || new Error("Download failed after all retry attempts");
}

export async function saveCache(
    key: string,
    paths: string[],
    archivePath: string,
    { compressionMethod, enableCrossOsArchive }: { compressionMethod?: CompressionMethod; enableCrossOsArchive?: boolean; cacheSize?: number }
): Promise<void> {
    if (!bucketName) {
        throw new Error("Environment variable RUNS_ON_S3_BUCKET_CACHE not set");
    }

    if (!region) {
        throw new Error("Environment variable RUNS_ON_AWS_REGION not set");
    }

    const s3Prefix = getS3Prefix(paths, {
        compressionMethod,
        enableCrossOsArchive
    });
    const s3Key = `${s3Prefix}/${key}`;

    core.debug(`S3 saving cache: bucket=${bucketName}, key=${s3Key}`);
    core.debug(`S3 upload settings: partSize=${uploadPartSize / (1024 * 1024)}MB, queueSize=${uploadQueueSize}`);

    const multipartUpload = new Upload({
        client: s3Client,
        params: {
            Bucket: bucketName,
            Key: s3Key,
            Body: createReadStream(archivePath)
        },
        // Part size in bytes
        partSize: uploadPartSize,
        // Max concurrency
        queueSize: uploadQueueSize
    });

    // Commit Cache
    const cacheSize = utils.getArchiveFileSizeInBytes(archivePath);
    core.info(
        `Cache Size: ~${Math.round(
            cacheSize / (1024 * 1024)
        )} MB (${cacheSize} B)`
    );

    const totalParts = Math.ceil(cacheSize / uploadPartSize);
    core.info(`Uploading cache from ${archivePath} to ${bucketName}/${s3Key}`);
    core.debug(`S3 multipart upload: ${totalParts} part(s) expected`);

    multipartUpload.on("httpUploadProgress", progress => {
        core.debug(`S3 upload progress: part ${progress.part}/${totalParts}, loaded=${progress.loaded} bytes`);
        core.info(`Uploaded part ${progress.part}/${totalParts}.`);
    });

    await multipartUpload.done();
    core.debug(`S3 multipart upload complete: ${bucketName}/${s3Key}`);
    core.info(`Cache saved successfully.`);
}
