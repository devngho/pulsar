/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.mledger.offload.awssdk;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import lombok.CustomLog;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.LedgerOffloaderStats;
import org.apache.bookkeeper.mledger.OffloadedLedgerMetadataConsumer;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

@CustomLog
public class AwsSdkS3ManagedLedgerOffloader implements LedgerOffloader {
    private final AwsSdkS3Configuration config;
    private final Map<String, String> userMetadata;
    private final OrderedScheduler scheduler;
    private final OrderedScheduler readExecutor;
    private final LedgerOffloaderStats offloaderStats;
    private final S3Client s3Client;
    private final OffloadPolicies policies;

    AwsSdkS3ManagedLedgerOffloader(AwsSdkS3Configuration config, Map<String, String> userMetadata,
                                   OrderedScheduler scheduler, OrderedScheduler readExecutor,
                                   LedgerOffloaderStats offloaderStats) {
        this.config = config;
        this.userMetadata = userMetadata;
        this.scheduler = scheduler;
        this.readExecutor = readExecutor;
        this.offloaderStats = offloaderStats;
        this.s3Client = createS3Client(config);
        Properties properties = new Properties();
        properties.putAll(config.getProperties());
        this.policies = OffloadPoliciesImpl.create(properties);
    }

    @Override
    public String getOffloadDriverName() {
        return AwsSdkS3Configuration.DRIVER;
    }

    @Override
    public Map<String, String> getOffloadDriverMetadata() {
        return config.driverMetadata();
    }

    @Override
    public CompletableFuture<Void> offload(ReadHandle readHandle, UUID uuid, Map<String, String> extraMetadata) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        scheduler.chooseThread(readHandle.getId()).execute(() -> doOffload(readHandle, uuid, extraMetadata, promise));
        return promise;
    }

    private void doOffload(ReadHandle readHandle, UUID uuid, Map<String, String> extraMetadata,
                           CompletableFuture<Void> promise) {
        String managedLedgerName = extraMetadata.get(AwsSdkS3Configuration.MANAGED_LEDGER_NAME);
        String topicName = TopicName.fromPersistenceNamingEncoding(managedLedgerName);
        if (!readHandle.isClosed() || readHandle.getLastAddConfirmed() < 0) {
            promise.completeExceptionally(new IllegalArgumentException("An empty or open ledger should never be offloaded"));
            return;
        }
        String dataKey = dataKey(readHandle.getId(), uuid);
        String indexKey = indexKey(readHandle.getId(), uuid);
        Path dataFile = null;
        Path indexFile = null;
        try {
            dataFile = Files.createTempFile("pulsar-awssdk-s3-data-", ".tmp");
            indexFile = Files.createTempFile("pulsar-awssdk-s3-index-", ".tmp");
            Map<Long, AwsSdkS3Index.EntryLocation> entries = new LinkedHashMap<>();
            long offset = 0;
            try (OutputStream out = Files.newOutputStream(dataFile)) {
                for (long startEntry = 0; startEntry <= readHandle.getLastAddConfirmed(); ) {
                    long endEntry = Math.min(startEntry + 99, readHandle.getLastAddConfirmed());
                    long readStart = System.nanoTime();
                    try (LedgerEntries ledgerEntries = readHandle.read(startEntry, endEntry)) {
                        offloaderStats.recordReadLedgerLatency(topicName, System.nanoTime() - readStart,
                                TimeUnit.NANOSECONDS);
                        for (LedgerEntry entry : ledgerEntries) {
                            byte[] entryBytes = entry.getEntryBytes();
                            entries.put(entry.getEntryId(), new AwsSdkS3Index.EntryLocation(offset, entryBytes.length));
                            out.write(entryBytes);
                            offset += entryBytes.length;
                            offloaderStats.recordOffloadBytes(topicName, entryBytes.length);
                        }
                    }
                    startEntry = endEntry + 1;
                }
            }
            Map<String, String> metadata = objectMetadata(extraMetadata);
            uploadMultipart(dataKey, dataFile, metadata);
            try (OutputStream out = Files.newOutputStream(indexFile)) {
                new AwsSdkS3Index(readHandle.getLedgerMetadata(), offset, entries).writeTo(out);
            }
            s3Client.putObject(PutObjectRequest.builder().bucket(config.bucket()).key(indexKey)
                            .metadata(metadata).contentType("application/octet-stream").build(),
                    RequestBody.fromFile(indexFile));
            promise.complete(null);
        } catch (Throwable t) {
            offloaderStats.recordWriteToStorageError(topicName);
            offloaderStats.recordOffloadError(topicName);
            promise.completeExceptionally(t);
        } finally {
            deleteTemp(dataFile);
            deleteTemp(indexFile);
        }
    }

    @Override
    public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uid,
                                                       Map<String, String> offloadDriverMetadata) {
        CompletableFuture<ReadHandle> promise = new CompletableFuture<>();
        String managedLedgerName = offloadDriverMetadata.get(AwsSdkS3Configuration.MANAGED_LEDGER_NAME);
        readExecutor.chooseThread(ledgerId).execute(() -> {
            try {
                long start = System.nanoTime();
                GetObjectRequest request = GetObjectRequest.builder().bucket(config.bucket()).key(indexKey(ledgerId, uid)).build();
                try (ResponseInputStream<GetObjectResponse> input = s3Client.getObject(request)) {
                    checkVersion(indexKey(ledgerId, uid), input.response().metadata());
                    offloaderStats.recordReadOffloadIndexLatency(TopicName.fromPersistenceNamingEncoding(managedLedgerName),
                            System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    AwsSdkS3Index index = AwsSdkS3Index.readFrom(ledgerId, input);
                    promise.complete(new AwsSdkS3ReadHandle(readExecutor.chooseThread(ledgerId), s3Client,
                            config.bucket(), dataKey(ledgerId, uid), ledgerId, index, offloaderStats, managedLedgerName));
                }
            } catch (Throwable t) {
                promise.completeExceptionally(t);
            }
        });
        return promise;
    }

    @Override
    public CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uid, Map<String, String> offloadDriverMetadata) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        scheduler.chooseThread(ledgerId).execute(() -> {
            try {
                s3Client.deleteObjects(DeleteObjectsRequest.builder().bucket(config.bucket()).delete(Delete.builder()
                        .objects(ObjectIdentifier.builder().key(dataKey(ledgerId, uid)).build(),
                                ObjectIdentifier.builder().key(indexKey(ledgerId, uid)).build())
                        .build()).build());
                promise.complete(null);
            } catch (Throwable t) {
                promise.completeExceptionally(t);
            }
        });
        return promise;
    }

    @Override
    public OffloadPolicies getOffloadPolicies() {
        return policies;
    }

    @Override
    public void close() {
        s3Client.close();
    }

    @Override
    public void scanLedgers(OffloadedLedgerMetadataConsumer consumer, Map<String, String> offloadDriverMetadata) {
        throw new UnsupportedOperationException("scanLedgers is not implemented for aws-sdk-s3 offloader yet");
    }

    private void uploadMultipart(String key, Path file, Map<String, String> metadata) throws IOException {
        CreateMultipartUploadResponse upload = s3Client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                .bucket(config.bucket()).key(key).metadata(metadata).contentType("application/octet-stream").build());
        List<CompletedPart> parts = new ArrayList<>();
        long size = Files.size(file);
        long partSize = config.maxBlockSizeInBytes();
        try {
            int partNumber = 1;
            for (long offset = 0; offset < size; offset += partSize) {
                long length = Math.min(partSize, size - offset);
                Path partFile = Files.createTempFile("pulsar-awssdk-s3-part-", ".tmp");
                try (InputStream in = Files.newInputStream(file); OutputStream out = Files.newOutputStream(partFile)) {
                    in.skipNBytes(offset);
                    long copied = copyLimited(in, out, length);
                    if (copied != length) {
                        throw new IOException("Failed to copy full multipart part");
                    }
                }
                try {
                    String eTag = s3Client.uploadPart(UploadPartRequest.builder().bucket(config.bucket()).key(key)
                                    .uploadId(upload.uploadId()).partNumber(partNumber).contentLength(length).build(),
                            RequestBody.fromFile(partFile)).eTag();
                    parts.add(CompletedPart.builder().partNumber(partNumber).eTag(eTag).build());
                    partNumber++;
                } finally {
                    deleteTemp(partFile);
                }
            }
            s3Client.completeMultipartUpload(CompleteMultipartUploadRequest.builder().bucket(config.bucket()).key(key)
                    .uploadId(upload.uploadId()).multipartUpload(CompletedMultipartUpload.builder().parts(parts).build())
                    .build());
        } catch (RuntimeException | IOException t) {
            s3Client.abortMultipartUpload(AbortMultipartUploadRequest.builder().bucket(config.bucket()).key(key)
                    .uploadId(upload.uploadId()).build());
            throw t;
        }
    }

    private long copyLimited(InputStream in, OutputStream out, long limit) throws IOException {
        byte[] buffer = new byte[8192];
        long copied = 0;
        while (copied < limit) {
            int read = in.read(buffer, 0, (int) Math.min(buffer.length, limit - copied));
            if (read < 0) {
                break;
            }
            out.write(buffer, 0, read);
            copied += read;
        }
        return copied;
    }

    private Map<String, String> objectMetadata(Map<String, String> extraMetadata) {
        Map<String, String> metadata = new HashMap<>(userMetadata);
        metadata.putAll(extraMetadata);
        metadata.put(AwsSdkS3Configuration.METADATA_FORMAT_VERSION_KEY.toLowerCase(),
                AwsSdkS3Configuration.CURRENT_VERSION);
        return metadata;
    }

    private static S3Client createS3Client(AwsSdkS3Configuration config) {
        S3ClientBuilder builder = S3Client.builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(Region.of(config.region()))
                .credentialsProvider(credentialsProvider(config));
        if (config.endpoint() != null) {
            S3Configuration.Builder serviceConfiguration = S3Configuration.builder().pathStyleAccessEnabled(true);

            if (config.endpoint().contains("r2.cloudflarestorage.com")) {
                serviceConfiguration.chunkedEncodingEnabled(false);
            }

            builder.endpointOverride(URI.create(config.endpoint()))
                    .serviceConfiguration(serviceConfiguration.build());
        }
        return builder.build();
    }

    private static AwsCredentialsProvider credentialsProvider(AwsSdkS3Configuration config) {
        if (config.accessKeyId() != null && config.secretAccessKey() != null) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(config.accessKeyId(),
                    config.secretAccessKey()));
        }
        return DefaultCredentialsProvider.create();
    }

    private static void checkVersion(String key, Map<String, String> metadata) throws IOException {
        String version = metadata.get(AwsSdkS3Configuration.METADATA_FORMAT_VERSION_KEY.toLowerCase());
        if (!AwsSdkS3Configuration.CURRENT_VERSION.equals(version)) {
            throw new IOException(String.format("Invalid object version %s for %s, expect %s",
                    version, key, AwsSdkS3Configuration.CURRENT_VERSION));
        }
    }

    private static String dataKey(long ledgerId, UUID uid) {
        return String.format("%s-ledger-%d", uid, ledgerId);
    }

    private static String indexKey(long ledgerId, UUID uid) {
        return String.format("%s-ledger-%d-index", uid, ledgerId);
    }

    private static void deleteTemp(Path path) {
        if (path != null) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                log.warn().attr("path", path).exception(e).log("Failed to delete temporary offload file");
            }
        }
    }
}
