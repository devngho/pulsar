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

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import lombok.CustomLog;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.LedgerOffloaderStats;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.naming.TopicName;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

@CustomLog
class AwsSdkS3ReadHandle implements ReadHandle {
    private final ExecutorService executor;
    private final S3Client s3Client;
    private final String bucket;
    private final String dataKey;
    private final long ledgerId;
    private final AwsSdkS3Index index;
    private final LedgerOffloaderStats offloaderStats;
    private final String topicName;
    private final AtomicReference<CompletableFuture<Void>> closeFuture = new AtomicReference<>();
    private volatile boolean closed;

    AwsSdkS3ReadHandle(ExecutorService executor, S3Client s3Client, String bucket, String dataKey, long ledgerId,
                       AwsSdkS3Index index, LedgerOffloaderStats offloaderStats, String managedLedgerName) {
        this.executor = executor;
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.dataKey = dataKey;
        this.ledgerId = ledgerId;
        this.index = index;
        this.offloaderStats = offloaderStats;
        this.topicName = TopicName.fromPersistenceNamingEncoding(managedLedgerName);
    }

    @Override
    public long getId() {
        return ledgerId;
    }

    @Override
    public LedgerMetadata getLedgerMetadata() {
        return index.ledgerMetadata;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (closeFuture.get() != null || !closeFuture.compareAndSet(null, new CompletableFuture<>())) {
            return closeFuture.get();
        }
        closed = true;
        closeFuture.get().complete(null);
        return closeFuture.get();
    }

    @Override
    public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
        CompletableFuture<LedgerEntries> promise = new CompletableFuture<>();
        executor.execute(() -> {
            List<LedgerEntry> entries = new ArrayList<>();
            try {
                if (closed) {
                    promise.completeExceptionally(new ManagedLedgerException.OffloadReadHandleClosedException());
                    return;
                }
                if (firstEntry > lastEntry || firstEntry < 0 || lastEntry > getLastAddConfirmed()) {
                    promise.completeExceptionally(new BKException.BKIncorrectParameterException());
                    return;
                }
                for (long entryId = firstEntry; entryId <= lastEntry; entryId++) {
                    AwsSdkS3Index.EntryLocation location = index.entries.get(entryId);
                    if (location == null) {
                        throw new BKException.BKUnexpectedConditionException();
                    }
                    long start = System.nanoTime();
                    GetObjectRequest request = GetObjectRequest.builder()
                            .bucket(bucket)
                            .key(dataKey)
                            .range(String.format("bytes=%d-%d", location.offset(), location.offset() + location.length() - 1))
                            .build();
                    try (ResponseInputStream<GetObjectResponse> input = s3Client.getObject(request)) {
                        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(location.length(), location.length());
                        entries.add(LedgerEntryImpl.create(ledgerId, entryId, location.length(), buf));
                        int remaining = location.length();
                        while (remaining > 0) {
                            remaining -= buf.writeBytes(input, remaining);
                        }
                    }
                    offloaderStats.recordReadOffloadDataLatency(topicName, System.nanoTime() - start,
                            java.util.concurrent.TimeUnit.NANOSECONDS);
                    offloaderStats.recordReadOffloadBytes(topicName, location.length());
                }
                promise.complete(LedgerEntriesImpl.create(entries));
            } catch (Throwable t) {
                offloaderStats.recordReadOffloadError(topicName);
                entries.forEach(LedgerEntry::close);
                promise.completeExceptionally(t);
            }
        });
        return promise;
    }

    @Override
    public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
        return readAsync(firstEntry, lastEntry);
    }

    @Override
    public CompletableFuture<Long> readLastAddConfirmedAsync() {
        return CompletableFuture.completedFuture(getLastAddConfirmed());
    }

    @Override
    public CompletableFuture<Long> tryReadLastAddConfirmedAsync() {
        return CompletableFuture.completedFuture(getLastAddConfirmed());
    }

    @Override
    public long getLastAddConfirmed() {
        return getLedgerMetadata().getLastEntryId();
    }

    @Override
    public long getLength() {
        return getLedgerMetadata().getLength();
    }

    @Override
    public boolean isClosed() {
        return getLedgerMetadata().isClosed();
    }

    @Override
    public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntryAsync(long entryId,
                                                                                      long timeOutInMillis,
                                                                                      boolean parallel) {
        CompletableFuture<LastConfirmedAndEntry> promise = new CompletableFuture<>();
        promise.completeExceptionally(new UnsupportedOperationException());
        return promise;
    }
}
