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
package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import static org.apache.bookkeeper.client.api.BKException.Code.NoSuchLedgerExistsException;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.LedgerOffloader.OffloadHandle;
import org.apache.bookkeeper.mledger.LedgerOffloaderStats;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.JCloudBlobStoreProvider;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration;
import org.apache.bookkeeper.mledger.proto.OffloadContext;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.io.Payload;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

@CustomLog
public class BlobStoreManagedLedgerOffloaderStreamingTest extends BlobStoreManagedLedgerOffloaderBase {
    private TieredStorageConfiguration mockedConfig;
    private static final Random random = new Random();
    private final LedgerOffloaderStats offloaderStats;

    BlobStoreManagedLedgerOffloaderStreamingTest() throws Exception {
        super();
        config = getConfiguration(BUCKET);
        JCloudBlobStoreProvider provider = getBlobStoreProvider();
        assertNotNull(provider);
        provider.validate(config);
        blobStore = provider.getBlobStore(config);
        this.offloaderStats = LedgerOffloaderStats.create(false, false, null, 0);
    }

    private BlobStoreManagedLedgerOffloader getOffloader(Map<String, String> additionalConfig) throws IOException {
        return getOffloader(BUCKET, additionalConfig);
    }

    private BlobStoreManagedLedgerOffloader getOffloader(BlobStore mockedBlobStore,
                                                         Map<String, String> additionalConfig) throws IOException {
        return getOffloader(BUCKET, mockedBlobStore, additionalConfig);
    }

    private BlobStoreManagedLedgerOffloader getOffloader(String bucket, Map<String, String> additionalConfig) throws
            IOException {
        mockedConfig = mock(TieredStorageConfiguration.class, delegatesTo(getConfiguration(bucket, additionalConfig)));
        Mockito.doReturn(blobStore).when(mockedConfig).getBlobStore(); // Use the REAL blobStore
        BlobStoreManagedLedgerOffloader offloader = BlobStoreManagedLedgerOffloader
                .create(mockedConfig, new HashMap<String, String>(), scheduler, scheduler,
                        this.offloaderStats, entryOffsetsCache);
        return offloader;
    }

    private BlobStoreManagedLedgerOffloader getOffloader(String bucket, BlobStore mockedBlobStore,
                                                         Map<String, String> additionalConfig) throws IOException {
        mockedConfig = mock(TieredStorageConfiguration.class, delegatesTo(getConfiguration(bucket, additionalConfig)));
        Mockito.doReturn(mockedBlobStore).when(mockedConfig).getBlobStore();
        BlobStoreManagedLedgerOffloader offloader = BlobStoreManagedLedgerOffloader
                .create(mockedConfig, new HashMap<String, String>(), scheduler, scheduler,
                        this.offloaderStats, entryOffsetsCache);
        return offloader;
    }

    @Test
    public void testHappyCase() throws Exception {
        @Cleanup
        LedgerOffloader offloader = getOffloader(new HashMap<String, String>() {{
            put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES, "1000");
            put(config.getKeys(TieredStorageConfiguration.METADATA_FIELD_MAX_BLOCK_SIZE).get(0), "5242880");
            put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC, "600");
        }});
        ManagedLedger ml = createMockManagedLedger();
        UUID uuid = UUID.randomUUID();
        long beginLedger = 0;
        long beginEntry = 0;
        log.info("Trying to begin offload");
        @Cleanup
        OffloadHandle offloadHandle = offloader
                .streamingOffload(ml, uuid, beginLedger, beginEntry, new HashMap<>()).get();
        //Segment should closed because size in bytes full
        for (int i = 0; i < 10; i++) {
            final byte[] data = new byte[100];
            random.nextBytes(data);
            final OffloadHandle.OfferEntryResult offerEntryResult = offloadHandle
                    .offerEntry(EntryImpl.create(0, i, data));
            log.info().attr("result", offerEntryResult).log("Offer result");
        }
        final LedgerOffloader.OffloadResult offloadResult = offloadHandle.getOffloadResultAsync().get();
        log.info().attr("result", offloadResult).log("Offload result");
    }

    @Test
    public void testIndexPayloadIsRepeatable() throws Exception {
        BlobStore spiedBlobStore = mock(BlobStore.class, delegatesTo(blobStore));
        Map<String, String> additionalConfig = new HashMap<>();
        additionalConfig.put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES, "1000");
        additionalConfig.put(config.getKeys(TieredStorageConfiguration.METADATA_FIELD_MAX_BLOCK_SIZE).get(0),
                "5242880");
        additionalConfig.put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC, "600");
        @Cleanup
        LedgerOffloader offloader = getOffloader(spiedBlobStore, additionalConfig);
        ManagedLedger ml = createMockManagedLedger();
        UUID uuid = UUID.randomUUID();
        @Cleanup
        OffloadHandle offloadHandle = offloader.streamingOffload(ml, uuid, 0, 0, new HashMap<>()).get();
        try {
            for (int i = 0; i < 10; i++) {
                byte[] data = new byte[100];
                random.nextBytes(data);
                Entry entry = EntryImpl.create(0, i, data);
                try {
                    offloadHandle.offerEntry(entry);
                } finally {
                    entry.release();
                }
            }
            offloadHandle.getOffloadResultAsync().get();

            ArgumentCaptor<Blob> indexBlob = ArgumentCaptor.forClass(Blob.class);
            Mockito.verify(spiedBlobStore).putBlob(Mockito.eq(BUCKET), indexBlob.capture());
            assertRepeatablePayload(indexBlob.getValue());
        } finally {
            offloader.deleteOffloaded(uuid, offloader.getOffloadDriverMetadata()).get();
        }
        Assert.assertFalse(blobStore.blobExists(BUCKET, uuid.toString()));
        Assert.assertFalse(blobStore.blobExists(BUCKET, DataBlockUtils.indexBlockOffloadKey(uuid)));
    }

    @Test
    public void testStreamingPartsHaveFixedLengthAndRepeatablePayloads() throws Exception {
        BlobStore spiedBlobStore = mock(BlobStore.class, delegatesTo(blobStore));
        int entrySize = DEFAULT_BLOCK_SIZE - StreamingDataBlockHeaderImpl.getDataStartOffset()
                - BufferedOffloadStream.ENTRY_HEADER_SIZE;
        Map<String, String> additionalConfig = new HashMap<>();
        additionalConfig.put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES,
                Integer.toString(3 * entrySize));
        additionalConfig.put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC, "600");
        @Cleanup
        LedgerOffloader offloader = getOffloader(spiedBlobStore, additionalConfig);
        UUID uuid = UUID.randomUUID();
        @Cleanup
        OffloadHandle offloadHandle = offloader.streamingOffload(createMockManagedLedger(), uuid, 0, 0,
                new HashMap<>()).get();

        for (int i = 0; i < 3; i++) {
            Entry entry = EntryImpl.create(0, i, new byte[entrySize]);
            try {
                assertEquals(offloadHandle.offerEntry(entry), OffloadHandle.OfferEntryResult.SUCCESS);
            } finally {
                entry.release();
            }
        }
        offloadHandle.getOffloadResultAsync().get();

        ArgumentCaptor<Payload> partPayloads = ArgumentCaptor.forClass(Payload.class);
        Mockito.verify(spiedBlobStore, Mockito.times(3))
                .uploadMultipartPart(Mockito.any(), Mockito.anyInt(), partPayloads.capture());
        assertFixedLengthRepeatablePayloads(partPayloads.getAllValues(), 3, DEFAULT_BLOCK_SIZE);
    }

    private static void assertFixedLengthRepeatablePayloads(List<Payload> payloads, int expectedParts,
                                                             int expectedLength) throws IOException {
        assertEquals(payloads.size(), expectedParts);
        for (Payload payload : payloads) {
            Assert.assertTrue(payload.isRepeatable());
            assertEquals(payload.getContentMetadata().getContentLength().longValue(), expectedLength);
            byte[] firstRead;
            byte[] secondRead;
            try (InputStream input = payload.openStream()) {
                firstRead = input.readAllBytes();
            }
            try (InputStream input = payload.openStream()) {
                secondRead = input.readAllBytes();
            }
            assertEquals(firstRead.length, expectedLength);
            assertEquals(secondRead, firstRead);
        }
    }

    private static void assertRepeatablePayload(Blob blob) throws IOException {
        Payload payload = blob.getPayload();
        Assert.assertTrue(payload.isRepeatable());
        byte[] firstRead;
        byte[] secondRead;
        try (InputStream input = payload.openStream()) {
            firstRead = input.readAllBytes();
        }
        try (InputStream input = payload.openStream()) {
            secondRead = input.readAllBytes();
        }
        assertEquals(secondRead, firstRead);
        assertEquals(payload.getContentMetadata().getContentLength().longValue(), firstRead.length);
        assertEquals(blob.getMetadata().getContentMetadata().getContentLength().longValue(), firstRead.length);
    }

    @Test
    public void testReadAndWrite() throws Exception {
        @Cleanup
        LedgerOffloader offloader = getOffloader(new HashMap<String, String>() {{
            put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES,
                    Integer.toString(6 * 1024 * 1024));
            put(config.getKeys(TieredStorageConfiguration.METADATA_FIELD_MAX_BLOCK_SIZE).get(0), "5242880");
            put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC, "600");
        }});
        ManagedLedger ml = createMockManagedLedger();
        UUID uuid = UUID.randomUUID();
        long beginLedger = 0;
        long beginEntry = 0;

        Map<String, String> driverMeta = new HashMap<String, String>() {{
            put(TieredStorageConfiguration.METADATA_FIELD_BUCKET, BUCKET);
        }};
        @Cleanup
        OffloadHandle offloadHandle = offloader
                .streamingOffload(ml, uuid, beginLedger, beginEntry, driverMeta).get();
        final LinkedList<Entry> entries = new LinkedList<>();
        try {
            //Segment should closed because size in bytes full
            for (int i = 0; i < 2; i++) {
                final byte[] data = new byte[3 * 1024 * 1024];
                random.nextBytes(data);
                final EntryImpl entry = EntryImpl.create(0, i, data);
                offloadHandle.offerEntry(entry);
                entries.add(entry);
            }
            final LedgerOffloader.OffloadResult offloadResult = offloadHandle.getOffloadResultAsync().get();
            assertEquals(offloadResult.endLedger, 0);
            assertEquals(offloadResult.endEntry, 1);
            final OffloadContext context = new OffloadContext();
            context.addOffloadSegment()
                    .setUidLsb(uuid.getLeastSignificantBits())
                    .setUidMsb(uuid.getMostSignificantBits())
                    .setComplete(true).setEndEntryId(1);

            try (ReadHandle readHandle = offloader.readOffloaded(0, context, driverMeta).get();
                 LedgerEntries ledgerEntries = readHandle.readAsync(0, 1).get()) {
                for (LedgerEntry ledgerEntry : ledgerEntries) {
                    final EntryImpl storedEntry = (EntryImpl) entries.get((int) ledgerEntry.getEntryId());
                    final byte[] storedData = storedEntry.getData();
                    final byte[] entryBytes = ledgerEntry.getEntryBytes();
                    assertEquals(storedData, entryBytes);
                }
            }
        } finally {
            entries.forEach(Entry::release);
            offloader.deleteOffloaded(uuid, driverMeta).get();
        }
        Assert.assertFalse(blobStore.blobExists(BUCKET, uuid.toString()));
        Assert.assertFalse(blobStore.blobExists(BUCKET, DataBlockUtils.indexBlockOffloadKey(uuid)));
    }

    @Test
    public void testReadAndWriteAcrossLedger() throws Exception {
        @Cleanup
        LedgerOffloader offloader = getOffloader(new HashMap<String, String>() {{
            put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES, "2000");
            put(config.getKeys(TieredStorageConfiguration.METADATA_FIELD_MAX_BLOCK_SIZE).get(0), "5242880");
            put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC, "600");
        }});
        ManagedLedger ml = createMockManagedLedger();
        UUID uuid = UUID.randomUUID();
        long beginLedger = 0;
        long beginEntry = 0;

        Map<String, String> driverMeta = new HashMap<String, String>() {{
            put(TieredStorageConfiguration.METADATA_FIELD_BUCKET, BUCKET);
        }};
        @Cleanup
        OffloadHandle offloadHandle = offloader
                .streamingOffload(ml, uuid, beginLedger, beginEntry, driverMeta).get();

        //Segment should closed because size in bytes full
        final LinkedList<Entry> entries = new LinkedList<>();
        final LinkedList<Entry> ledger2Entries = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            final byte[] data = new byte[100];
            random.nextBytes(data);
            final EntryImpl entry = EntryImpl.create(0, i, data);
            offloadHandle.offerEntry(entry);
            entries.add(entry);
        }
        for (int i = 0; i < 10; i++) {
            final byte[] data = new byte[100];
            random.nextBytes(data);
            final EntryImpl entry = EntryImpl.create(1, i, data);
            offloadHandle.offerEntry(entry);
            ledger2Entries.add(entry);
        }

        final LedgerOffloader.OffloadResult offloadResult = offloadHandle.getOffloadResultAsync().get();
        assertEquals(offloadResult.endLedger, 1);
        assertEquals(offloadResult.endEntry, 9);
        final OffloadContext context = new OffloadContext();
        context.addOffloadSegment()
                .setUidLsb(uuid.getLeastSignificantBits())
                .setUidMsb(uuid.getMostSignificantBits())
                .setComplete(true).setEndEntryId(9);

        @Cleanup
        final ReadHandle readHandle = offloader.readOffloaded(0, context, driverMeta).get();
        @Cleanup
        final LedgerEntries ledgerEntries = readHandle.readAsync(0, 9).get();

        for (LedgerEntry ledgerEntry : ledgerEntries) {
            final EntryImpl storedEntry = (EntryImpl) entries.get((int) ledgerEntry.getEntryId());
            final byte[] storedData = storedEntry.getData();
            final byte[] entryBytes = ledgerEntry.getEntryBytes();
            assertEquals(storedData, entryBytes);
        }

        @Cleanup
        final ReadHandle readHandle2 = offloader.readOffloaded(1, context, driverMeta).get();
        @Cleanup
        final LedgerEntries ledgerEntries2 = readHandle2.readAsync(0, 9).get();

        for (LedgerEntry ledgerEntry : ledgerEntries2) {
            final EntryImpl storedEntry = (EntryImpl) ledger2Entries.get((int) ledgerEntry.getEntryId());
            final byte[] storedData = storedEntry.getData();
            final byte[] entryBytes = ledgerEntry.getEntryBytes();
            assertEquals(storedData, entryBytes);
        }
    }

    @Test
    public void testReadAndWriteAcrossSegment() throws Exception {
        @Cleanup
        LedgerOffloader offloader = getOffloader(new HashMap<String, String>() {{
            put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES, "1000");
            put(config.getKeys(TieredStorageConfiguration.METADATA_FIELD_MAX_BLOCK_SIZE).get(0), "5242880");
            put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC, "600");
        }});
        @Cleanup
        LedgerOffloader offloader2 = getOffloader(new HashMap<String, String>() {{
            put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES, "1000");
            put(config.getKeys(TieredStorageConfiguration.METADATA_FIELD_MAX_BLOCK_SIZE).get(0), "5242880");
            put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC, "600");
        }});
        ManagedLedger ml = createMockManagedLedger();
        UUID uuid = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        long beginLedger = 0;
        long beginEntry = 0;

        Map<String, String> driverMeta = new HashMap<String, String>() {{
            put(TieredStorageConfiguration.METADATA_FIELD_BUCKET, BUCKET);
        }};
        @Cleanup
        OffloadHandle offloadHandle = offloader
                .streamingOffload(ml, uuid, beginLedger, beginEntry, driverMeta).get();

        //Segment should closed because size in bytes full
        final LinkedList<Entry> entries = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            final byte[] data = new byte[100];
            random.nextBytes(data);
            final EntryImpl entry = EntryImpl.create(0, i, data);
            offloadHandle.offerEntry(entry);
            entries.add(entry);
        }

        final LedgerOffloader.OffloadResult offloadResult = offloadHandle.getOffloadResultAsync().get();
        assertEquals(offloadResult.endLedger, 0);
        assertEquals(offloadResult.endEntry, 9);

        //Segment should closed because size in bytes full
        @Cleanup
        OffloadHandle offloadHandle2 = offloader2
                .streamingOffload(ml, uuid2, beginLedger, 10, driverMeta).get();
        for (int i = 0; i < 10; i++) {
            final byte[] data = new byte[100];
            random.nextBytes(data);
            final EntryImpl entry = EntryImpl.create(0, i + 10, data);
            offloadHandle2.offerEntry(entry);
            entries.add(entry);
        }
        final LedgerOffloader.OffloadResult offloadResult2 = offloadHandle2.getOffloadResultAsync().get();
        assertEquals(offloadResult2.endLedger, 0);
        assertEquals(offloadResult2.endEntry, 19);

        final OffloadContext context = new OffloadContext();
        context.addOffloadSegment()
                .setUidLsb(uuid.getLeastSignificantBits())
                .setUidMsb(uuid.getMostSignificantBits())
                .setComplete(true).setEndEntryId(9);
        context.addOffloadSegment()
                .setUidLsb(uuid2.getLeastSignificantBits())
                .setUidMsb(uuid2.getMostSignificantBits())
                .setComplete(true).setEndEntryId(19);

        @Cleanup
        final ReadHandle readHandle = offloader.readOffloaded(0, context, driverMeta).get();
        @Cleanup
        final LedgerEntries ledgerEntries = readHandle.readAsync(0, 19).get();

        for (LedgerEntry ledgerEntry : ledgerEntries) {
            final EntryImpl storedEntry = (EntryImpl) entries.get((int) ledgerEntry.getEntryId());
            final byte[] storedData = storedEntry.getData();
            final byte[] entryBytes = ledgerEntry.getEntryBytes();
            assertEquals(storedData, entryBytes);
        }
    }

    @Test
    public void testRandomRead() throws Exception {
        @Cleanup
        LedgerOffloader offloader = getOffloader(new HashMap<String, String>() {{
            put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES, "1000");
            put(config.getKeys(TieredStorageConfiguration.METADATA_FIELD_MAX_BLOCK_SIZE).get(0), "5242880");
            put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC, "600");
        }});
        LedgerOffloader offloader2 = getOffloader(new HashMap<String, String>() {{
            put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES, "1000");
            put(config.getKeys(TieredStorageConfiguration.METADATA_FIELD_MAX_BLOCK_SIZE).get(0), "5242880");
            put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC, "600");
        }});
        ManagedLedger ml = createMockManagedLedger();
        UUID uuid = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        long beginLedger = 0;
        long beginEntry = 0;

        Map<String, String> driverMeta = new HashMap<String, String>() {{
            put(TieredStorageConfiguration.METADATA_FIELD_BUCKET, BUCKET);
        }};
        @Cleanup
        OffloadHandle offloadHandle = offloader
                .streamingOffload(ml, uuid, beginLedger, beginEntry, driverMeta).get();

        //Segment should closed because size in bytes full
        final LinkedList<Entry> entries = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            final byte[] data = new byte[100];
            random.nextBytes(data);
            final EntryImpl entry = EntryImpl.create(0, i, data);
            offloadHandle.offerEntry(entry);
            entries.add(entry);
        }

        final LedgerOffloader.OffloadResult offloadResult = offloadHandle.getOffloadResultAsync().get();
        assertEquals(offloadResult.endLedger, 0);
        assertEquals(offloadResult.endEntry, 9);

        //Segment should closed because size in bytes full
        @Cleanup
        OffloadHandle offloadHandle2 = offloader2
                .streamingOffload(ml, uuid2, beginLedger, 10, driverMeta).get();
        for (int i = 0; i < 10; i++) {
            final byte[] data = new byte[100];
            random.nextBytes(data);
            final EntryImpl entry = EntryImpl.create(0, i + 10, data);
            offloadHandle2.offerEntry(entry);
            entries.add(entry);
        }
        final LedgerOffloader.OffloadResult offloadResult2 = offloadHandle2.getOffloadResultAsync().get();
        assertEquals(offloadResult2.endLedger, 0);
        assertEquals(offloadResult2.endEntry, 19);

        final OffloadContext context = new OffloadContext();
        context.addOffloadSegment()
                .setUidLsb(uuid.getLeastSignificantBits())
                .setUidMsb(uuid.getMostSignificantBits())
                .setComplete(true).setEndEntryId(9);
        context.addOffloadSegment()
                .setUidLsb(uuid2.getLeastSignificantBits())
                .setUidMsb(uuid2.getMostSignificantBits())
                .setComplete(true).setEndEntryId(19);

        @Cleanup
        final ReadHandle readHandle = offloader.readOffloaded(0, context, driverMeta).get();

        for (int i = 0; i <= 19; i++) {
            Random seed = new Random(0);
            int begin = seed.nextInt(20);
            int end = seed.nextInt(20);
            if (begin >= end) {
                int temp = begin;
                begin = end;
                end = temp;
            }
            @Cleanup
            final LedgerEntries ledgerEntries = readHandle.readAsync(begin, end).get();
            for (LedgerEntry ledgerEntry : ledgerEntries) {
                final EntryImpl storedEntry = (EntryImpl) entries.get((int) ledgerEntry.getEntryId());
                final byte[] storedData = storedEntry.getData();
                final byte[] entryBytes = ledgerEntry.getEntryBytes();
                assertEquals(storedData, entryBytes);
            }
        }
    }

    @Test
    public void testInvalidEntryIds() throws Exception {
        @Cleanup
        LedgerOffloader offloader = getOffloader(new HashMap<String, String>() {{
            put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES, "1000");
            put(config.getKeys(TieredStorageConfiguration.METADATA_FIELD_MAX_BLOCK_SIZE).get(0), "5242880");
            put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC, "600");
        }});
        ManagedLedger ml = createMockManagedLedger();
        UUID uuid = UUID.randomUUID();
        long beginLedger = 0;
        long beginEntry = 0;

        Map<String, String> driverMeta = new HashMap<String, String>() {{
            put(TieredStorageConfiguration.METADATA_FIELD_BUCKET, BUCKET);
        }};
        @Cleanup
        OffloadHandle offloadHandle = offloader
                .streamingOffload(ml, uuid, beginLedger, beginEntry, driverMeta).get();

        //Segment should closed because size in bytes full
        final LinkedList<Entry> entries = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            final byte[] data = new byte[100];
            random.nextBytes(data);
            final EntryImpl entry = EntryImpl.create(0, i, data);
            offloadHandle.offerEntry(entry);
            entries.add(entry);
        }

        final LedgerOffloader.OffloadResult offloadResult = offloadHandle.getOffloadResultAsync().get();
        assertEquals(offloadResult.endLedger, 0);
        assertEquals(offloadResult.endEntry, 9);
        final OffloadContext context = new OffloadContext();
        context.addOffloadSegment()
                .setUidLsb(uuid.getLeastSignificantBits())
                .setUidMsb(uuid.getMostSignificantBits())
                .setComplete(true).setEndEntryId(9);

        @Cleanup
        final ReadHandle readHandle = offloader.readOffloaded(0, context, driverMeta).get();
        try {
            readHandle.read(-1, -1);
            Assert.fail("Shouldn't be able to read anything");
        } catch (Exception e) {
        }

        try {
            readHandle.read(0, 20);
            Assert.fail("Shouldn't be able to read anything");
        } catch (Exception e) {
        }
    }

    @Test
    public void testReadNotExistLedger() throws Exception {
        @Cleanup
        LedgerOffloader offloader = getOffloader(new HashMap<String, String>() {{
            put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES, "1000");
            put(config.getKeys(TieredStorageConfiguration.METADATA_FIELD_MAX_BLOCK_SIZE).get(0), "5242880");
            put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC, "600");
        }});
        ManagedLedger ml = createMockManagedLedger();
        UUID uuid = UUID.randomUUID();
        long beginLedger = 0;
        long beginEntry = 0;

        Map<String, String> driverMeta = new HashMap<String, String>() {{
            put(TieredStorageConfiguration.METADATA_FIELD_BUCKET, BUCKET);
        }};
        @Cleanup
        OffloadHandle offloadHandle = offloader
                .streamingOffload(ml, uuid, beginLedger, beginEntry, driverMeta).get();

        // Segment should closed because size in bytes full
        final LinkedList<Entry> entries = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            final byte[] data = new byte[100];
            random.nextBytes(data);
            final EntryImpl entry = EntryImpl.create(0, i, data);
            offloadHandle.offerEntry(entry);
            entries.add(entry);
        }

        final LedgerOffloader.OffloadResult offloadResult = offloadHandle.getOffloadResultAsync().get();
        assertEquals(offloadResult.endLedger, 0);
        assertEquals(offloadResult.endEntry, 9);
        final OffloadContext context = new OffloadContext();
        context.addOffloadSegment()
                .setUidLsb(uuid.getLeastSignificantBits())
                .setUidMsb(uuid.getMostSignificantBits())
                .setComplete(true).setEndEntryId(9);

        @Cleanup
        final ReadHandle readHandle = offloader.readOffloaded(0, context, driverMeta).get();

        // delete blob(ledger)
        blobStore.removeBlob(BUCKET, uuid.toString());

        try {
            readHandle.read(0, 9);
            fail("Should be read fail");
        } catch (BKException e) {
            assertEquals(e.getCode(), NoSuchLedgerExistsException);
        }
    }
}
