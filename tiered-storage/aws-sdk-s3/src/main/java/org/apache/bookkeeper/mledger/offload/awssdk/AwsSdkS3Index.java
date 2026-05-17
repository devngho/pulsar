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

import static org.apache.bookkeeper.mledger.offload.OffloadUtils.buildLedgerMetadataFormat;
import static org.apache.bookkeeper.mledger.offload.OffloadUtils.parseLedgerMetadata;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.bookkeeper.client.api.LedgerMetadata;

class AwsSdkS3Index {
    private static final int MAGIC = 0x41575332;
    private static final int VERSION = 1;

    final LedgerMetadata ledgerMetadata;
    final long dataObjectLength;
    final Map<Long, EntryLocation> entries;

    AwsSdkS3Index(LedgerMetadata ledgerMetadata, long dataObjectLength, Map<Long, EntryLocation> entries) {
        this.ledgerMetadata = ledgerMetadata;
        this.dataObjectLength = dataObjectLength;
        this.entries = entries;
    }

    void writeTo(OutputStream outputStream) throws IOException {
        byte[] metadata = buildLedgerMetadataFormat(ledgerMetadata);
        try (DataOutputStream out = new DataOutputStream(outputStream)) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            out.writeLong(dataObjectLength);
            out.writeInt(metadata.length);
            out.write(metadata);
            out.writeInt(entries.size());
            for (Map.Entry<Long, EntryLocation> entry : entries.entrySet()) {
                out.writeLong(entry.getKey());
                out.writeLong(entry.getValue().offset);
                out.writeInt(entry.getValue().length);
            }
        }
    }

    static AwsSdkS3Index readFrom(long ledgerId, InputStream inputStream) throws IOException {
        try (DataInputStream in = new DataInputStream(inputStream)) {
            int magic = in.readInt();
            int version = in.readInt();
            if (magic != MAGIC || version != VERSION) {
                throw new IOException("Invalid AWS SDK S3 offload index header");
            }
            long dataObjectLength = in.readLong();
            byte[] metadata = new byte[in.readInt()];
            in.readFully(metadata);
            int entryCount = in.readInt();
            Map<Long, EntryLocation> entries = new LinkedHashMap<>();
            for (int i = 0; i < entryCount; i++) {
                long entryId = in.readLong();
                entries.put(entryId, new EntryLocation(in.readLong(), in.readInt()));
            }
            return new AwsSdkS3Index(parseLedgerMetadata(ledgerId, metadata), dataObjectLength, entries);
        }
    }

    record EntryLocation(long offset, int length) {}
}
