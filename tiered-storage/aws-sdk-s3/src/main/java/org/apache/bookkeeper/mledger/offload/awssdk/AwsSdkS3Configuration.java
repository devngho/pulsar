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

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

class AwsSdkS3Configuration {
    static final String DRIVER = "aws-sdk-s3";
    static final String MANAGED_LEDGER_NAME = "ManagedLedgerName";
    static final String METADATA_FORMAT_VERSION_KEY = "S3ManagedLedgerOffloaderFormatVersion";
    static final String CURRENT_VERSION = "aws-sdk-s3-1";

    private static final int MB = 1024 * 1024;

    @Getter
    private final Map<String, String> properties;

    static AwsSdkS3Configuration create(Properties props) {
        return new AwsSdkS3Configuration(props.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString())));
    }

    AwsSdkS3Configuration(Map<String, String> properties) {
        this.properties = properties;
    }

    String bucket() {
        return first("managedLedgerOffloadBucket", "s3ManagedLedgerOffloadBucket");
    }

    String region() {
        return StringUtils.defaultIfBlank(first("managedLedgerOffloadRegion", "s3ManagedLedgerOffloadRegion"),
                "us-east-1");
    }

    String endpoint() {
        return first("managedLedgerOffloadServiceEndpoint", "s3ManagedLedgerOffloadServiceEndpoint");
    }

    String accessKeyId() {
        return first("s3ManagedLedgerOffloadCredentialId", "managedLedgerOffloadCredentialId");
    }

    String secretAccessKey() {
        return first("s3ManagedLedgerOffloadCredentialSecret", "managedLedgerOffloadCredentialSecret");
    }

    int readBufferSizeInBytes() {
        return intValue(MB, "managedLedgerOffloadReadBufferSizeInBytes", "s3ManagedLedgerOffloadReadBufferSizeInBytes");
    }

    int maxBlockSizeInBytes() {
        return intValue(64 * MB, "managedLedgerOffloadMaxBlockSizeInBytes", "s3ManagedLedgerOffloadMaxBlockSizeInBytes");
    }

    Map<String, String> driverMetadata() {
        return Map.of(
                "managedLedgerOffloadDriver", DRIVER,
                "bucket", StringUtils.defaultString(bucket()),
                "region", StringUtils.defaultString(region()),
                "serviceEndpoint", StringUtils.defaultString(endpoint()));
    }

    void validate() {
        if (StringUtils.isBlank(bucket())) {
            throw new IllegalArgumentException("S3 bucket is required");
        }
        if (maxBlockSizeInBytes() < 5 * MB) {
            throw new IllegalArgumentException("S3 multipart part size must be at least 5MB");
        }
    }

    private int intValue(int defaultValue, String... keys) {
        String value = first(keys);
        return StringUtils.isBlank(value) ? defaultValue : Integer.parseInt(value);
    }

    private String first(String... keys) {
        for (String key : keys) {
            String value = properties.get(key);
            if (StringUtils.isNotBlank(value)) {
                return value;
            }
        }
        return null;
    }
}
