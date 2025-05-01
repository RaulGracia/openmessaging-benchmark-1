/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.utils.payload;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilePayloadReader implements PayloadReader {
    private static final Logger log = LoggerFactory.getLogger(FilePayloadReader.class);

    private final int expectedLength;
    private static final AtomicReference<byte[]> payload = new AtomicReference<>();
    private static final AtomicInteger currentIndex = new AtomicInteger(0);

    public FilePayloadReader(int expectedLength) {
        log.info("Starting FilePayloadReader with improved payload supplier.");
        this.expectedLength = expectedLength;
    }

    @Override
    public byte[] load(String resourceName) {
        initializePayload(resourceName);
        return getNextPayloadSegment();
    }

    private void initializePayload(String resourceName) {
        payload.updateAndGet(
                existingPayload -> {
                    if (existingPayload == null) {
                        try {
                            return Files.readAllBytes(new File(resourceName).toPath());
                        } catch (IOException e) {
                            throw new PayloadException(e.getMessage());
                        }
                    }
                    return existingPayload;
                });
    }

    private byte[] getNextPayloadSegment() {
        byte[] fullPayload = payload.get();
        int payloadLength = fullPayload.length;
        int startIndex = currentIndex.getAndAdd(expectedLength);

        // Wrap around if the end index exceeds the payload length
        if (startIndex >= payloadLength) {
            startIndex = startIndex % payloadLength;
            currentIndex.set(expectedLength);
        }

        int endIndex = startIndex + expectedLength;
        byte[] result = new byte[expectedLength];

        if (endIndex <= payloadLength) {
            System.arraycopy(fullPayload, startIndex, result, 0, expectedLength);
        } else {
            int firstPartLength = payloadLength - startIndex;
            System.arraycopy(fullPayload, startIndex, result, 0, firstPartLength);
            System.arraycopy(fullPayload, 0, result, firstPartLength, expectedLength - firstPartLength);
            currentIndex.set(expectedLength - firstPartLength);
        }

        return result;
    }

    // Main method for validation
    /*public static void main(String[] args) throws Exception {
        String testFilePath = "/home/raul/Documents/workspace/nexus-tiered-stream-manager/" +
                "openmessaging-benchmark-1/payload/HDFS_100MB.log";
        int expectedLength = 1024;

        FilePayloadReader reader = new FilePayloadReader(expectedLength);

        // Read multiple chunks and print/validate
        byte[] fullPayload = Files.readAllBytes(Paths.get(testFilePath));
        int fullLength = fullPayload.length;

        System.out.println("Full Payload Length: " + fullLength);
        System.out.println("Expected Segment Size: " + expectedLength);

        int iterations = (int) Math.ceil((double) fullLength * 2 / expectedLength); // wraparound check
        int offset = 0;

        for (int i = 0; i < iterations; i++) {
            byte[] segment = reader.load(testFilePath);

            // 1. Validate size
            if (segment.length != expectedLength) {
                throw new RuntimeException("Segment size mismatch at iteration " + i);
            }

            // 2. Validate sequential content with wraparound
            for (int j = 0; j < expectedLength; j++) {
                byte expectedByte = fullPayload[(offset + j) % fullLength];
                if (segment[j] != expectedByte) {
                    throw new RuntimeException(String.format("Data mismatch at iteration %d, byte %d: expected %d,
                    got %d", i, j, expectedByte, segment[j]));
                }
            }

            System.out.printf("Segment %2d OK%n", i);
            offset = (offset + expectedLength) % fullLength;
        }

        System.out.println("All segments verified successfully.");
    }*/
}
