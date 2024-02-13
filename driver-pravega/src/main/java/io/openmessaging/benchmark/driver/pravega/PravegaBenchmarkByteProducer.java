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
package io.openmessaging.benchmark.driver.pravega;


import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.pravega.client.ByteStreamClientFactory;
import io.pravega.client.byteStream.ByteStreamWriter;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PravegaBenchmarkByteProducer implements BenchmarkProducer {
    private static final Logger log = LoggerFactory.getLogger(PravegaBenchmarkByteProducer.class);

    private final ByteStreamWriter writer;

    public PravegaBenchmarkByteProducer(String streamName, ByteStreamClientFactory clientFactory) {
        log.info("PravegaBenchmarkByteProducer: BEGIN: streamName={}", streamName);
        writer = clientFactory.createByteStreamWriter(streamName);
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        return writeEvent(key, ByteBuffer.wrap(payload));
    }

    private CompletableFuture<Void> writeEvent(Optional<String> key, ByteBuffer payload) {
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        writer.write(payload);
                    } catch (Exception e) {
                        log.error("Problem while writing bytes.", e);
                    }
                });
    }

    @Override
    public void close() throws Exception {
        writer.close();
    }
}
