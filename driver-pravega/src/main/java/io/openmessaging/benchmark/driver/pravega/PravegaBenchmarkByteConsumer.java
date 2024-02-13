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


import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.pravega.client.ByteStreamClientFactory;
import io.pravega.client.byteStream.ByteStreamReader;
import io.pravega.client.stream.ReinitializationRequiredException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PravegaBenchmarkByteConsumer implements BenchmarkConsumer {
    private static final Logger log = LoggerFactory.getLogger(PravegaBenchmarkByteConsumer.class);

    private final ExecutorService executor;
    private final ByteStreamReader reader;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public PravegaBenchmarkByteConsumer(
            String streamName, ConsumerCallback consumerCallback, ByteStreamClientFactory clientFactory) {
        log.info("PravegaBenchmarkByteConsumer: BEGIN: streamName={}", streamName);
        // Create reader.
        reader = clientFactory.createByteStreamReader(streamName);
        // Start a thread to read events.
        this.executor = Executors.newSingleThreadExecutor();
        this.executor.submit(
                () -> {
                    // We will read data and write it on this buffer.
                    byte[] readData = new byte[1024 * 1024];
                    log.info("PravegaBenchmarkByteConsumer: Instantiated byte buffer");
                    while (!closed.get()) {
                        try {
                            int readBytes = reader.read(readData);
                            if (readBytes > 0) {
                                consumerCallback.messageReceived(
                                        ByteBuffer.wrap(readData, 0, readBytes),
                                        TimeUnit.MICROSECONDS.toMillis(Long.MAX_VALUE));
                            } else {
                                Thread.sleep(10); // Wait for some time to have data available.
                            }
                        } catch (ReinitializationRequiredException e) {
                            log.error("Exception during read", e);
                            throw e;
                        } catch (IOException | InterruptedException e) {
                            log.error("Exception during read", e);
                            throw new RuntimeException(e);
                        }
                    }
                });
    }

    @Override
    public void close() throws Exception {
        closed.set(true);
        this.executor.shutdown();
        this.executor.awaitTermination(1, TimeUnit.MINUTES);
        reader.close();
    }
}
