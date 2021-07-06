/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.system;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runner.RunWith;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Slf4j
@RunWith(SystemTestRunner.class)
public class MultiControllerCBRTest extends AbstractSystemTest {

    private static final String SCOPE = "MultiControllerCBRScope" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String STREAM = "MultiControllerCBRStream" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String READER_GROUP = "MultiControllerCBRReaderGroup" + RandomFactory.create().nextInt(Integer.MAX_VALUE);

    private static final int READ_TIMEOUT = 1000;
    private static final int MAX = 300;
    private static final int MIN = 30;

    private final ReaderConfig readerConfig = ReaderConfig.builder().build();
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(4, "executor");
    private final ScheduledExecutorService streamCutExecutor = ExecutorServiceHelpers.newScheduledThreadPool(1, "streamCutExecutor");
    private StreamManager streamManager = null;
    private Controller controller = null;

    private Service controllerService = null;
    private Service segmentStoreService = null;
    private AtomicReference<URI> controllerURIDirect = new AtomicReference<>();

    @Environment
    public static void initialize() throws MarathonException, ExecutionException {
        URI zkUris = startZookeeperInstance();
        startBookkeeperInstances(zkUris);
        URI controllerUri = ensureControllerRunning(zkUris);
        log.info("Controller is currently running at {}", controllerUri);
        Service controllerService = Utils.createPravegaControllerService(zkUris);

        // With Kvs we need segment stores to be running.
        ensureSegmentStoreRunning(zkUris, controllerUri);

        // scale to two controller instances.
        Futures.getAndHandleExceptions(controllerService.scaleService(2), ExecutionException::new);

        List<URI> conUris = controllerService.getServiceDetails();
        log.debug("Pravega Controller service  details: {}", conUris);
    }

    @Before
    public void getControllerInfo() {
        Service zkService = Utils.createZookeeperService();
        Assert.assertTrue(zkService.isRunning());
        List<URI> zkUris = zkService.getServiceDetails();
        log.info("zookeeper service details: {}", zkUris);

        controllerService = Utils.createPravegaControllerService(zkUris.get(0));

        List<URI> conUris = controllerService.getServiceDetails();
        log.debug("Pravega Controller service  details: {}", conUris);
        // Fetch all the RPC endpoints and construct the client URIs.
        final List<String> uris = conUris.stream().filter(ISGRPC).map(URI::getAuthority).collect(Collectors.toList());
        assertEquals("2 controller instances should be running", 2, uris.size());

        // use the last two uris
        controllerURIDirect.set(URI.create((Utils.TLS_AND_AUTH_ENABLED ? TLS : TCP) + String.join(",", uris)));
        log.info("Controller Service direct URI: {}", controllerURIDirect);

        segmentStoreService = Utils.createPravegaSegmentStoreService(zkUris.get(0), controllerService.getServiceDetails().get(0));
    }

    @After
    public void tearDown() {
        ExecutorServiceHelpers.shutdown(executor);
        // The test scales down the controller instances to 0.
        // Scale down the segment store instances to 0 to ensure the next tests start with a clean slate.
        segmentStoreService.scaleService(0);
    }

    public void multiControllerCBRTest() throws Exception {
        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURIDirect.get());

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, clientConfig);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM, new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        // Write a single event.
        log.info("Writing event e1 to {}/{}", SCOPE, STREAM);
        writer.writeEvent("e1", "data of size 30").join();

        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE, clientConfig);
        readerGroupManager.createReaderGroup(READER_GROUP, ReaderGroupConfig.builder()
                .retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT)
                .disableAutomaticCheckpoints()
                .stream(Stream.of(SCOPE, STREAM)).build());
        ReaderGroup readerGroup = readerGroupManager.getReaderGroup(READER_GROUP);
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(READER_GROUP + "-" + 1,
                READER_GROUP, new JavaSerializer<>(), readerConfig);

        // Read one event.
        log.info("Reading event e1 from {}/{}", SCOPE, STREAM);
        EventRead<String> read = reader.readNextEvent(READ_TIMEOUT);
        assertFalse(read.isCheckpoint());
        assertEquals("data of size 30", read.getEvent());

        // Update the retention stream-cut.
        log.info("{} generating stream-cuts for {}/{}", READER_GROUP, SCOPE, STREAM);
        CompletableFuture<Map<Stream, StreamCut>> futureCuts = readerGroup.generateStreamCuts(streamCutExecutor);
        // Wait for 5 seconds to force reader group state update.
        Exceptions.handleInterrupted(() -> TimeUnit.SECONDS.sleep(5));
        EventRead<String> emptyEvent = reader.readNextEvent(100);
    }
}
