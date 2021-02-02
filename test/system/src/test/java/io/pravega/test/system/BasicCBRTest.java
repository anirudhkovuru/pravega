/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.hash.RandomFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class BasicCBRTest extends AbstractReadWriteTest {

    private static final String SCOPE = "testCBRScope" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String STREAM = "testCBRStream" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String READER_GROUP = "testCBRReaderGroup" + RandomFactory.create().nextInt(Integer.MAX_VALUE);

    private static final int MAX = 300;
    private static final int MIN = 30;

    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(4, "executor");
    private URI controllerURI = null;
    private StreamManager streamManager = null;
    private Controller controller = null;

    /**
     * This is used to setup the various services required by the system test framework.
     *
     * @throws MarathonException    when error in setup
     */
    @Environment
    public static void initialize() throws MarathonException {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = ensureControllerRunning(zkUri);
        ensureSegmentStoreRunning(zkUri, controllerUri);
    }

    @Before
    public void setup() {
        Service conService = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = conService.getServiceDetails();
        controllerURI = ctlURIs.get(0);

        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);

        controller = new ControllerImpl(ControllerImplConfig.builder()
                .clientConfig(clientConfig)
                .maxBackoffMillis(5000).build(), executor);
        streamManager = StreamManager.create(clientConfig);

        assertTrue("Creating scope", streamManager.createScope(SCOPE));
        assertTrue("Creating stream", streamManager.createStream(SCOPE, STREAM,
                StreamConfiguration.builder()
                        .scalingPolicy(ScalingPolicy.fixed(1))
                        .retentionPolicy(RetentionPolicy.bySizeBytes(MIN, MAX)).build()));
    }

    @After
    public void tearDown() {
        streamManager.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test
    public void basicCBRTest() throws Exception {
        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);

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
        EventStreamReader<String> reader = clientFactory.createReader(READER_GROUP + "-" + String.valueOf(1),
                READER_GROUP, new JavaSerializer<>(), ReaderConfig.builder().build());

        // Read one event and update the retention stream-cut.
        log.info("Reading event e1 from {}/{}", SCOPE, STREAM);
        reader.readNextEvent(1000);
        log.info("{} generating stream-cuts for {}/{}", READER_GROUP, SCOPE, STREAM);
        Map<Stream, StreamCut> streamCuts = readerGroup.generateStreamCuts(executor).join();
        log.info("{} updating its retention stream-cut to {}", READER_GROUP, streamCuts);
        readerGroup.updateRetentionStreamCut(streamCuts);

        // Wait for the retention to kick in.
        Exceptions.handleInterrupted(() -> Thread.sleep(5 * 60 * 1000));

        // Check to make sure no truncation happened as the min policy is 30 bytes.
        AssertExtensions.assertEventuallyEquals(true, () -> controller.getSegmentsAtTime(
                new StreamImpl(SCOPE, STREAM), 0L).join().values().stream().anyMatch(off -> off == 0),
                120 * 1000L);

        // Write two more events.
        log.info("Writing event e2 to {}/{}", SCOPE, STREAM);
        writer.writeEvent("e2", "data of size 30").join();
        log.info("Writing event e3 to {}/{}", SCOPE, STREAM);
        writer.writeEvent("e3", "data of size 30").join();

        // Give the controller a chance to truncate
        Exceptions.handleInterrupted(() -> Thread.sleep(5 * 60 * 1000));

        // Check to make sure truncation happened after the first event.
        AssertExtensions.assertEventuallyEquals(true, () -> controller.getSegmentsAtTime(
                new StreamImpl(SCOPE, STREAM), 0L).join().values().stream().anyMatch(off -> off >= 30),
                120 * 1000L);

        // Read next event and update the retention stream-cut.
        log.info("Reading event e2 from {}/{}", SCOPE, STREAM);
        reader.readNextEvent(1000);
        log.info("{} generating stream-cuts for {}/{}", READER_GROUP, SCOPE, STREAM);
        Map<Stream, StreamCut> streamCuts2 = readerGroup.generateStreamCuts(executor).join();
        log.info("{} updating its retention stream-cut to {}", READER_GROUP, streamCuts2);
        readerGroup.updateRetentionStreamCut(streamCuts2);

        // Give the controller a chance to truncate
        Exceptions.handleInterrupted(() -> Thread.sleep(5 * 60 * 1000));

        // Check to make sure truncation happened after the second event.
        AssertExtensions.assertEventuallyEquals(true, () -> controller.getSegmentsAtTime(
                new StreamImpl(SCOPE, STREAM), 0L).join().values().stream().anyMatch(off -> off >= 60),
                120 * 1000L);
    }
}
