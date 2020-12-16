/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.controller;

import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.utils.TestUtils;
import io.pravega.client.ClientConfig;
import io.pravega.test.integration.demo.ClusterWrapper;
import lombok.SneakyThrows;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import static io.pravega.cli.admin.utils.TestUtils.createAdminCLIConfig;
import static io.pravega.cli.admin.utils.TestUtils.createPravegaCluster;
import static io.pravega.cli.admin.utils.TestUtils.createReaderGroup;
import static io.pravega.cli.admin.utils.TestUtils.createScopedStream;
import static io.pravega.cli.admin.utils.TestUtils.getCLIControllerRestUri;
import static io.pravega.cli.admin.utils.TestUtils.getCLIControllerUri;
import static io.pravega.cli.admin.utils.TestUtils.prepareValidClientConfig;

public class SecureControllerCommandsTest {
    private static final ClusterWrapper CLUSTER = createPravegaCluster(true, true);
    private static final AdminCommandState STATE;

    // The controller REST URI is generated only after the Pravega cluster has been started. So to maintain STATE as
    // static final, we use this instead of @BeforeClass.
    static {
        CLUSTER.start();
        STATE = createAdminCLIConfig(getCLIControllerRestUri(CLUSTER.controllerRestUri()),
                getCLIControllerUri(CLUSTER.controllerUri()), CLUSTER.zookeeperConnectString(), CLUSTER.getContainerCount(), true, true);
    }

    protected AdminCommandState cliConfig() {
        return STATE;
    }

    protected void createScopeAndStream(String scope, String stream) {
        ClientConfig clientConfig = prepareValidClientConfig(CLUSTER.controllerUri(), true, true);
        // Generate the scope and stream required for testing.
        createScopedStream(clientConfig, scope, stream);
    }

    protected void createScopedReaderGroup(String scope, String stream, String readerGroup) {
        ClientConfig clientConfig = prepareValidClientConfig(CLUSTER.controllerUri(), true, true);
        createReaderGroup(clientConfig, scope, stream, readerGroup);
    }

    @AfterClass
    public static void shutDown() {
        if (CLUSTER != null) {
            CLUSTER.close();
        }
        STATE.close();
    }

    @Test
    @SneakyThrows
    public void testListScopesCommand() {
        String scope = "listScope";
        String stream = "listScopeStream";
        createScopeAndStream(scope, stream);
        String commandResult = TestUtils.executeCommand("controller list-scopes", cliConfig());
        Assert.assertTrue(commandResult.contains(scope));
    }

    @Test
    @SneakyThrows
    public void testListStreamsCommand() {
        String scope = "listStream";
        String stream = "listStreamStream";
        createScopeAndStream(scope, stream);
        String commandResult = TestUtils.executeCommand("controller list-streams " + scope, cliConfig());
        Assert.assertTrue(commandResult.contains(stream));
    }

    @Test
    @SneakyThrows
    public void testListReaderGroupsCommand() {
        String scope = "listReaderGroup";
        String stream = "listReaderGroupStream";
        String readerGroup = "listReaderGroupRG";
        createScopeAndStream(scope, stream);
        createScopedReaderGroup(scope, stream, readerGroup);
        String commandResult = TestUtils.executeCommand("controller list-readergroups " + scope, cliConfig());
        Assert.assertTrue(commandResult.contains(readerGroup));
    }

    @Test
    @SneakyThrows
    public void testDescribeScopeCommand() {
        String scope = "describeScope";
        String stream = "describeScopeStream";
        createScopeAndStream(scope, stream);
        String commandResult = TestUtils.executeCommand("controller describe-scope " + scope, cliConfig());
        Assert.assertTrue(commandResult.contains(scope));
    }

    // TODO: Test controller describe-stream command in the secure scenario (auth+TLS).
    // Cannot at this point due to the following issue:
    // Issue 3821: Create describeStream REST call in Controller
    // https://github.com/pravega/pravega/issues/3821

    // TODO: Test controller describe-readergroup command in the secure scenario (auth+TLS).
    // Cannot at this point due to the following issue:
    // Issue 5196: REST call for fetching readergroup properties does not work when TLS is enabled in the standalone
    // https://github.com/pravega/pravega/issues/5196
}

