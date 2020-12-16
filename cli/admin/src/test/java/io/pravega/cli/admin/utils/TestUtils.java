/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.utils;

import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.Parser;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.integration.demo.ClusterWrapper;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static io.pravega.shared.NameUtils.getScopedStreamName;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Class to contain convenient utilities for writing test cases.
 */
public final class TestUtils {

    /**
     * Invoke any command and get the result by using a mock PrintStream object (instead of System.out). The returned
     * String is the output written by the Command that can be check in any test.
     *
     * @param inputCommand Command to execute.
     * @param state        Configuration to execute the command.
     * @return             Output of the command.
     * @throws Exception   If a problem occurs.
     */
    public static String executeCommand(String inputCommand, AdminCommandState state) throws Exception {
        Parser.Command pc = Parser.parse(inputCommand);
        CommandArgs args = new CommandArgs(pc.getArgs(), state);
        AdminCommand cmd = AdminCommand.Factory.get(pc.getComponent(), pc.getName(), args);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8)) {
            cmd.setOut(ps);
            cmd.execute();
        }
        return new String(baos.toByteArray(), StandardCharsets.UTF_8);
    }

    /**
     * Returns the relative path to `pravega/config` source directory from cli tests.
     *
     * @return the path
     */
    public static String pathToConfig() {
        return "../../config/";
    }

    /**
     * Creates a local Pravega cluster to test on using {@link ClusterWrapper}.
     *
     * @param authEnabled whether accessing the cluster require authentication or not.
     * @param tlsEnabled whether accessing the cluster require TLS or not.
     * @return A local Pravega cluster
     */
    public static ClusterWrapper createPravegaCluster(boolean authEnabled, boolean tlsEnabled) {
        ClusterWrapper.ClusterWrapperBuilder clusterWrapperBuilder = ClusterWrapper.builder().authEnabled(authEnabled);
        if (tlsEnabled) {
            clusterWrapperBuilder
                    .tlsEnabled(true)
                    .tlsServerCertificatePath(pathToConfig() + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME)
                    .tlsServerKeyPath(pathToConfig() + SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_FILE_NAME)
                    .tlsHostVerificationEnabled(false)
                    .tlsServerKeystorePath(pathToConfig() + SecurityConfigDefaults.TLS_SERVER_KEYSTORE_NAME)
                    .tlsServerKeystorePasswordPath(pathToConfig() + SecurityConfigDefaults.TLS_PASSWORD_FILE_NAME);
        }
        return clusterWrapperBuilder.controllerRestEnabled(true).build();
    }

    /**
     * Creates the admin state with the necessary CLI properties to use during testing.
     *
     * @param controllerRestUri the controller REST URI.
     * @param controllerUri the controller URI.
     * @param zkConnectUri the zookeeper URI.
     * @param containerCount the container count.
     * @param authEnabled whether the cli requires authentication to access the cluster.
     * @param tlsEnabled whether the cli requires TLS to access the cluster.
     */
    @SneakyThrows
    public static AdminCommandState createAdminCLIConfig(String controllerRestUri, String controllerUri, String zkConnectUri,
                                                         int containerCount, boolean authEnabled, boolean tlsEnabled) {
        AdminCommandState state = new AdminCommandState();
        Properties pravegaProperties = new Properties();
        System.out.println("REST URI: " + controllerRestUri);
        pravegaProperties.setProperty("cli.controller.connect.rest.uri", controllerRestUri);
        pravegaProperties.setProperty("cli.controller.connect.grpc.uri", controllerUri);
        pravegaProperties.setProperty("pravegaservice.zk.connect.uri", zkConnectUri);
        pravegaProperties.setProperty("pravegaservice.container.count", Integer.toString(containerCount));
        pravegaProperties.setProperty("cli.controller.connect.channel.auth", Boolean.toString(authEnabled));
        pravegaProperties.setProperty("cli.controller.connect.credentials.username", SecurityConfigDefaults.AUTH_ADMIN_USERNAME);
        pravegaProperties.setProperty("cli.controller.connect.credentials.pwd", SecurityConfigDefaults.AUTH_ADMIN_PASSWORD);
        pravegaProperties.setProperty("cli.controller.connect.channel.tls", Boolean.toString(tlsEnabled));
        pravegaProperties.setProperty("cli.controller.connect.trustStore.location", pathToConfig() + SecurityConfigDefaults.TLS_CA_CERT_FILE_NAME);
        state.getConfigBuilder().include(pravegaProperties);
        return state;
    }

    public static void createScopedStream(ClientConfig clientConfig, String scope, String stream) {
        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        assertNotNull(streamManager);
        boolean isScopeCreated = streamManager.createScope(scope);
        // Check if scope created successfully.
        assertTrue("Failed to create scope", isScopeCreated);

        boolean isStreamCreated = streamManager.createStream(scope, stream, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());
        // Check if stream created successfully.
        assertTrue("Failed to create the stream ", isStreamCreated);
    }

    public static void createReaderGroup(ClientConfig clientConfig, String scope, String stream, String readerGroup) {
        @Cleanup
        val rgManager = ReaderGroupManager.withScope(scope, clientConfig);
        val rgConfig = ReaderGroupConfig.builder().stream(getScopedStreamName(scope, stream)).build();
        rgManager.createReaderGroup(readerGroup, rgConfig);
    }

    public static ClientConfig prepareValidClientConfig(String controllerUri, boolean authEnabled, boolean tlsEnabled) {
        ClientConfig.ClientConfigBuilder clientBuilder = ClientConfig.builder()
                .controllerURI(URI.create(controllerUri));
        if (authEnabled) {
            clientBuilder.credentials(new DefaultCredentials(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD,
                    SecurityConfigDefaults.AUTH_ADMIN_USERNAME));
        }
        if (tlsEnabled) {
            clientBuilder.trustStore(pathToConfig() + SecurityConfigDefaults.TLS_CA_CERT_FILE_NAME)
                    .validateHostName(false);
        }
        return clientBuilder.build();
    }

    public static String getCLIControllerUri(String uri) {
        return uri.replace("tcp://", "").replace("tls://", "");
    }

    public static String getCLIControllerRestUri(String uri) {
        return uri.replace("http://", "").replace("https://", "");
    }
}