/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin;

import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.integration.utils.SecureSetupUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.Timeout;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractAdminCommandTest {

    // Setup utility.
    protected static final SecureSetupUtils SETUP_UTILS = new SecureSetupUtils();
    protected static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();

    @Rule
    public final Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);

    @Before
    public void setUp() throws Exception {
        SETUP_UTILS.startAllServices();
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("cli.controller.rest.uri", SETUP_UTILS.getControllerRestUri().toString());
        pravegaProperties.setProperty("cli.controller.grpc.uri", SETUP_UTILS.getControllerUri().toString());
        pravegaProperties.setProperty("pravegaservice.zk.connect.uri", SETUP_UTILS.getZkTestServer().getConnectString());
        pravegaProperties.setProperty("pravegaservice.container.count", "4");
        pravegaProperties.setProperty("cli.security.auth.enable", Boolean.toString(SETUP_UTILS.isAuthEnabled()));
        pravegaProperties.setProperty("cli.security.auth.credentials.username", "admin");
        pravegaProperties.setProperty("cli.security.auth.credentials.password", "1111_aaaa");
        pravegaProperties.setProperty("cli.security.tls.enable", Boolean.toString(SETUP_UTILS.isTlsEnabled()));
        pravegaProperties.setProperty("cli.security.tls.trustStore.location", "../" + SecurityConfigDefaults.TLS_CLIENT_TRUSTSTORE_PATH);
        STATE.get().getConfigBuilder().include(pravegaProperties);
    }

    @After
    public void tearDown() throws Exception {
        SETUP_UTILS.stopAllServices();
        STATE.get().close();
    }

}
