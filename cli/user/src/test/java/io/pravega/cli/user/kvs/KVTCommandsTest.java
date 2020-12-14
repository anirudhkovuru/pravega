/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.user.kvs;

import io.pravega.cli.user.CommandArgs;
import io.pravega.cli.user.TestUtils;
import io.pravega.cli.user.config.InteractiveConfig;
import io.pravega.cli.user.scope.ScopeCommand;
import io.pravega.shared.NameUtils;
import io.pravega.test.integration.demo.ClusterWrapper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

import static io.pravega.cli.user.TestUtils.createPravegaCluster;
import static io.pravega.cli.user.TestUtils.getCLIControllerUri;
import static io.pravega.cli.user.TestUtils.createCLIConfig;

public class KVTCommandsTest extends AuthEnabledKVTCommandsTest {
    static final ClusterWrapper CLUSTER = createPravegaCluster(false, false);
    static final InteractiveConfig CONFIG = createCLIConfig(getCLIControllerUri(CLUSTER.controllerUri()), false, false);

    @BeforeClass
    public static void start() {
        CLUSTER.start();
    }

    @AfterClass
    public static void shutDown() {
        if (CLUSTER != null) {
            CLUSTER.close();
        }
    }

    @Test(timeout = 60000)
    public void testPutAndGetKVT() throws Exception {
        final String scope = "putAndGetKVTable";
        final String table = NameUtils.getScopedStreamName(scope, "kvt1");
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scope), CONFIG);
        new ScopeCommand.Create(commandArgs).execute();
        commandArgs = new CommandArgs(Collections.singletonList(table), CONFIG);
        new KeyValueTableCommand.Create(commandArgs).execute();

        // Exercise puts first.
        String commandResult = TestUtils.executeCommand("kvt put " + table + " key-family-1 key1 value1", CONFIG);
        Assert.assertTrue(commandResult.contains("updated successfully"));
        Assert.assertNotNull(KeyValueTableCommand.Put.descriptor());

        commandResult = TestUtils.executeCommand("kvt put-if " + table + " key-family-1 key1 0:0 value2", CONFIG);
        Assert.assertTrue(commandResult.contains("BadKeyVersionException"));
        Assert.assertNotNull(KeyValueTableCommand.PutIf.descriptor());

        commandResult = TestUtils.executeCommand("kvt put-if-absent " + table + " key-family-1 key2 value1", CONFIG);
        Assert.assertTrue(commandResult.contains("inserted successfully"));
        Assert.assertNotNull(KeyValueTableCommand.PutIfAbsent.descriptor());

        commandResult = TestUtils.executeCommand("kvt put-all " + table + " key-family-1 {[[key3, value2]]}", CONFIG);
        Assert.assertTrue(commandResult.contains("Updated"));
        Assert.assertNotNull(KeyValueTableCommand.PutAll.descriptor());

        commandResult = TestUtils.executeCommand("kvt put-range " + table + " key-family-1 1 2", CONFIG);
        Assert.assertTrue(commandResult.contains("Bulk-updated"));
        Assert.assertNotNull(KeyValueTableCommand.PutRange.descriptor());

        // Exercise list commands.
        commandResult = TestUtils.executeCommand("kvt list-keys " + table + " key-family-1", CONFIG);
        Assert.assertTrue(commandResult.contains("key1"));
        Assert.assertNotNull(KeyValueTableCommand.ListKeys.descriptor());

        commandResult = TestUtils.executeCommand("kvt list-entries " + table + " key-family-1", CONFIG);
        Assert.assertTrue(commandResult.contains("value1"));
        Assert.assertNotNull(KeyValueTableCommand.ListEntries.descriptor());

        // Exercise Get command.
        commandResult = TestUtils.executeCommand("kvt get " + table + " key-family-1 \"{[key1, key2]}\"", CONFIG);
        Assert.assertTrue(commandResult.contains("Get"));
        Assert.assertNotNull(KeyValueTableCommand.Get.descriptor());

        // Exercise Remove commands.
        commandResult = TestUtils.executeCommand("kvt remove " + table + " key-family-1 {[[key1]]}", CONFIG);
        Assert.assertTrue(commandResult.contains("Removed"));
        Assert.assertNotNull(KeyValueTableCommand.Remove.descriptor());
    }

    public static class TLSEnabledKVTCommandsTest extends KVTCommandsTest {
        protected static final ClusterWrapper CLUSTER = createPravegaCluster(false, true);
        static final InteractiveConfig CONFIG = createCLIConfig(getCLIControllerUri(CLUSTER.controllerUri()), false, true);

        @BeforeClass
        public static void start() {
            CLUSTER.start();
        }

        @AfterClass
        public static void shutDown() {
            if (CLUSTER != null) {
                CLUSTER.close();
            }
        }
    }
}
