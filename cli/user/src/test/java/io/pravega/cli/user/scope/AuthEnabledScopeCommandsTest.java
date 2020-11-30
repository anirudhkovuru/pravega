/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.user.scope;

import org.junit.Before;

public class AuthEnabledScopeCommandsTest extends ScopeCommandsTest {

    @Before
    @Override
    public void setUp() throws Exception {
        authEnabled = true;
        super.setUp();
    }
}