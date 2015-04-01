// Copyright (c) 2015 Uber Technologies, Inc. All rights reserved.
// @author Seung-Yeoul Yang (syyang@uber.com)

package com.uber.kafka.tools;

import static org.junit.Assert.assertEquals;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

/**
 * Tests for {@link com.uber.kafka.tools.MigrationUtils}
 */
public class MigrationUtilsTest {

    private static final String TEST_ZK_HOSTS = "localhost:2182";
    private static final Set<String> KAFAK08_TOPICS = ImmutableSet.of("a", "b", "c", "foo_bar");

    private MigrationUtils utils;

    @Before
    public void setUp() {
        utils = new MigrationUtils() {
            @Override
            public Set<String> getAllTopicsInKafka08(String kafka08ZKHosts) {
                return KAFAK08_TOPICS;
            }
        };
    }

    @Test
    public void testRewriteWhitelist() {
        assertEquals("a", utils.rewriteTopicWhitelist(TEST_ZK_HOSTS, "d|a"));
    }

    @Test
    public void testRewriteWhitelistWithDot() {
        assertEquals("a", utils.rewriteTopicWhitelist(TEST_ZK_HOSTS, "d|a|foo.bar"));
    }

    @Test
    public void testRewriteBlacklist() {
        assertEquals("b|c|foo_bar", utils.rewriteTopicBlacklist(TEST_ZK_HOSTS, "a|d"));
    }

}
