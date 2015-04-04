// Copyright (c) 2015 Uber Technologies, Inc. All rights reserved.
// @author Seung-Yeoul Yang (syyang@uber.com)

package com.uber.kafka.tools;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;

import org.I0Itec.zkclient.ZkClient;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Tests for {@link com.uber.kafka.tools.MigrationUtils}
 */
public class MigrationUtilsTest {

    private static final String TEST_CONSUMER_GROUP = "test_consumer_group";
    private static final String TEST_ZK_HOSTS = "localhost:2182";
    private static final Set<String> KAFAK08_TOPICS = ImmutableSet.of("a", "b", "c", "foo_bar");

    private MigrationUtils utils;
    private ZkClient zkClient;

    @Before
    public void setUp() {
        zkClient = mock(ZkClient.class);
        utils = new MigrationUtils() {
            @Override
            public Set<String> getAllTopicsInKafka08(String kafka08ZKHosts) {
                return KAFAK08_TOPICS;
            }
            @Override
            public ZkClient newZkClient(String zkServers) {
                return zkClient;
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

    @Test
    public void testRemoveUnreleasedConsumerLocks() {
        final String TOPIC_WHITELIST = "a|b";
        String basePath = "/consumers/" + TEST_CONSUMER_GROUP + "/ids";

        // Mock zk client
        when(zkClient.exists(basePath)).thenReturn(true);
        List< String > locks = ImmutableList.of("lockA", "lockB");
        when(zkClient.getChildren(basePath)).thenReturn(locks);

        // Set up mock registry files
        byte[] registryA = "*1*a|c".getBytes();
        when(zkClient.readData(basePath + "/lockA")).thenReturn(registryA);
        byte[] registryB = "*1*e|f".getBytes();
        when(zkClient.readData(basePath + "/lockB")).thenReturn(registryB);

        // Remove stale locks
        utils.removeUnreleasedConsumerLocks(TEST_CONSUMER_GROUP, TOPIC_WHITELIST);

        // Should be deleted since topic "a" overlaps.
        verify(zkClient).delete(basePath + "/lockA");
        // Should not be deleted since there are no overlapping topics.
        verify(zkClient, never()).delete(basePath + "/lockB");
    }
}
