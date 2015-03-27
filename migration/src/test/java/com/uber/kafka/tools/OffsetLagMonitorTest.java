// Copyright (c) 2015 Uber Technologies, Inc. All rights reserved.
// @author Seung-Yeoul Yang (syyang@uber.com)

package com.uber.kafka.tools;

import com.google.common.collect.ImmutableList;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link com.uber.kafka.tools.OffsetLagMonitor}
 */
public class OffsetLagMonitorTest {

    private static final String TEST_CONSUMER_GROUP = OffsetLagMonitorTest.class.getSimpleName();
    private static final String TEST_TOPIC = "test_topic";

    private MigrationContext context;
    private Kafka7LatestOffsetReader offsetReader;
    private ZkClient zkClient;

    @Before
    public void setUp() {
        context = new MigrationContext();
        offsetReader = mock(Kafka7LatestOffsetReader.class);
        zkClient = mock(ZkClient.class);
    }

    @Test
    public void testBasic() {
        OffsetLagMonitor monitor = new OffsetLagMonitor(
                context, TEST_CONSUMER_GROUP, offsetReader, 1) {
            @Override
            ZkClient getZkClient() {
                return zkClient;
            }
        };

        final long latestOffset = 50L;
        final long currentOffset = 10L;
        final long offsetLag = latestOffset - currentOffset;

        // Mock zookeeper client
        String basePath = String.format("/consumers/%s/offsets", TEST_CONSUMER_GROUP);
        String currentOffsetPath = String.format("%s/%s/0-0", basePath, TEST_TOPIC);
        List<String> children = Collections.singletonList(TEST_TOPIC);
        when(zkClient.getChildren(basePath)).thenReturn(children);
        when(zkClient.readData(currentOffsetPath)).thenReturn(
            Long.toString(currentOffset).getBytes());

        // Mock latest offset reader
        when(offsetReader.getLatestOffset(TEST_TOPIC)).thenReturn(latestOffset);

        // Verify offset lag
        monitor.logOffsetDelta();
        assertEquals(offsetLag, monitor.getOffsetLag(TEST_TOPIC));
    }

}
