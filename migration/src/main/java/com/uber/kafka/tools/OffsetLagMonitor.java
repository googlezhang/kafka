// Copyright (c) 2015 Uber Technologies, Inc. All rights reserved.
// @author Seung-Yeoul Yang (syyang@uber.com)

package com.uber.kafka.tools;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class OffsetLagMonitor {

    private static final Logger LOGGER = Logger.getLogger(OffsetLagMonitor.class);

    private static final String LEAF_KAFKA07_ZK_HOST = "localhost:2182";

    private final long INITIAL_DELAY_SEC = 0L;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private final MigrationContext context;
    private final String consumerGroup;
    private final Kafka7LatestOffsetReader offsetReader;
    private final int offsetLagMonitorPeriodSec;
    private final Map<String, Long> offsetLags = Maps.newConcurrentMap();
    private final Set<String> trackedTopics = Sets.newHashSet();

    private class KafkaOffsetLagGauge implements Gauge<Long> {
        private final String topic;

        KafkaOffsetLagGauge(String topic) {
            this.topic = Preconditions.checkNotNull(topic, "Topic can't be null");
        }

        @Override
        public Long getValue() {
            return offsetLags.get(topic);
        }
    }

    public OffsetLagMonitor(MigrationContext context, String consumerGroup,
                            Kafka7LatestOffsetReader offsetReader,
                            int offsetLagMonitorPeriodSec) {
        this.context = Preconditions.checkNotNull(context, "Context can't be null");
        this.consumerGroup = Preconditions.checkNotNull(consumerGroup,
            "Consumer group can't be null");
        this.offsetReader = Preconditions.checkNotNull(offsetReader,
            "Latest offset reader can't be null");
        this.offsetLagMonitorPeriodSec = offsetLagMonitorPeriodSec;
    }

    public long getOffsetLag(String topic) {
        Preconditions.checkArgument(offsetLags.containsKey(topic),
            "Offset lag missing for the topic: " + topic);
        return offsetLags.get(topic);
    }

    // Exposed for tests
    ZkClient getZkClient() {
        return MigrationUtils.get().newZkClient(LEAF_KAFKA07_ZK_HOST);
    }

    // Exposed for tests
    void logOffsetDelta() {
        Map<String, Long> currentOffsets;
        try {
            // Get all topics that are being migrated
            ZkClient zkClient = null;
            try {
                zkClient = getZkClient();
                String basePath = String.format("/consumers/%s/offsets", consumerGroup);
                if (!zkClient.exists(basePath)) {
                    LOGGER.warn("Couldn't find consumer offsets dir: " + basePath);
                    return;
                }
                List<String> topics = zkClient.getChildren(basePath);
                currentOffsets = Maps.newHashMapWithExpectedSize(topics.size());
                for (String topic : topics) {
                    String currentOffsetPath = String.format("%s/%s/0-0", basePath, topic);
                    if (!zkClient.exists(currentOffsetPath)) {
                        LOGGER.warn("Couldn't find consumer offset file: " + currentOffsetPath);
                        continue;
                    }
                    byte[] currentOffsetBytes = zkClient.readData(currentOffsetPath);
                    long currentOffset = Long.parseLong(new String(currentOffsetBytes));
                    currentOffsets.put(topic, currentOffset);
                }
            } finally {
                if (zkClient != null) {
                    zkClient.close();
                }
            }

            try {
                offsetReader.reset();
                for (Map.Entry<String, Long> e : currentOffsets.entrySet()) {
                    final String topic = e.getKey();
                    final long currentOffset = e.getValue();
                    long latestOffset = offsetReader.getLatestOffset(topic);
                    long offsetLag = latestOffset - currentOffset;
                    if (offsetLag < 0) {
                        LOGGER.error("Detected negative offset lag for topic: " + topic +
                            ", lag: " + offsetLag);
                    } else {
                        offsetLags.put(topic, offsetLag);
                    }
                    if (!trackedTopics.contains(topic)) {
                        // Register a new gauge
                        MetricRegistry registry = context.getMetrics().getRegistry();
                        registry.register(MigrationMetrics.nameWithTopic(topic, "kafka_offset_lag"),
                            new KafkaOffsetLagGauge(topic));
                        trackedTopics.add(topic);
                    }
                }
            } finally {
                offsetReader.close();
            }
        } catch (Exception e) {
            LOGGER.error("Unexpected failure when logging offset delta", e);
            context.failed();
        }
    }

    public void start() {
        if (offsetLagMonitorPeriodSec <= 0) {
            LOGGER.info("Offset lag monitor is disabled, skipping start");
            return;
        }
        LOGGER.info("Scheduled offset lag monitor to run every " + offsetLagMonitorPeriodSec +
            " seconds");
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                logOffsetDelta();
            }
        }, INITIAL_DELAY_SEC, offsetLagMonitorPeriodSec, TimeUnit.SECONDS);
    }

    public void shutdown() {
        if (offsetLagMonitorPeriodSec <= 0) {
            LOGGER.info("Offset lag monitor was not started, skipping shutdown");
            return;
        }
        LOGGER.info("Offset lag monitor shutting down...");
        scheduler.shutdown();
        LOGGER.info("Offset lag monitor shutdown complete");
    }

}
