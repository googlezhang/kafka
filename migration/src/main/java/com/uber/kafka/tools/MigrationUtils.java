// Copyright (c) 2015 Uber Technologies, Inc. All rights reserved.
// @author Seung-Yeoul Yang (syyang@uber.com)

package com.uber.kafka.tools;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;

import com.google.common.base.Splitter;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.log4j.Logger;

import scala.collection.Iterator;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

/**
 * Utility methods for Kafka 0.7 to Kafak 0.8 migrator.
 */
public class MigrationUtils {

    private static final String LEAF_KAFKA07_ZK_HOST = "localhost:2182";

    private static final Logger LOGGER = Logger.getLogger(MigrationUtils.class);

    private static final int ZK_CONN_TIMEOUT_MS = 5 * 1000;
    private static final int ZK_SOCKET_TIMEOUT_MS = 30 * 1000;

    private static final char OR_DELIMITER = '|';
    private static final Joiner OR_JOINER = Joiner.on(OR_DELIMITER);
    private static final Splitter OR_SPLITTER = Splitter.on(OR_DELIMITER);

    private static final MigrationUtils INSTANCE = new MigrationUtils();

    private final String hostName;

    // For tests.
    MigrationUtils() {
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public static MigrationUtils get() {
        return INSTANCE;
    }

    public String rewriteTopicWhitelist(String kafka08ZKHosts, String whitelist) {
        return getTopicList(kafka08ZKHosts, whitelist, true);
    }

    public String rewriteTopicBlacklist(String kafka08ZKHosts, String blacklist) {
        return getTopicList(kafka08ZKHosts, blacklist, false);
    }

    public ZkClient newZkClient(String zkServers) {
        return new ZkClient(zkServers, ZK_CONN_TIMEOUT_MS, ZK_SOCKET_TIMEOUT_MS,
            new BytesPushThroughSerializer());
    }

    private String getTopicList(String kafka08ZKHosts, String topicList, boolean isWhitelist) {
        Set<String> topics = Sets.newHashSet(topicList.split("\\|"));
        Set<String> topicsInKafka08 = getAllTopicsInKafka08(kafka08ZKHosts);
        Set<String> filteredTopics = Sets.newTreeSet();
        for (String topic : topicsInKafka08) {
            if (topics.contains(topic) ^ !isWhitelist) {
                filteredTopics.add(topic);
            } else {
                LOGGER.info("Skip migrating topic " + topic);
            }
        }
        return OR_JOINER.join(filteredTopics);
    }

    public Set<String> getAllTopicsInKafka08(String kafka08ZKHosts) {
        ZkClient zkClient = newZkClient(kafka08ZKHosts);
        try {
            Iterator<String> allTopics = ZkUtils.getAllTopics(zkClient).toIterator();
            Set<String> res = Sets.newHashSet();
            while (allTopics.hasNext()) {
                res.add(allTopics.next());
            }
            return res;
        } finally {
            zkClient.close();
        }
    }

    /**
     * There are a few cases when zookeeper ephemeral lock held by high-level consumer is not
     * properly released (the lock specifies topics claimed by the consumer), blocking subsequent
     * consumers from processing topics claimed by the previous consumer. This method releases the
     * locks if the topics for the locks collide with the topics that are processed by the
     * current consumer.
     */
    public void removeUnreleasedConsumerLocks(String consumerGroup, String topicWhitelist) {
        Set<String> whitelistedTopics = Sets.newHashSet(OR_SPLITTER.split(topicWhitelist));

        ZkClient zkClient = newZkClient(LEAF_KAFKA07_ZK_HOST);
        String basePath = String.format("/consumers/%s/ids", consumerGroup);
        boolean didRemove = false;
        try {
            if (!zkClient.exists(basePath)) {
                return;
            }
            List<String> registries = zkClient.getChildren(basePath);
            for (String registry : registries) {
                String registryPath = basePath + "/" + registry;
                // Delete if the registry contains one of the whitelisted topics.
                // Example content: *1*api.created_trips|artemis_staging_sjc1.query.log
                byte[] dataBytes = zkClient.readData(registryPath);
                if (dataBytes == null || dataBytes.length == 0) {
                    continue;
                }
                String data = new String(dataBytes);
                int startIdx = data.lastIndexOf('*');
                if (startIdx < 0 || startIdx == data.length() - 1) {
                    continue;
                }
                Set<String> topics = Sets.newHashSet(OR_SPLITTER.split(data.substring(startIdx + 1)));
                if (!Sets.intersection(whitelistedTopics, topics).isEmpty()) {
                    zkClient.delete(registryPath);
                    LOGGER.info("Deleted lock: " + registryPath);
                    didRemove = true;
                }
            }
        } finally {
            if (!didRemove) {
                LOGGER.info("Didn't find any locks for removal: " + basePath);
            }
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }

    public String getHostName() {
        return hostName;
    }

}
