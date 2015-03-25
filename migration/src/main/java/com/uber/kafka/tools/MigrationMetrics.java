// Copyright (c) 2015 Uber Technologies, Inc. All rights reserved.
// @author Seung-Yeoul Yang (syyang@uber.com)

package com.uber.kafka.tools;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;

public class MigrationMetrics {

    private static final Logger LOGGER = Logger.getLogger(MigrationMetrics.class);

    public static class Builder {
        private int consoleReportIntervalSec = -1;

        private HostAndPort graphiteHostInfo;
        private int graphiteReportPeriodSec = -1;

        private String dataCenterPrefix = null;

        private String graphiteLogNamespace = null;

        public Builder setConsoleReporter(int consoleReportIntervalSec) {
            this. consoleReportIntervalSec = consoleReportIntervalSec;
            return this;
        }

        public Builder setGraphiteReporter(String graphiteHost, int graphiteReportPeriodSec) {
            this.graphiteHostInfo = HostAndPort.fromString(graphiteHost);
            this.graphiteReportPeriodSec = graphiteReportPeriodSec;
            return this;
        }

        public Builder setDataCenterPrefix(String dataCenterPrefix) {
            this.dataCenterPrefix = dataCenterPrefix;
            return this;
        }

        public Builder setGraphiteLogNamespace(String graphiteLogNamespace) {
            this.graphiteLogNamespace = graphiteLogNamespace;
            return this;
        }

        public MigrationMetrics build() {
            if (graphiteReportPeriodSec > 0) {
                if (Strings.isNullOrEmpty(dataCenterPrefix)) {
                    throw new IllegalArgumentException("Invalid data center prefix when graphite " +
                        "stats logging is enabled");
                } else if (Strings.isNullOrEmpty(graphiteLogNamespace)) {
                    throw new IllegalArgumentException("Invalid graphite log namespace when graphite " +
                        "stats logging is enabled");
                }
            }
            return new MigrationMetrics(graphiteHostInfo, graphiteReportPeriodSec,
                consoleReportIntervalSec, dataCenterPrefix, graphiteLogNamespace);
        }
    }

    private final MetricRegistry registry;
    private final GraphiteReporter graphiteReporter;
    private final ConsoleReporter consoleReporter;

    private MigrationMetrics(HostAndPort graphiteHostInfo,
                             int graphiteReportIntervalSec,
                             int consoleReportPeriodSec,
                             String dataCenterPrefix,
                             String graphiteLogNamespace) {
        this.registry = new MetricRegistry();

        if (graphiteReportIntervalSec > 0) {
            // Set up graphite reporter.
            InetSocketAddress graphiteAddr = new InetSocketAddress(
                graphiteHostInfo.getHostText(), graphiteHostInfo.getPort());
            Graphite graphite = new Graphite(graphiteAddr);
            String prefix = String.format("stats.%s.migrator.%s", dataCenterPrefix,
                graphiteLogNamespace);
            graphiteReporter = GraphiteReporter.forRegistry(registry)
                .prefixedWith(prefix)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .build(graphite);
            graphiteReporter.start(graphiteReportIntervalSec, TimeUnit.SECONDS);
            LOGGER.info("Started graphite reporter: " + graphiteHostInfo.toString());
        } else {
            graphiteReporter = null;
        }

        if (consoleReportPeriodSec > 0) {
            // Set up console reporter.
            consoleReporter = ConsoleReporter.forRegistry(registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .build();
            consoleReporter.start(consoleReportPeriodSec, TimeUnit.SECONDS);
            LOGGER.info("Started console reporter");
        } else {
            consoleReporter = null;
        }
    }

    public MetricRegistry getRegistry() {
        return registry;
    }

    public void close() {
        if (consoleReporter != null) {
            consoleReporter.close();
        }
        if (graphiteReporter != null) {
            graphiteReporter.close();
        }
    }

}
