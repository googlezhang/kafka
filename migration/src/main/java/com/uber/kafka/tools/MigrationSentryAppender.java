package com.uber.kafka.tools;

import java.net.InetAddress;
import java.net.UnknownHostException;

import net.kencochrane.raven.Raven;
import net.kencochrane.raven.event.Event;
import net.kencochrane.raven.log4j.SentryAppender;

import org.apache.log4j.MDC;
import org.apache.log4j.spi.LoggingEvent;

/**
 * Adds host name tag to what SentryAppender logs.
 *
 * SentryAppender finds the host name using InetAddress.getLocalHost().getCanonicalHostName()
 * which resolves to "localhost" due to the way /etc/hosts is configured in Kafka leaf hosts.
 */
public class MigrationSentryAppender extends SentryAppender {

    private static final String HOST_NAME = "host_name";

    private final String hostName;

    public MigrationSentryAppender() {
        try {
            this.hostName = InetAddress.getLocalHost().getHostName();
            setExtraTags(HOST_NAME);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public MigrationSentryAppender(Raven raven) {
        this.raven = raven;
        try {
            this.hostName = InetAddress.getLocalHost().getHostName();
            setExtraTags(HOST_NAME);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Event buildEvent(LoggingEvent loggingEvent) {
        MDC.put(HOST_NAME, hostName);
        Event event = super.buildEvent(loggingEvent);
        MDC.remove(HOST_NAME);
        return event;
    }
}
