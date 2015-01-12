package com.uber.kafka.graphite

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.{Clock, Metric, MetricName}
import com.yammer.metrics.reporting.GraphiteReporter
import java.util.concurrent.TimeUnit
import kafka.metrics.{KafkaMetricsConfig, KafkaMetricsReporter, KafkaMetricsReporterMBean}
import kafka.utils.VerifiableProperties
import org.apache.log4j.Logger
import scala.concurrent.promise


private object MetricsReporter extends KafkaMetricsReporterMBean {
  
  class Reporter(graphiteHost: String,
                 graphitePort: Int,
                 groupPrefix: String,
                 metricSeparator: Option[Char],
                 pollingPeriodSecs: Long) extends
      GraphiteReporter(Metrics.defaultRegistry,
                       groupPrefix,
                       null,
                       new GraphiteReporter.DefaultSocketProvider(graphiteHost, graphitePort),
                       Clock.defaultClock) {

    // automatically start polling
    start(pollingPeriodSecs, TimeUnit.SECONDS)

    // The following is ported from http://grepcode.com/file/repo1.maven.org/maven2/com.yammer.metrics/metrics-graphite/2.2.0/com/yammer/metrics/reporting/GraphiteReporter.java#285
    // with an extra name.getName.replace call to prevent the '.' in metric names from creating
    // unnecessary hierarchy levels in graphite.
    override def sanitizeName(name: MetricName): String = {
      name.getGroup + '.' + name.getType + '.' +
        (if (name.hasScope) name.getScope + '.' else "") +
        metricSeparator.map(c => name.getName.replace('.', c)).getOrElse(name.getName)
    }
  }

  def apply(props: VerifiableProperties) {
    initReporter.success(pollingPeriodSecs => try {
      val graphiteHost = props.getString("kafka.graphite.metrics.host", "localhost")
      val graphitePort = props.getInt("kafka.graphite.metrics.port", 2002)
      val groupPrefix = props.getString("kafka.graphite.metrics.group", "kafka")
      val metricsSeparator = {
        val separator = props.getString("kafka.graphite.metrics.separator", "")
        if (separator != "") {
          Some(separator(0))
        } else None
      }
      Some(new Reporter(graphiteHost, graphitePort, groupPrefix, metricsSeparator,
                        pollingPeriodSecs))
    } catch {
      case e: Throwable => {
        logger.error("Cannot initialize Kafka Graphite metrics reporter: " + e)
        None
      }
    })

    startReporter(new KafkaMetricsConfig(props).pollingIntervalSecs)
  }

  private val logger = Logger.getLogger(getClass)

  // helper with pre-parsed parameters, i.e. host, port, etc.
  private val initReporter = promise[(Long) => Option[Reporter]]

  private var reporter: Option[Reporter] = None

  override val getMBeanName = "kafka:type=com.uber.kafka.graphite.MetricsReporter"

  override def startReporter(pollingPeriodSecs: Long) {
    stopReporter() // just to be safe
    this.synchronized {
      reporter = initReporter.future.value.flatMap(_.get(pollingPeriodSecs))
      logger.info("Started Kafka Graphite metrics reporter polling at " + pollingPeriodSecs)
    }
  }

  override def stopReporter() {
    this.synchronized {
      reporter.map(r => {
        r.shutdown()
        logger.info("Stopped Kafka Graphite metrics reporter")
      })
      reporter = None
    }
  }
}

class MetricsReporter extends KafkaMetricsReporter {
  def init(props: VerifiableProperties) {
    MetricsReporter(props)
  }
}
