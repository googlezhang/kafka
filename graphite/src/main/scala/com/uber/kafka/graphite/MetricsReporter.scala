package com.uber.kafka.graphite

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.{Clock, Gauge, Metric, MetricName, MetricPredicate}
import com.yammer.metrics.reporting.GraphiteReporter
import java.util.concurrent.TimeUnit
import kafka.admin.AdminUtils
import kafka.metrics.{KafkaMetricsConfig, KafkaMetricsReporter, KafkaMetricsReporterMBean}
import kafka.server.KafkaConfig
import kafka.utils.{VerifiableProperties, ZKStringSerializer}
import org.apache.log4j.Logger
import org.I0Itec.zkclient.ZkClient
import scala.concurrent.promise


private object MetricsReporter extends KafkaMetricsReporterMBean {
  
  class Reporter(graphiteHost: String,
                 graphitePort: Int,
                 groupPrefix: String,
                 metricSeparator: Option[Char],
                 pollingPeriodSecs: Long,
                 kafkaConfig: KafkaConfig,
                 zkClient: ZkClient) extends
      GraphiteReporter(Metrics.defaultRegistry,
                       groupPrefix,
                       MetricPredicate.ALL,
                       new GraphiteReporter.DefaultSocketProvider(graphiteHost, graphitePort),
                       Clock.defaultClock) {

    // automatically start polling
    start(pollingPeriodSecs, TimeUnit.SECONDS)

    private var configs = Map.empty[String, Long]

    override def run {
      // Fetch configs for all topics on every polling period.
      configs = AdminUtils.fetchAllTopicConfigs(zkClient)
                          .mapValues(_.getProperty("log.retention.bytes", "-1").toLong)
                          .filter(_._2 > 0)
                          .toMap

      super.run()
    }

    override def processGauge(name: MetricName, gauge: Gauge[_], epoch: java.lang.Long) {
      (name.getGroup, name.getType, name.getName) match {
        case ("kafka.log", "Log", "Size") =>
          // we're only looking for the topic spelling in the metric name
          name.getMBeanName.split(',').foreach(kv => {
            kv.split('=') match {
              case Array("topic", topic) =>
                (gauge.value, configs.get(topic)) match {
                  case (value: Long, Some(logRetentionBytes)) =>
                    sendToGraphite(epoch, sanitizeName(name),
                                   "util " + (value / logRetentionBytes))
                  case (value: Long, None) =>
                    sendToGraphite(epoch, sanitizeName(name),
                                   "util " + (value / kafkaConfig.logRetentionBytes))
                  case _ => logger.warn("Gauge is of wrong type: " + gauge)
                }
              case _ =>
            }
          })
        case _ => // Not a [kafka.log].[Log].[Size] metric
      }

      super.processGauge(name, gauge, epoch)
    }

    override def sanitizeName(name: MetricName): String = {
      // The following rewrites the metric name so that all the additional tags are not lost.
      // NOTE: This is essentially resurrecting the format of kafka 0.8.1
      name.getGroup + '.' + name.getType + '.' + name.getMBeanName.split(',').tail.flatMap(kv => {
        kv.split('=') match {
          case Array(_, v) => metricSeparator.map(c => v.replace('.', c)).orElse(Some(v))
          case _ => {
            logger.warn("Unrecognized key-value format: " + name)
            None
          }
        }
      }).mkString(".")
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

      val kafkaConfig = new KafkaConfig(props.props)
      val zkClient = new ZkClient(kafkaConfig.zkConnect,
                                  kafkaConfig.zkSessionTimeoutMs,
                                  kafkaConfig.zkConnectionTimeoutMs,
                                  ZKStringSerializer)

      Some(new Reporter(graphiteHost, graphitePort, groupPrefix, metricsSeparator,
                        pollingPeriodSecs, kafkaConfig, zkClient))
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
