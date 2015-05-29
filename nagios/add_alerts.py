import sys
from commands import *

class Alerts:	  
          def __init__(self, alert_name, alert_condition, box_granularity, max_boxes, critical, warning):                     
                     self.critical = critical
                     self.alert_name = alert_name
                     self.alert_condition = alert_condition
                     self.warning = warning
                     self.box_granularity = box_granularity
                     self.max_boxes = max_boxes                  
          
          def set_topic_alert(self):
                     alert_name = self.alert_name
                     granularity = self.box_granularity
                     alert_condition = self.alert_condition
                     max_boxes = self.max_boxes
                     critical = self.critical
                     warning = self.warning
                     start = 0
                     for i in range(1, max_boxes):
                        if(i % granularity == 1):
                           start = i
                           end = start + (granularity - 1)
                           if(end > max_boxes):
                              end = max_boxes
                           self.create_file(alert_name, alert_condition, start, end, critical, warning)

          def create_file(self, alert_name, alert_condition, start, end, critical, warning):
                     file_name = alert_name + "_" + str(start) + "_" + str(end)
                     fptr = open("./thresholds/" + file_name, "w")
                     box_name = "kloak{"
                     for i in range(start, end):
                         if (i>=1 and i<=9):
                            box_name = box_name + "0" + str(i) + ","
                         else:
                            box_name = box_name + str(i) + ","
                     if(end < 10):
                         box_name = box_name + "0" + str(end) + "}" + "*"
                     else:
                         box_name = box_name + str(end) + "}" + "*"
                     alert_condition = alert_condition.replace("boxes", box_name)
                     condition = "graphite.absolute_threshold('" + alert_condition + "'," + "alias=" + "'" + file_name + "'" + "," + "warning_over=" + str(warning) + "," + "critical_over=" + str(critical) + ")"
                     print >> fptr, condition 
                     fptr.close()
								       
alertList = [
          Alerts('cluster_under_replication_partition_0','highestMax(movingAverage(servers.boxes.*.kafka.cluster.Partition.UnderReplicated.*.0.value,"15min"),1)',16,48,0.99,None),
          Alerts('cluster_under_replication_partition_1','highestMax(movingAverage(servers.boxes.*.kafka.cluster.Partition.UnderReplicated.*.1.value,"15min"),1)',16,48,0.99,None),
          Alerts('cluster_under_replication_partition_2','highestMax(movingAverage(servers.boxes.*.kafka.cluster.Partition.UnderReplicated.*.2.value,"15min"),1)',16,48,0.99,None),
          Alerts('cluster_under_replication_partition_3','highestMax(movingAverage(servers.boxes.*.kafka.cluster.Partition.UnderReplicated.*.3.value,"15min"),1)',16,48,0.99,None),
          Alerts('cluster_under_replication_partition_4','highestMax(movingAverage(servers.boxes.*.kafka.cluster.Partition.UnderReplicated.*.4.value,"15min"),1)',16,48,0.99,None),
          Alerts('cluster_under_replication_partition_5','highestMax(movingAverage(servers.boxes.*.kafka.cluster.Partition.UnderReplicated.*.5.value,"15min"),1)',16,48,0.99,None),
          Alerts('cluster_under_replication_partition_6','highestMax(movingAverage(servers.boxes.*.kafka.cluster.Partition.UnderReplicated.*.6.value,"15min"),1)',16,48,0.99,None),
          Alerts('cluster_under_replication_partition_7','highestMax(movingAverage(servers.boxes.*.kafka.cluster.Partition.UnderReplicated.*.7.value,"15min"),1)',16,48,0.99,None),
          Alerts('cluster_under_replication_partition_8','highestMax(movingAverage(servers.boxes.*.kafka.cluster.Partition.UnderReplicated.*.8.value,"15min"),1)',16,48,0.99,None),
          Alerts('cluster_under_replication_partition_9','highestMax(movingAverage(servers.boxes.*.kafka.cluster.Partition.UnderReplicated.*.9.value,"15min"),1)',16,48,0.99,None),
          Alerts('cluster_under_replication_partition_??','highestMax(movingAverage(servers.boxes.*.kafka.cluster.Partition.UnderReplicated.*.??.value,"15min"),1)',16,48,0.99,None),
          Alerts('cluster_under_replication_partition_???','highestMax(movingAverage(servers.boxes.*.kafka.cluster.Partition.UnderReplicated.*.???.value,"15min"),1)',16,48,0.99,None),
         ]

#clear all files

status, text = getstatusoutput("rm -rf /thresholds/cluster_under_replication_partition_*")
print text
if (status != 0):
    print "unable to rm existing files, please debug !... exiting"
    exit(0)

status, text = getstatusoutput("git rm ./thresholds/cluster_under_replication_partition_*")
print text
if (status != 0):
    print "unable to git rm the existing files, please debug !... exiting"
    exit(0)

for alert in alertList:
     alert.set_topic_alert()

status, text = getstatusoutput("git add ./thresholds/cluster_under_replication_partition_*")
if (status !=0):
    print "unable to git add the newly created files"

