import sys

file_ptr = open("./kloak_migrator_alerts","w")
boxes = "sjc1"
env = "kloak_a"
topics = [
	  "api_client_signups",
	  "api_client_transactions",
	  "api_created_trips",
	  "api_db_nearestcab_logs_client",
	  "api_driver_signups",
	  "api_driver_status_change",
	  "api_goldeta_accuracy",
	  "api_lib_trips_trip_updator",
	  "api_promotions_apply",
	  "cerebro_geosurge_recommended_multipliers_v2",
	  "cleopatra_artemis_data_email",
	  "cleopatra_artemis_data_sms",
 	  "cream_transactions",
	  "dispatch_events",
	  "dispatch_events_client_canceled",
	  "dispatch_events_client_eyeballed",
	  "dispatch_events_destination_set",
	  "dispatch_events_driver_accepted",
	  "dispatch_events_driver_arrived",
	  "dispatch_events_driver_canceled",
	  "dispatch_events_driver_dispatched",
	  "dispatch_events_driver_expired",
	  "dispatch_events_driver_rated",
	  "dispatch_events_driver_rejected",
	  "dispatch_events_driver_status_changes",
	  "dispatch_events_fare_split_accepted",
	  "dispatch_events_fare_split_invited",
	  "dispatch_events_pickup_requested",
	  "dispatch_events_ridepool_merged",
	  "dispatch_events_surge_multipliers_received",
	  "dispatch_events_trip_began",
	  "dispatch_events_trip_ended",
	  "dispatch_events_trip_shared",
	  "dispatch_events_trip_unfulfilled",
	  "dispatch_events_trip_sent_to_api",
	  "dispatch_events_user_session",
	  "free-candy_rider_signups",
	  "halyard_events_treatments",
	  "hp_artemis_query",
	  "hp_event_user",
	  "hp_event_user_driver_app",
	  "hp_uberex_metrics",
	  "mobile_events",
	  "money_fraud_creditcard_create_decline",
	  "nagios_notifications",
	  "zendesk_tickets_first_filed",
	  "zendesk_tickets_first_reply",
	  "zendesk_tickets_first_resolved",
         ]

# movingAverage(maxSeries(stats.sjc1.migrator.kloak_a.api_client_signups.kafka_offset_lag.*),"15min")

time = '"15min"'
for topic_name in topics:
	entry = "graphite.relative_threshold('movingAverage(maxSeries(stats." + boxes + ".migrator." + env + "." + topic_name + ".kafka_offset_lag.*)," + time + ")', '-7d', warning_over=4.0, critical_over=7.0)"
        print >> file_ptr, entry

file_ptr.close()
