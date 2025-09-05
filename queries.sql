select * from race.b_alert_processing_data_detail where (event_id,exp_id) in (select event_id, schedule_id from race.b_user_alert_log where id=3956391)
