--not the most elegant of solutions, but made it quickly

SELECT a.instrument_id, b.when_timestamp,
	(SELECT TOP 1 gamma FROM value_data WHERE when_timestamp<=DATEADD(second, 5,a.event_timestamp) AND instrument_id=a.instrument_id ORDER BY when_timestamp DESC) as gamma_5s,
	(SELECT TOP 1 gamma FROM value_data WHERE when_timestamp<=DATEADD(minute, 1,a.event_timestamp) AND instrument_id=a.instrument_id ORDER BY when_timestamp DESC) as gamma_1m,
	(SELECT TOP 1 gamma FROM value_data WHERE when_timestamp<=DATEADD(minute, 30,a.event_timestamp) AND instrument_id=a.instrument_id ORDER BY when_timestamp DESC) as gamma_30m,
	(SELECT TOP 1 gamma FROM value_data WHERE when_timestamp<=DATEADD(minute, 60,a.event_timestamp) AND instrument_id=a.instrument_id ORDER BY when_timestamp DESC) as gamma_60m,
	(SELECT TOP 1 theta FROM value_data WHERE when_timestamp<=DATEADD(second, 5,a.event_timestamp) AND instrument_id=a.instrument_id ORDER BY when_timestamp DESC) as theta_5s,
	(SELECT TOP 1 theta FROM value_data WHERE when_timestamp<=DATEADD(minute, 1,a.event_timestamp) AND instrument_id=a.instrument_id ORDER BY when_timestamp DESC) as theta_1m,
	(SELECT TOP 1 theta FROM value_data WHERE when_timestamp<=DATEADD(minute, 30,a.event_timestamp) AND instrument_id=a.instrument_id ORDER BY when_timestamp DESC) as theta_30m,
	(SELECT TOP 1 theta FROM value_data WHERE when_timestamp<=DATEADD(minute, 60,a.event_timestamp) AND instrument_id=a.instrument_id ORDER BY when_timestamp DESC) as theta_60m,
	(SELECT TOP 1 vega FROM value_data WHERE when_timestamp<=DATEADD(second, 5,a.event_timestamp) AND instrument_id=a.instrument_id ORDER BY when_timestamp DESC) as vega_5s,
	(SELECT TOP 1 vega FROM value_data WHERE when_timestamp<=DATEADD(minute, 1,a.event_timestamp) AND instrument_id=a.instrument_id ORDER BY when_timestamp DESC) as vega_1m,
	(SELECT TOP 1 vega FROM value_data WHERE when_timestamp<=DATEADD(minute, 30,a.event_timestamp) AND instrument_id=a.instrument_id ORDER BY when_timestamp DESC) as vega_30m,
	(SELECT TOP 1 vega FROM value_data WHERE when_timestamp<=DATEADD(minute, 60,a.event_timestamp) AND instrument_id=a.instrument_id ORDER BY when_timestamp DESC) as vega_60m
FROM trades a
JOIN value_data b
ON a.instrument_id=b.instrument_id
