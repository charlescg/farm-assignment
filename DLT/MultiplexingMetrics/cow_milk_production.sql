
CREATE OR REFRESH STREAMING LIVE TABLE dev_farm.slv_sensor.cow_milk_production(
      CONSTRAINT expected_or_drop EXPECT (value is null)
)
AS 
select json_payload.cow_id, 
      time_serie_event,
      json_payload.sensor_id,
      json_payload.value as value,
      json_payload.unit as unit     
from (
select id,
        time_serie_event,
        from_json(cast(payload as string),'STRUCT<cow_id: STRING, sensor_id: STRING, sensor_type: STRING, unit: STRING, value: DOUBLE>') as json_payload
from stream dev_farm.brz_sensor.cow_measurements )
where json_payload.sensor_type = 'milk_production'
