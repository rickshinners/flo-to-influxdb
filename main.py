#!/usr/bin/python3

import logging
import os
import sched
import sys
import time
from influxdb import InfluxDBClient

from pyflowater import PyFlo

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

flo_username = os.getenv('FLO_USERNAME', None)
flo_password = os.getenv('FLO_PASSWORD', None)
if (flo_username is None) or (flo_password is None):
    logger.error("Must define env variables FLO_USERNAME and FLO_PASSWORD")
    raise SystemExit
flo = PyFlo(flo_username, flo_password)

scheduler = sched.scheduler(time.time, time.sleep)

flo_poll_seconds = os.getenv("FLO_POLL_SECONDS", 300)

influxdb_client = InfluxDBClient(
    host=os.getenv('INFLUXDB_HOST', 'localhost'),
    port=int(os.getenv('INFLUXDB_PORT', 8086)),
    username=os.getenv('INFLUXDB_USERNAME', None),
    password=os.getenv('INFLUXDB_PASSWORD', None),
    ssl=bool(os.getenv('INFLUXDB_SSL', False)),
    verify_ssl=bool(os.getenv('INFLUXDB_VERIFY_SSL', False))
)
influxdb_client.create_database(os.getenv('INFLUXDB_DATABASE', 'flo'))
influxdb_client.switch_database(os.getenv('INFLUXDB_DATABASE', 'flo'))


def polling_loop():
    points = []
    locations = flo.locations()
    for location in locations:
        for device in location['devices']:
            info = flo.device(device['id'])
            points.append({
                "measurement": "flo_sensor",
                "tags": {
                    "sensor_id": info['id'],
                    "sensor_name": info['nickname']
                },
                "time": info['lastHeardFromTime'],
                "fields": info['telemetry']['current']
            })
    logger.debug("Writing points to influxdb: " + str(points))
    influxdb_client.write_points(points)
    scheduler.enter(flo_poll_seconds, 1, polling_loop)


def main():
    scheduler.enter(0, 1, polling_loop)
    try:
        scheduler.run()
    except KeyboardInterrupt:
        raise SystemExit
    ##TODO: try reconnecting if influx server or flo exception (ConnectionError)


if __name__ == "__main__":
    main()