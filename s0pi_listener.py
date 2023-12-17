# std lib
import json
import time
import datetime
import signal
import os
import argparse

# pip
import gpiozero # gpiozero
from gpiozero.pins.rpigpio import RPiGPIOFactory # RPi.GPIO
import influxdb

def s0_change(ticks, state):
    if state == True:
        global s0_counter
        global client
        s0_counter += 1
        print(f"{datetime.datetime.now()}\tpulse detected. No: {s0_counter}")
        json_data = [
                {
                    "measurement": "generic",
                    "tags": {},
                    "time": None,
                    "fields": {"pulse_number" : s0_counter, "device_name": config["device_name"]}
                }
        ]
        client.write_points(json_data)
# argparse
parser = argparse.ArgumentParser(description="")
parser.add_argument("-c", "--config", action="store", help="")
args = parser.parse_args()

configfile = "config.listener.json"
if args.config is not None:
    configfile = args.config
starttime = datetime.datetime.now()
# load config
with open(configfile, "r") as f:
    config = json.loads(f.read())
print(f"config(file: '{configfile}'): {config}")

# setup runtime
global s0_counter
global client
s0_counter = 0
client = influxdb.InfluxDBClient(host = config["influxdb_host"], database = config["influxdb_dbname"], username = config["influxdb_username"], password = config["influxdb_password"])

# setting up pins
factory = RPiGPIOFactory()
dev =  gpiozero.GPIODevice(config["s0_pin"],
pin_factory=factory
)
dev.pin.edges = "rising"
dev.pin.bounce = 90/1000
dev.pin.when_changed = s0_change
dev.pin.pull = "up"
# signal pause() to stay alive
try:
    signal.pause()
except KeyboardInterrupt:
    factory.close()
    print(f"consumed power: {format(float(s0_counter / config['pulse_per_kwh']), '.3f')}kW/h ({s0_counter} pulses)\nstarted: {starttime}\nran: {datetime.datetime.now()-starttime}")
