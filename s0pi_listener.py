# std lib
import json
import time
import datetime
import signal
import os
import argparse
import asyncio
import threading

# pip
import gpiozero # gpiozero
from gpiozero.pins.rpigpio import RPiGPIOFactory # RPi.GPIO
import influxdb # influxdb



_loop = None

def fire_and_forget(coro):
    global _loop
    if _loop is None:
        _loop = asyncio.new_event_loop()
        threading.Thread(target=_loop.run_forever, daemon=True).start()
    _loop.call_soon_threadsafe(asyncio.create_task, coro)

def ensure_write(client, data):
    retry_limit = 32
    retry = 0
    sucess = False
    while retry <= retry_limit and sucess == False:
        try:
            client.write_points(data)
            sucess = True
        except (influxdb.exceptions.InfluxDBClientError, influxdb.exceptions.InfluxDBServerError):
            print(f"writing to db failed! starting retry No. {retry} of {retry_limit}")
            retry += 1
            time.sleep(0.2)

async def s0_async():
    global s0_counter
    global client
    global config
    s0_counter += 1
    print(f"{datetime.datetime.now(datetime.timezone.utc)}\tpulse detected. No: {s0_counter}")
    json_data = [
            {
                "measurement": "generic",
                "tags": {},
                "time": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                "fields": {"pulse_number" : s0_counter, "device_name": config["device_name"]}
            }
    ]
    ensure_write(client, json_data)
def s0_change(ticks, state):
    fire_and_forget(s0_async())
# argparse
parser = argparse.ArgumentParser(description="")
parser.add_argument("-c", "--config", action="store", help="")
args = parser.parse_args()

configfile = "config.listener.json"
if args.config is not None:
    configfile = args.config
starttime = datetime.datetime.now()
# load config
global config
with open(configfile, "r") as f:
    config = json.loads(f.read())
print(f"config(file: '{configfile}'): {config}")

# setup runtime
global s0_counter
global client
s0_counter = 0
client = influxdb.InfluxDBClient(
    host = config["influxdb_host"], 
    database = config["influxdb_dbname"], 
    username = config["influxdb_username"], 
    password = config["influxdb_password"]
)

# setting up pins
factory = RPiGPIOFactory()
dev =  gpiozero.GPIODevice(
    config["s0_pin"],
    pin_factory=factory
)
dev.pin.edges = config["gpio_edges"]
dev.pin.bounce = config["gpio_bounce_in_ms"]/1000
dev.pin.pull = config["gpio_pull"]
dev.pin.when_changed = s0_change
# signal pause() to stay alive
try:
    signal.pause()
except KeyboardInterrupt:
    factory.close()
    print(f"consumed power: {format(float(s0_counter / config['pulse_per_kwh']), '.3f')}kW/h ({s0_counter} pulses)\nstarted: {starttime}\nran: {datetime.datetime.now()-starttime}")
