from typing import Union
from datetime import datetime, timedelta
import json
import random

sensor_list = {  # TODO: add typical values to be more realistic
    "temperature": {
        "name": "temperature",
        "comment": "Celsius degrees, sampled 4x per hour",
        "frequency": 4,
        "unit": "celsius",
        "values": [15, 18, 20, 23, 25]
    },
    "humidity": {
        "name": "humidity",
        "comment": "Relative humidity, %, sampled 1x per hour",
        "frequency": 1,
        "unit": "%",
        "values": [20, 30, 50, 100]
    },
    "pressure": {
        "name": "pressure",
        "comment": "barometric pressure, mbar, sampled 1x per hour",
        "frequency": 1,
        "unit": "mbar",
        "values": [100, 120, 150, 230]
    },
    "light": {
        "name": "light",
        "comment": "lux, sampled 1x per hour, https://en.wikipedia.org/wiki/Lux",
        "frequency": 1,
        "unit": "lux",
        "values": [0, 100, 1000]  # it's a function of the time mostly
    },
    "eaqi": {
        "name": "eaqi",
        "comment": "sampled 1x per hour, range 0-100, European Air Quality Index, "
                   "https://airindex.eea.europa.eu/Map/AQI/Viewer/",
        "frequency": 1,
        "unit": "%",
        "values": [5, 10, 20, 30],  # random, but more to the lower
    },
    "contact": {
        "name": "contact",
        "comment": "door open/close events, randomly occurs, 1 measure == open AND close door, "
                   "measures the time the door was opened in seconds",
        "frequency":  -1,  # => random
        "unit": "seconds",
        "values": [30, 60, 120]
    },
}

device_list = [
    {
        "id": "temp1",
        "location": "living_room",
        "sensors": ["temperature"]
    },
    {
        "id": "temp2",
        "location": "master_bedroom",
        "sensors": ["temperature"]
    },
    {
        "id": "temp3",
        "location": "hallway",
        "sensors": ["temperature"]
    },
    {
        "id": "weather1",
        "location": "living_room",
        "sensors": ["humidity", "pressure", "light"]
    },
    {
        "id": "weather2",
        "location": "bathroom",
        "sensors": ["humidity", "pressure", "light"]
    },
    {
        "id": "weather3",
        "location": "master_bedroom",
        "sensors": ["humidity", "pressure", "light"]
    },
    {
        "id": "contact1",
        "location": "balcony",
        "sensors": ["contact"]
    },
    {
        "id": "contact2",
        "location": "hallway",
        "sensors": ["contact"]
    },
    {
        "id": "air1",
        "location": "master_bedroom",
        "sensors": ["eaqi"]
    },
    {
        "id": "air2",
        "location": "living_room",
        "sensors": ["eaqi"]
    },
]


def extract_hours(
        date_from: datetime,
        date_to: datetime
) -> int:
    """
    Extracts the number of hours in the provided time span [from, to)
    Disregards the minutes and seconds.
    """
    delta = date_to - date_from
    return (delta.days * 24) + (delta.seconds // 3600)


def generate_measurement(
    sensor: dict,
    previous_measurement: Union[dict, None]
):
    direction = random.choice([-1, 0, 1])
    measurement = random.choice(sensor["values"])
    if previous_measurement:
        measurement = previous_measurement["measurement"]
    measurement += direction * random.random()
    print(f"sensor: {sensor['name']}, direction: {direction}, measurement: {measurement:.2f}")
    return {
        "measurement": measurement,
        "direction": direction
    }


def generate_data(
    date_from: datetime,
    date_to: datetime,
    sensors: dict,
    devices: list,
) -> list:
    """
    Generates sensor data for the provided time interval
    """
    hours = extract_hours(date_from, date_to)
    starting_hour = date_from.hour
    data = []  # 1 entry == 1 line in the output == device readings per hour
    previous_measurements = {}  # key = id + sensor: `temp01_temperature,
    for hour in range(hours):
        current_hour = (starting_hour + hour) % 24
        # TODO: determine the direction of the next sample: ↑ (1), ↓ (-1) or - (0)
        for device in devices:  # TODO: change this per hour, figure out the dependencies from below
            sensor_data = []
            for sensor in device["sensors"]:
                sensor_unit = sensors[sensor]["unit"]
                sensor_frequency = sensors[sensor]["frequency"]
                measurement_key = f'{device["id"]}_{sensor}'
                for _ in range(sensor_frequency):
                    previous_measurement = previous_measurements[measurement_key] if measurement_key in previous_measurements else None
                    measurement = generate_measurement(sensors[sensor], previous_measurement)
                    print("measurement: ", measurement)
                    previous_measurements[measurement_key] = measurement
                    sensor_data.append({
                        "unit": sensor_unit,
                        "type": sensor,
                        "measurement": measurement["measurement"],
                    })
            data.append({
                "id": device["id"],
                "location": device["location"],
                "data": sensor_data,
                "timestamp": (date_from + timedelta(hours=hour)).timestamp(),
            })
    return data


if __name__ == '__main__':
    data = generate_data(
        date_from=datetime(2020, 9, 1, 1, 0, 0),
        date_to=datetime(2020, 9, 2, 2, 2, 0),
        sensors=sensor_list,
        devices=device_list
    )
    with open("sensor_data.json", "w") as out:
        json.dump(data, out)
