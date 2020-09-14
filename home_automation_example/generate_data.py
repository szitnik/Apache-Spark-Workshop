from datetime import datetime, timedelta
import json

sensor_list = {  # TODO: add typical values to be more realistic
    "temperature": {
        "comment": "Celsius degrees, sampled 4x per hour",
        "frequency": 4,
        "unit": "celsius",
    },
    "humidity": {
        "comment": "Relative humidity, %, sampled 1x per hour",
        "frequency": 1,
        "unit": "%",
    },
    "pressure": {
        "comment": "barometric pressure, mbar, sampled 1x per hour",
        "frequency": 1,
        "unit": "mbar",
    },
    "light": {
        "comment": "lux, sampled 1x per hour, https://en.wikipedia.org/wiki/Lux",
        "frequency": 1,
        "unit": "lux",
    },
    "eaqi": {
        "comment": "sampled 1x per hour, range 0-100, European Air Quality Index, "
                   "https://airindex.eea.europa.eu/Map/AQI/Viewer/",
        "frequency": 1,
        "unit": "%",
    },
    "contact": {
        "comment": "door open/close events, randomly occurs, 1 measure == open AND close door, "
                   "measures the time the door was opened in seconds",
        "frequency": -1,  # random
        "unit": "seconds",
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


def generate_data(
        date_from: datetime,
        date_to: datetime,
        sensors,
        devices,
) -> list:
    """
    Generates sensor data for the provided time interval
    :param sensors:
    :param devices:
    """
    hours = extract_hours(date_from, date_to)
    current_hour = date_from.hour
    data = []
    for device in devices: # TODO: change this per hour, figure out the dependencies from below
        sensor_data = []
        for sensor in device["sensors"]:
            # print(device["id"], sensor)
            sensor_unit = sensors[sensor]["unit"]
            sensor_frequency = sensors[sensor]["frequency"]
            for hour in range(hours):
                # print(f"{hour}, {(current_hour + hour) % 24}")
                # TODO: determine the direction of the next sample: ↑ (1), ↓ (-1) or - (0)
                for _ in range(sensor_frequency):
                    measurement = 0
                    sensor_data.append({
                        "unit": sensor_unit,
                        "type": sensor,
                        "measurement": measurement
                    })
        data.append({
            "id": device["id"],
            "location": device["location"],
            "data": sensor_data
        })
    print(hours)
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
