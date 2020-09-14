from datetime import datetime
import json

sensor_list = [
    "temperature",  # Celsius degrees, sampled 4x per hour
    "humidity",  # relative humidity, %, sampled 1x per hour
    "pressure",  # barometric pressure, mbar, sampled 1x per hour
    "light",  # lux, sampled 1x per hour, https://en.wikipedia.org/wiki/Lux
    "EAQI",  # sampled 1x per hour, range 0-100, European Air Quality Index, https://airindex.eea.europa.eu/Map/AQI/Viewer/
    "contact",  # door open/close events, randomly occurs, 1 measure == open AND close door, measures the time the door was opened in seconds
]

frequency = {
    "temperature": 4,
    "humidity": 1,
    "light": 1,
    "EAQI": 1,
    "contact": -1,  # random
}


def generate_data(
    date_from: datetime,
    date_to: datetime
) -> list:

    return []


if __name__ == '__main__':
    data = generate_data(
        date_from=datetime(2020, 9, 1, 0, 0, 0),
        date_to=datetime(2020, 9, 2, 0, 0, 0),
    )
    print(json.dumps(data))
