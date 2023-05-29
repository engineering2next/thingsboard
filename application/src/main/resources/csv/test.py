import logging
import csv
from time import time
from datetime import datetime
from tb_rest_client.rest_client_ce import RestClientCE, EntityId
from tb_rest_client.rest import ApiException
import os

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')



url = "http://localhost:8080"
username = "tenant@thingsboard.org"
password = "tenant"
end_ts = int(time()*1000)
start_ts = end_ts - (1*60*1000)
deviceId = "2a2afb40-fe1d-11ed-aff2-ef6b9f9d6cfe"

# @dataclass
# class FloatValue:
#     ts: int
#     value: float

# @dataclass
# class TimeSerie:
#     key: str
#     start_ts: int
#     end_ts: int
#     values: List[FloatValue] = field(default_factory=list)

# @dataclass
# class TimeSeries:
#     series: List[TimeSerie]
#     start_ts: int
#     end_ts: int

with RestClientCE(url) as rest_client:
    try:
        rest_client.login(username=username, password=password)

        vibrometer_1 = EntityId(deviceId, "DEVICE")

        print(vibrometer_1)

        timeseries_keys = rest_client.get_timeseries_keys_v1(vibrometer_1)

        deviceProfile = rest_client.get_device_by_id(vibrometer_1)

        print(deviceProfile)
        print(int(time()*1000))

        for key in timeseries_keys:

            timeseries = rest_client.get_timeseries(vibrometer_1, key, 0, end_ts)


            # Define the output file name based on the key
            output_file = f'{"application/src/main/resources/csv"}/{deviceId}/{key}.csv'

            directory = os.path.dirname(output_file)
            os.makedirs(directory, exist_ok=True)

            # Extract the keys from the data
            keys = list(timeseries.keys())

            # Write the data to the CSV file
            with open(output_file, 'w', newline='') as csv_file:
                writer = csv.writer(csv_file)

                # Write the header row with keys as column names
                writer.writerow(keys + ['entity_type', 'device_name', 'date'])

                # Write the data rows
                for entry in zip(*[timeseries[key] for key in keys]):
                    timestamp = entry[0]['ts'] / 1000  # Convert to seconds
                    date = datetime.fromtimestamp(timestamp).isoformat()

                    row = [item['value'] for item in entry] + [vibrometer_1.entity_type] + [deviceProfile.name] + [date]
                    writer.writerow(row)

    except ApiException as e:
        logging.exception(e)

