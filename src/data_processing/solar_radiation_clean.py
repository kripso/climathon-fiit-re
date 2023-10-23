from datetime import datetime
import time

TIME_STRING_FORMAT = "%d/%m/%Y, %H:%M:%S"
MODULE_AREA = 1.755 * 1.038  # m^2
MODULE_EFFICIENCY = 0.19
MODULE_COUNT = 8
SECONDS_IN_HOUR = 3600
INTERVAL_LENGTH_IN_SECONDS = 20

def process_solar_radiation_csv():
    print("Processing raw file...")

    mapping = {}  # map the 10 second interval readings to the hour
    with open("climathon-fiit-re/data/raw/solar_radiation.csv", "r") as f:
        next(f)
        for line in f:
            unix_timestamp, watts = line.strip().split(',')
            readable_timestamp = datetime.fromtimestamp(float(unix_timestamp))
            hourly_timestamp = datetime.strftime(readable_timestamp.replace(minute=0, second=0, microsecond=0), TIME_STRING_FORMAT)
            if hourly_timestamp not in mapping.keys():
                mapping[hourly_timestamp] = []
            mapping[hourly_timestamp].append(int(watts))
    
    # calculate average for each hour and save to file
    with open("climathon-fiit-re/data/processed/solar_radiation.csv", "w") as f:
        f.write("timestamp,wats_per_meter\n")
        for hour, watts in mapping.items():
            average = sum(watts)/len(watts)
            timestamp = str(int(time.mktime(datetime.strptime(hour, TIME_STRING_FORMAT).timetuple())))
            f.write(timestamp + ',' + str(average) + '\n')

    # calculate average for each hour and save to file
    with open("climathon-fiit-re/data/processed/solar_radiation_datetimes.csv", "w") as f:
        f.write("timestamp,wats_per_meter\n")
        for hour, watts in mapping.items():
            average = sum(watts)/len(watts)
            f.write(hour + ',' + str(average) + '\n')

    print("Done")

def calculate_watts_per_hour(watts_per_meter_square: float):
    return watts_per_meter_square * SECONDS_IN_HOUR / INTERVAL_LENGTH_IN_SECONDS * MODULE_EFFICIENCY * MODULE_AREA * MODULE_COUNT

def calculate_kilo_watt_hours():
    print('Calculating total kwh...')

    kwh = 0
    with open('climathon-fiit-re/data/processed/solar_radiation.csv') as f:
        next(f)
        
        for line in f:
            timestamp, watts_per_meter = line.strip().split(',')
            kwh += calculate_watts_per_hour(float(watts_per_meter)) / 1000

        print(kwh)

    print("Done")


if __name__ == "__main__":
    process_solar_radiation_csv()
    calculate_kilo_watt_hours()
