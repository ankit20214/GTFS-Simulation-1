import json, re, datetime, time
from google.transit import gtfs_realtime_pb2
import requests
from google.protobuf import json_format
import time
import schedule

#  Virtual Env -> py -3 -m venv file_name

all_entities = []
def load_stops_data():
    stops = open('stops.txt', 'r')
    stops_data = stops.readlines()
    stops.close()
    stop_lat_long_info = {}
    stop_name = {}
    for i in range(1, len(stops_data)):
        stops_data[i] = stops_data[i].strip()
        stops_data[i] = list(stops_data[i].split(','))
        stop_lat_long_info[stops_data[i][0]] = [stops_data[i][3], stops_data[i][4]]
        stop_name[stops_data[i][0]] = stops_data[i][2]

    return stop_lat_long_info, stop_name


def create_json(trip_id, start_time, start_date, route_id, lat_long):
    global all_entities
    vehicle_id = 'DMRC' + str(route_id) + str(trip_id)
    entity = {'id': vehicle_id, 'vehicle': {
        'trip': {'trip_id': trip_id, 'start_time': start_time.strftime("%H:%M:%S"), 'start_date': str(start_date),
                 'route_id': route_id}, 'position': {'latitude': lat_long[0], 'longitude': lat_long[1]},
        'timestamp': int(datetime.datetime.now().timestamp()),
        'vehicle': {'id': vehicle_id, 'label': vehicle_id}}}
    all_entities += [entity]
    m = json.dumps(entity, indent=4)
    # print(re.sub(r'(?<!: )"(\S*?)"', '\\1', m))
    return m


def collect_data():
    stop_times = open('stop_times.txt', 'r')
    stop_times_data = stop_times.readlines()
    stop_times.close()

    stops_lat_long, stop_name = load_stops_data()

    stop_ids = []
    index = []
    for i in range(1, len(stop_times_data)):
        stop_times_data[i] = stop_times_data[i].split(',')
        if len(stop_ids) == 0:
            stop_ids += [stop_times_data[i][0]]
            index += [1]
        else:
            if stop_times_data[i][0] != stop_ids[-1]:
                stop_ids += [stop_times_data[i][0]]
                index += [i]
    index += [len(stop_times_data) - 1]

    trips = open('trips.txt', 'r')
    trips_data = trips.readlines()
    trips.close()
    route_ids = {}
    for i in range(len(trips_data)):
        trips_data[i] = trips_data[i].strip()
        trips_data[i] = trips_data[i].split(',')
        route_ids[trips_data[i][2]] = trips_data[i][0]

    return stop_times_data, stops_lat_long, stop_name, index, route_ids


def write_proto_buffer_data(container):
    file = open("proto_buffer_data.pb",'wb')
    file.write(container.SerializeToString())
    file.close()


def read_proto_buffer_data():

    file = open('proto_buffer_data.pb', 'rb')
    container = gtfs_realtime_pb2.FeedMessage()
    container.ParseFromString(file.read())

    return container


def container_put_entities(all_entities):
    container = gtfs_realtime_pb2.FeedMessage()
    header = container.header
    header.gtfs_realtime_version = "2.0"
    header.incrementality = 0
    header.timestamp = round(time.time() + 1)

    for entity_data in all_entities:
        entity = container.entity.add()
        entity.id = entity_data['id']
        vehicle = entity.vehicle

        vehicle.trip.trip_id = entity_data["vehicle"]["trip"]["trip_id"]
        vehicle.trip.route_id = entity_data["vehicle"]["trip"]["route_id"]
        vehicle.trip.start_time = entity_data["vehicle"]["trip"]["start_time"]
        vehicle.trip.start_date = entity_data["vehicle"]["trip"]["start_date"]
        vehicle.vehicle.id = entity_data["vehicle"]["vehicle"]['id']
        vehicle.vehicle.label = entity_data["vehicle"]["vehicle"]['label']

        vehicle.position.latitude = float(entity_data["vehicle"]["position"]['latitude'])
        vehicle.position.longitude = float(entity_data["vehicle"]["position"]['longitude'])

        vehicle.timestamp = int(entity_data["vehicle"]["timestamp"])

    write_proto_buffer_data(container)

    return container

def find_transit_vehicle(stop_times_data, stops_lat_long, stop_name, index, route_ids):
    # print( datetime.datetime.now().time())
    global all_entities
    st = time.time()

    current_time = datetime.datetime.now().time()

    date_today = datetime.datetime.today().strftime('%Y%m%d')

    transit_trip_id = []

    transit_stop_id = []

    outfile = open('final_json.json', 'w+')
    outfile.write('[\n')
    transit_found = False
    route_data_file = open('route_data_file.json', 'w+')
    route_data_file.write('[\n')
    for i in range(len(index) - 1):
        low = index[i]
        high = index[i + 1] - 1

        transit_found = False
        while low <= high:

            mid = low + (high - low) // 2
            # print(low, high, mid)
            current_data = stop_times_data[mid]
            dep_time = stop_times_data[mid][2]
            # print(dep_time, stop_times_data[mid])
            if int(dep_time[0:2]) >= 24:
                dep_time = '0' + str(int(dep_time[0:2]) - 24) + dep_time[2:]
            dep_time_dt_obj = datetime.datetime.strptime(dep_time, '%H:%M:%S')
            dep_time = dep_time_dt_obj.time()
            if dep_time <= current_time and stop_times_data[mid + 1][0] == current_data[0]:
                next_stop_data = stop_times_data[mid + 1]
                next_arr_time = next_stop_data[1]
                if int(next_arr_time[0:2]) >= 24:
                    next_arr_time = '0' + str(int(next_arr_time[0:2]) - 24) + next_arr_time[2:]
                next_stop_dt_obj = datetime.datetime.strptime(next_arr_time, '%H:%M:%S')
                next_arr_time = next_stop_dt_obj.time()
                if next_arr_time >= current_time:
                    transit_trip_id += [stop_times_data[mid][0]]
                    transit_stop_id += [current_data[3]]
                    # print(current_data[0],current_data[3])
                    # print(current_data[0], current_time, date_today,route_ids[current_data[0]], stops_lat_long[current_data[3]])
                    json_transit_data = create_json(current_data[0], current_time, date_today,
                                                    route_ids[current_data[0]], stops_lat_long[current_data[3]])
                    if len(transit_trip_id) != 1:
                        outfile.write(',\n')
                    outfile.write(json_transit_data)
                    transit_found = True
                    break
                else:
                    low = mid + 1
            else:
                high = mid - 1

        if transit_found:
            covered_route = []
            remaining_route = []
            reached = False
            for j in range(index[i], index[i + 1]):
                if not reached:
                    covered_route += [stop_name[stop_times_data[j][3]]]
                    if stop_times_data[j][3] == transit_stop_id[-1]:
                        reached = True

                else:
                    remaining_route += [stop_name[stop_times_data[j][3]]]
            route_map = {'covered': covered_route, 'remaining': remaining_route}
            if len(transit_trip_id) != 1:
                route_data_file.write(',\n')
            route_data_file.write(json.dumps(route_map, indent=4))

    outfile.write(']\n')
    outfile.close()
    container_put_entities(all_entities)
    route_data_file.write(']\n')
    route_data_file.close()
    print(len(transit_trip_id))
    en = time.time()
    print(en - st)


def main():
    stop_times_data, stops_lat_long, stop_name, index, route_ids = collect_data()
    while True:
        st = time.time()
        find_transit_vehicle(stop_times_data, stops_lat_long, stop_name, index, route_ids)
        en = time.time()
        time.sleep(30 - (en - st) % 60)


if __name__ == '__main__':
    main()
