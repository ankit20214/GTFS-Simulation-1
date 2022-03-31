import json, re, datetime, time


def load_stops_data():
    stops = open('stops.txt', 'r')
    stops_data = stops.readlines()
    stops.close()
    stop_lat_long_info = {}
    stop_name = {}

    stops_data = [i for i in stops_data if i != '\n']

    for i in range(1, len(stops_data)):
        stops_data[i] = list(stops_data[i].split(','))
        stop_lat_long_info[stops_data[i][0]] = [stops_data[i][3], stops_data[i][4]]
        stop_name[stops_data[i][0]] = stops_data[i][2]

    return stop_lat_long_info, stop_name


def create_json(trip_id, start_time, start_date, route_id, lat_long):
    vehicle_id = str(route_id) + str(trip_id.split('_')[1]) + str(trip_id.split('_')[2])
    vehicle_id = 'DL' + '0' * (4 - len(str(route_id))) + vehicle_id
    entity = {'id': vehicle_id, 'vehicle': {
        'trip': {'trip_id': trip_id, 'start_time': start_time.strftime("%H:%M:%S"), 'start_date': str(start_date),
                 'route_id': route_id}, 'position': {'latitude': lat_long[0], 'longitude': lat_long[1]},
        'timestamp': int(datetime.datetime.now().timestamp()),
        'vehicle': {'id': vehicle_id, 'label': vehicle_id}}}
    m = json.dumps(entity, indent=4)
    # print(re.sub(r'(?<!: )"(\S*?)"', '\\1', m))
    return m


def collect_data():
    stop_times = open('stop_times.txt', 'r')
    stop_times_data = stop_times.readlines()
    stop_times.close()

    stop_times_data = [i for i in stop_times_data if i != '\n']

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

    trips_data = [i for i in trips_data if i != '\n']

    route_ids = {}
    for i in range(len(trips_data)):
        trips_data[i] = trips_data[i].split(',')
        route_ids[trips_data[i][2]] = trips_data[i][0]

    return stop_times_data, stops_lat_long, stop_name, index, route_ids


def find_transit_vehicle(stop_times_data, stops_lat_long, stop_name, index, route_ids):
    # print( datetime.datetime.now().time())
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
