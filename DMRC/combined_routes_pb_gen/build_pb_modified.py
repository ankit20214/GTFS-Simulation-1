import json, re, datetime, time
from google.transit import gtfs_realtime_pb2
import requests
from google.protobuf import json_format
import time
import schedule

#  Virtual Env -> py -3 -m venv file_name

def search_on_shapes(shape_id_distances,shape_id,dist):
    arr = shape_id_distances[shape_id]
    if dist >= arr[-1]:
        return len(arr)-1
    for i in range(len(arr)):
        if arr[i] == dist:
            return i
        if arr[i] > dist:
            return i-1


def binary_on_shapes(shape_id_coordinates, shape_id, dist,current_data):
    arr = shape_id_coordinates[shape_id]
    print(arr)
    l = 0
    h = len(arr) - 1
    while l <= h:
        mid = l + (h - l) // 2
        if arr[mid] == dist:
            return mid
        elif arr[mid] < dist:
            if mid+1 < len(arr) and arr[mid + 1] > dist:
                return mid
            else:
                l = mid + 1
        else:
            if mid-1 > 0 and arr[mid - 1] < dist:
                return mid - 1
            else:
                h = mid - 1
    print(shape_id)
    print(dist)
    print(current_data)
    print("Here")


def load_stops_data():
    stops = open('stops.txt', 'r')
    stops_data = stops.readlines()
    stops.close()
    stop_lat_long_info = {}
    stop_name = {}
    for i in range(1, len(stops_data)):
        stops_data[i] = stops_data[i].strip()
        stops_data[i] = list(stops_data[i].split(','))
        stop_lat_long_info[stops_data[i][0]] = [stops_data[i][4], stops_data[i][5]]
        stop_name[stops_data[i][0]] = stops_data[i][2]

    return stop_lat_long_info, stop_name


def create_json(trip_id, start_time, start_date, route_id, lat_long, all_entities):
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

def read_shapes_data():
    # -----------------------------------------------------------------------
    #  Shapes Data Read and Segregate
    # -----------------------------------------------------------------------
    shapes = open('shapes.txt', 'r')
    shapes_data = shapes.readlines()
    shapes_data = [shapes_data[i].split(',') for i in range(len(shapes_data))]

    shape_ids = []
    shape_index = []
    for i in range(1, len(shapes_data)):
        if len(shape_ids) == 0:
            shape_ids += [shapes_data[i][0]]
            shape_index += [1]
        else:
            if shapes_data[i][0] != shape_ids[-1]:
                shape_ids += [shapes_data[i][0]]
                shape_index += [i]
    shape_index += [len(shapes_data) - 1]

    shape_id_ind_dict = {}
    for i in range(len(shape_index) - 1):
        shape_id_ind_dict[shape_ids[i]] = [shape_index[i], shape_index[i + 1]]

    shape_id_distances = {}
    shape_id_coordinates = {}
    for k in shape_id_ind_dict.keys():
        st_ind = shape_id_ind_dict[k][0]
        en_ind = shape_id_ind_dict[k][1]
        shape_id_distances[k] = [float(shapes_data[i][-1].strip()) for i in range(st_ind, en_ind, 1)]
        shape_id_coordinates[k] = [[float(shapes_data[i][1]),float(shapes_data[i][2])] for i in range(st_ind,en_ind,1)]
    return shapes_data,shape_id_distances,shape_id_coordinates
    # -------------------------------------------------------------------------------------------------------


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

    route_file = open('routes.txt','r')
    route_data = route_file.readlines()
    route_file.close()

    averages = [[] for i in range(len(route_data) -1)]
    f = open('last_final.txt', 'r')
    f_d = f.readlines()
    f.close()
    f_d = [f_d[i].strip() for i in range(len(f_d))]
    f_d = [f_d[i].split(",") for i in range(len(f_d))]
    for i in range(len(f_d)):
        averages[int(f_d[i][-3])] += [f_d[i][-1]]

    trips = open('trips.txt', 'r')
    trips_data = trips.readlines()
    trips.close()
    route_ids = {}
    route_to_shape_mapping = {}
    for i in range(len(trips_data)):
        trips_data[i] = trips_data[i].strip()
        trips_data[i] = trips_data[i].split(',')
        route_ids[trips_data[i][2]] = trips_data[i][0]
        route_to_shape_mapping[trips_data[i][0]] = trips_data[i][-3]

    return stop_times_data, stops_lat_long, stop_name, index, route_ids,route_to_shape_mapping,averages


def write_proto_buffer_data(container):
    file = open('proto_buffer_data.pb', 'wb')
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


def find_transit_vehicle(stop_times_data, stops_lat_long, stop_name, index, route_ids,route_to_shape_mapping,shapes_data,shape_id_distances,shapes_id_coordinates,averages):
    # print( datetime.datetime.now().time())
    all_entities = []
    st = time.time()

    current_time = datetime.datetime.now().time()
    # c_t = '06:44:20'
    # current_time = datetime.datetime.strptime(c_t, '%H:%M:%S').time()

    date_today = datetime.datetime.today().strftime('%Y%m%d')

    transit_trip_id = []

    transit_stop_id = []
    temp_arr = []
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
                    # --------------------------------------------------------------------------
                    #  New Code
                    travel_route = route_ids[current_data[0]]
                    average_speed = averages[int(travel_route)]
                    time_diff = (current_time.second + current_time.minute*60 + current_time.hour*3600) - (dep_time.second + dep_time.minute*60 + dep_time.hour*3600)
                    distace_travelled = float(current_data[-2]) + (float(average_speed[int(current_data[4])])*time_diff)
                    if distace_travelled > float(next_stop_data[-2]):
                        distace_travelled = float(next_stop_data[-2]) - 1
                    r_shape_id = route_to_shape_mapping[travel_route]
                    shape_lat_long_index = search_on_shapes(shape_id_distances,r_shape_id,distace_travelled)
                    final_lat_long = shapes_id_coordinates[r_shape_id][shape_lat_long_index]

                    print(current_data[0],distace_travelled)
                    # -----------------------------------------------------------------------


                    json_transit_data = create_json(current_data[0], current_time, date_today,
                                                    route_ids[current_data[0]], final_lat_long,
                                                    all_entities)
                    if travel_route == "40":
                        temp_arr += [final_lat_long]

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

    print(temp_arr)
    outfile.write(']\n')
    outfile.close()
    container_put_entities(all_entities)
    route_data_file.write(']\n')
    route_data_file.close()
    print(len(transit_trip_id))
    en = time.time()
    print(en - st)


def main():
    stop_times_data, stops_lat_long, stop_name, index, route_ids,route_to_shape_mapping,averages = collect_data()
    shapes_data,shape_id_distances,shapes_id_coordinates = read_shapes_data()

    while True:
        st = time.time()
        find_transit_vehicle(stop_times_data, stops_lat_long, stop_name, index, route_ids,route_to_shape_mapping,shapes_data,shape_id_distances,shapes_id_coordinates,averages)
        en = time.time()
        break
        # time.sleep(10 - (en - st) % 60)


if __name__ == '__main__':
    main()
