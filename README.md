# GTFS-Simulation-1
Upon execution,  identifies the trips (buses) that are operational at the time and the part of route that has been completed on these trips based on the static data in the GTFS folder. Using this static data we can then identify the position (bus stop) of a bus, for the current time. The code refreshes every 30 seconds and provides updated positions and the and buses in transit.
Updated 2 parts of code:- 1) Now runs every 30 seconds(The time taken upon running the program to parse through files to collect relevant information is not taken into account as it is a 1 time affair.) .    2)Now code uses route.txt to find the route_id.
