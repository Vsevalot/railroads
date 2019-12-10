#! -*- encoding: utf-8 -*-
import sqlite3
from typing import List, Tuple
import sys


HELP = """
  This script calculates the shortest distance between 
  two stations depends on data in railroads.db
  Make sure the railroads.db is in the same directory with script
  Make sure the railroads.db contains data 
  from Kniga_1...xls, Kniga_2...xls, Kniga_3...xls
  
  To calculate distance run script with code station from train starts 
  and code station to which train goes
  The script prints shortest calculated distance 
  if the stations with these codes are connected. 
  Prints -1 if stations are not connected 
  Prints -2 if arguments got unexpected arguments
  
  Example: D:\\work\\MyPyProjects\\railroads>distance_calculator.exe 060904 214109
  Output: 337
  
  Optional flag --debug allows to see all taken calculations
  Example: D:\\work\\MyPyProjects\\railroads>distance_calculator.exe 060904 214109 --debug
  Output: 060904 to 238207 42km + 238207 to 210201 282km + 210201 to 214109 24km = 348km
          060904 to 238207 42km + 238207 to 214700 364km + 214700 to 214109 58km = 464km
          060904 to 060001 76km + 060001 to 210201 237km + 210201 to 214109 24km = 337km
          060904 to 060001 76km + 060001 to 214700 319km + 214700 to 214109 58km = 453km
          060904 to 062100 120km + 062100 to 210201 444km + 210201 to 214109 24km = 588km
          060904 to 062100 120km + 062100 to 214700 526km + 214700 to 214109 58km = 704km
          337
          
  Этот скрипт расчитывает кратчайшее расстояние между двумя станциями 
  используя данные из базы railroads.db
  Убедитесь, что railroads.db находится в одной директории со скриптом
  Убедитесь, что railroads.db содержит все данные из книг
  Kniga_1...xls, Kniga_2...xls, Kniga_3...xls
  
  To calculate distance run script with code station from train starts 
  and code station to which train goes
  The script prints shortest calculated distance 
  if the stations with these codes are connected. 
  Prints -1 if stations are not connected 
  Prints -2 if arguments got unexpected arguments
  
  Example: D:\\work\\MyPyProjects\\railroads>distance_calculator.exe 060904 214109
  Output: 337
  
  Optional flag --debug allows to see all taken calculations
  Example: D:\\work\\MyPyProjects\\railroads>distance_calculator.exe 060904 214109 --debug
  Output: 060904 to 238207 42km + 238207 to 210201 282km + 210201 to 214109 24km = 348km
          060904 to 238207 42km + 238207 to 214700 364km + 214700 to 214109 58km = 464km
          060904 to 060001 76km + 060001 to 210201 237km + 210201 to 214109 24km = 337km
          060904 to 060001 76km + 060001 to 214700 319km + 214700 to 214109 58km = 453km
          060904 to 062100 120km + 062100 to 210201 444km + 210201 to 214109 24km = 588km
          060904 to 062100 120km + 062100 to 214700 526km + 214700 to 214109 58km = 704km
          337
          
  """


def get_distances_to_tp(cursor: sqlite3.Cursor, station_code: str) -> List[Tuple[str, int]]:
    """
    Search for all transit points connected to the given station
    :param cursor: cursor to the railroads.db
    :param station_code: Station code in r_transportation_railroad_stations or Kniga_2...xls
    :return: List of tuples with transit point code and distance to it
    """
    # Search for transit points connected to the station with given code
    transit_from_query = f"""SELECT code_to, transit_distance 
                             FROM r_transportation_transit_distances 
                             WHERE code_from = '{station_code}' 
                             ORDER BY transit_distance"""
    transit_from_select = cursor.execute(transit_from_query).fetchall()

    if len(transit_from_select) != 0:
        smallest_distance = transit_from_select[0][1]   # Because SELECT is ordered by distance, if station is a tp
        if smallest_distance == 0:  # the first distance would be 0, so no need to check all other distances
            return [transit_from_select[0]]  # Return only this transit point and distance
        return transit_from_select  # Else return all transit points and distances
    else:  # If station doesn't have any connection with transit points - look in parts
        part_query = f"""SELECT code_to, distance_between_stations 
                         FROM r_transportation_railroad_part_distances 
                         WHERE code_from = '{station_code}'"""
        part_select = cursor.execute(part_query).fetchall()

        station_code_distances = []
        for selected in part_select:  # Calculate distances to the closest transit points
            selected_code = selected[0]
            selected_distance = selected[1]
            is_station_tp_query = f"""SELECT * FROM r_transportation_transit_distances 
                                      WHERE code_from = '{selected_code}' AND code_to = '{selected_code}'"""
            is_station_tp_select = cursor.execute(is_station_tp_query).fetchall()  # Check if station is a transit point
            if len(is_station_tp_select) == 0:  # If station is not a transit point
                next_stations = get_distances_to_tp(cursor, selected_code)  # Look for transit points to that station
                for station in next_stations:
                    new_station_code = station[0]  # Code of the main station of the station_code main station
                    new_station_distance = station[1] + selected_distance  # Distance from station_code to main's main
                    station_code_distances.append((new_station_code, new_station_distance))
            else:
                station_code_distances.append(selected)
        return station_code_distances


def same_part_stations_distance(cursor: sqlite3.Cursor, code_from: str, code_to: str, debug: bool) -> int:
    """
    Checks if stations are at the same railroad part and return distance between them
    :param cursor: cursor to the railroads.db
    :param code_from: Station code in r_transportation_railroad_stations or Kniga_2...xls
    :param code_to: Station code in r_transportation_railroad_stations or Kniga_2...xls
    :param debug: Flag to print to all station codes and distances while calculating
    :return: distance between stations if they are at the same railroad part else -1
    """
    first_part_code_query = f"""SELECT DISTINCT part_code 
                                FROM r_transportation_railroad_part_distances 
                                WHERE code_from = '{code_from}'"""
    first_part_code_select = cursor.execute(first_part_code_query).fetchall()
    if len(first_part_code_select) > 0:
        first_part_codes = [part_code[0] for part_code in first_part_code_select]
        for first_part_code in first_part_codes:
            second_part_code_query = f"""SELECT DISTINCT part_code FROM r_transportation_railroad_part_distances 
                                         WHERE part_code = '{first_part_code}' AND code_from = '{code_to}'"""
            second_part_code_select = cursor.execute(second_part_code_query).fetchall()
            if len(second_part_code_select) > 0:
                second_part_codes = [part_code[0] for part_code in second_part_code_select]
                for second_part_code in second_part_codes:
                    if first_part_code == second_part_code:
                        distances_query = f"""SELECT r_transportation_railroad_part_distances.distance_between_stations 
                                              FROM r_transportation_railroad_part_distances 
                                              WHERE (code_from = '{code_from}' OR code_from = '{code_to}')
                                              AND part_code = '{first_part_code}' 
                                              ORDER BY code_to"""
                        distances_select = cursor.execute(distances_query).fetchall()
                        if len(distances_select) == 2 or len(distances_select) == 4:
                            from_to_origin = distances_select[0][0]  # Distance from code_from station to part origin
                            to_to_origin = distances_select[1][0]  # Distance from code_to station to part origin
                            distance = from_to_origin - to_to_origin
                            if distance < 0:
                                distance = -distance
                            if debug:
                                print(f"From {code_from} to {first_part_code} origin {from_to_origin}km, "
                                      f"from {code_to} to {first_part_code} origin {to_to_origin}km, "
                                      f"between = {distance}km")
                            return distance
    return -1


def is_station_exists(cursor: sqlite3.Cursor, station_code: str) -> bool:
    """
    Checks if station with given code exists in the database
    :param cursor: cursor to the railroads.db
    :param station_code: Code of the station in the r_transportation_railroad_stations table (Source - Kniga_2...xls)
    :return:
    """
    station_exists_query = "SELECT * FROM r_transportation_railroad_stations WHERE code = (?)"  # Safe check for
    station_exists_select = cursor.execute(station_exists_query, (station_code, )).fetchall()  # User input
    if len(station_exists_select) == 1:
        return True
    return False


def calculate_travel_distance(cursor: sqlite3.Cursor, code_from: str, code_to: str, debug: bool = False) -> int:
    """
    Calculates travel time between two stations with given codes, depends on average travel speed
    :param cursor: cursor to the railroads.db
    :param code_from: Station code in r_transportation_railroad_stations or Kniga_2...xls
    :param code_to: Station code in r_transportation_railroad_stations or Kniga_2...xls
    :param debug: Flag to print to all station codes and distances while calculating
    :return: Distance between stations or -1 if they are not connected
    """
    if not is_station_exists(cursor, code_from):
        print(f"  Station with code {code_from} does not exist in database")
        return -2

    if not is_station_exists(cursor, code_to):
        print(f"  Station with code {code_to} does not exist in database")
        return -2

    if code_from == code_to:  # Distance from a station to itself is 0
        return 0

    # Check if stations are transit points (ТП - Kniga_3 stations)
    transit_check_query = f"""SELECT transit_distance FROM r_transportation_transit_distances 
                              WHERE code_from = '{code_from}' AND code_to = '{code_to}'"""
    transit_check_select = cursor.execute(transit_check_query).fetchall()
    if len(transit_check_select) == 1:
        return transit_check_select[0][0]

    # Check if stations are located on the same railroad part
    distance = same_part_stations_distance(cursor, code_from, code_to, debug)
    if distance != -1:  # If distance != -1 the stations are at the same railroad part
        return distance

    transit_points_from = get_distances_to_tp(cursor, code_from)
    transit_points_to = get_distances_to_tp(cursor, code_to)

    distances = []
    for i in range(len(transit_points_from)):
        for k in range(len(transit_points_to)):
            transit_from = transit_points_from[i][0]
            transit_to = transit_points_to[k][0]
            transit_distance_query = f"""SELECT transit_distance FROM r_transportation_transit_distances 
                                         WHERE code_from = '{transit_from}' AND code_to = '{transit_to}'"""
            transit_distance_select = cursor.execute(transit_distance_query).fetchall()
            if len(transit_distance_select) != 0:  # If SELECT is empty - transit points are not connected
                transit_distance = transit_distance_select[0][0]
                distance_from = transit_points_from[i][1]
                distance_to = transit_points_to[k][1]
                distance = distance_from + transit_distance + distance_to
                if debug:
                    print(f"{code_from} to {transit_from} {distance_from}km + "
                          f"{transit_from} to {transit_to} {transit_distance}km + "
                          f"{transit_to} to {code_to} {distance_to}km = {distance}km")
                distances.append(distance)

    if len(distances) == 0:  # If no distances were added
        return -1

    return min(distances)


if __name__ == "__main__":
    if len(sys.argv) == 2:  # The first element is the script name
        if sys.argv[1] == "--help":
            print(HELP)
        else:
            print("\n  Wrong number of arguments. Run script with --help flag to learn more")
    elif 2 < len(sys.argv) < 5:  # Valid number of arguments is 3 or 4 (script name,code_from,code_to,debug - optional)
        code_from = sys.argv[1]
        code_to = sys.argv[2]
        debug = False
        if len(sys.argv) == 4:
            if sys.argv[3] == "--debug":
                debug = True

        path_to_database = "railroads.db"
        connection = sqlite3.connect(path_to_database)
        db_cursor = connection.cursor()

        # station_from = "060904"
        # station_to = "214109"
        distance = calculate_travel_distance(db_cursor, code_from, code_to, debug=debug)
        print(distance)
    else:
        input("\n  This script is supposed to be used via console.\n  Run script with --help flag to learn more")
        input("\n  Этот скрипт содан для консольного использования.\n  Запустите скрипт с флагом --help для информации")
