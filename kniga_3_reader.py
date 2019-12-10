#! -*- encoding: utf-8 -*-
from pandas import read_excel
import sqlite3
from typing import List, Tuple, Dict
from kniga_2_reader import repair_table


def get_transit_table(worksheet) -> List[List[str]]:
    """
    Takes only transit distances and station names from the given worksheet
    :param worksheet: pandas DataFrame of worksheet of Kniga_3...xls
    :return: List of lists of strings with station names at first row and distances between stations
    """
    data_list = [[str(cell) for cell in ndarray] for ndarray in worksheet.values]

    first_row = -1

    for i in range(len(data_list)):
        if data_list[i][0] == "№ п/п":
            first_row = i + 1
            break

    if first_row == -1:
        print("Start of the table not found!")
        exit(-1)

    return [data_list[first_row - 2]] + data_list[first_row:]  # Column names + data rows


def insert_transit_distances(cursor: sqlite3.Cursor, worksheet, ws_name: str):
    """
    Inserts all transit distances from the given worksheet of Kniga_3...xls
    :param cursor: cursor to the railroads.db
    :param worksheet: pandas DataFrame of worksheet of Kniga_3...xls
    :return: None
    """
    transit_table = get_transit_table(worksheet)
    transit_table = [transit_table[0]] + repair_table(transit_table[1:])  # Repair all rows except column names
    transit_table = [transit_table[0][1:]] + [transit_table[i][1:] for i in range(1, len(transit_table))]  # Remove №
    """
    Now the table contains stations and distances like this:
    ['nan', 'Батуми', 'Гантиади (эксп.)', 'Гардабани (эксп.)', ...]
    ['Батуми', '0', 'nan', '396', ...]
    ['Гантиади (эксп.)', 'nan', '0', '556', ...]
    ['Гардабани (эксп.)', '396', '556', '0', ...]
    ...  
    """
    code_distances_table = get_code_distances_table(cursor, transit_table, ws_name)
    insert_values = get_insert_values(code_distances_table)

    insert_query = """INSERT OR REPLACE INTO r_transportation_transit_distances (code_from, code_to, transit_distance) 
                      VALUES (?, ?, ?)"""

    for value in insert_values:
        cursor.execute(insert_query, value)


def get_insert_values(code_distances_table: List[List[str]]) -> List[Tuple[str, str, str]]:
    """
    Takes table with station codes as first row and first column and return a list of tuples to insert into db
    :param code_distances_table: table with station codes as first row and first column and distances in other cells
    :return: list of tuples with (station from code, station to code, distance)
    """
    """
    Function expects something like:
    
    ['', '571509', '574704', '563606', '572107', '564204', '570008', '571903', '560101', '577204', ...]
    ['571509', 0, 368, 396, 174, 423, 104, 132, 354, 228, ...]
    ['574704', 368, 0, 556, 278, 583, 264, 236, 514, 388, ...]
    ['563606', 396, 556, 0, 362, 111, 292, 320, 42, 168, ...]
    ['572107', 174, 278, 362, 0, 389, 70, 42, 320, 194, ...]
    ['564204', 423, 583, 111, 389, 0, 319, 347, 69, 195, ...]
    ['570008', 104, 264, 292, 70, 319, 0, 28, 250, 124, ...]
    ['571903', 132, 236, 320, 42, 347, 28, 0, 278, 152, ...]
    ['560101', 354, 514, 42, 320, 69, 250, 278, 0, 126, ...]
    ['577204', 228, 388, 168, 194, 195, 124, 152, 126, 0, ...]
    ...
    """

    values = []
    for i in range(1, len(code_distances_table)):  # The first element is row of station codes
        code_from = code_distances_table[i][0]
        if code_from == '':  # If no code found for this station - pass the entire row
            continue
        for k in range(1, len(code_distances_table[i])):  # The first element is code of the station
            code_to = code_distances_table[0][k]
            if code_to == '':  # If no code found for this station - pass the entire row
                continue

            distance = code_distances_table[i][k]
            if distance == -1:  # If distance between station is -1 that means that stations aren't connected
                continue

            values.append((code_from, code_to, distance))
    return values


def get_station_railroad(station_name: str) -> str:
    """
    Checks if the given table contains station names with railroad codes after it like: Station name (51 С-Кав)
    :param station_name: Cell with string station name
    :return: Code of the railroad if column names contain railroad codes else empty string
    """
    spit_name = station_name.split('(')  # "Армянск (эксп.) (85 Крым)" -> ["Армянск ", "эксп.) ", "85 Крым)"]
    if spit_name[-1][0] in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:  # If first char of the last element
        return spit_name[-1][:2]
    return ''


def get_station_name(station_cell: str) -> str:
    """
    Read cell and return it without last brackets "Занозная (17 Моск)" -> "Занозная"
    :param station_cell: A cell with station name and railroad
    :return: station name
    """
    for i in range(len(station_cell) - 1, -1, -1):
        if station_cell[i] == '(':  # The last opening bracket in the cell
            return station_cell[:i - 1]  # Return cell value without last brackets and space before them
    return ''


def station_code_by_name(cursor: sqlite3.Cursor, station_cell: str, ws_name: str = '') -> str:
    """
    Search for the station code with given the name in the database
    :param cursor: cursor to the railroads.db
    :param station_cell: cell value of a Kniga_3...xls worksheet with name of station (and sometimes railroad)
    :param ws_name: Name of the worksheet in the Kniga_3...xls (used to find station by name)
    :return: station code if only one found else -1 if not found and -2 if found several
    """

    railroad_code = get_station_railroad(station_cell)
    station_name = station_cell

    if railroad_code != '':  # Is station cell contains railroad code, than station name != station cell
        station_name = get_station_name(station_cell)
    else:  # Find railroad code using worksheet's name
        railroad_code_query = "SELECT code FROM r_transportation_railroads WHERE sname = (?)"
        railroad_code_select = cursor.execute(railroad_code_query, (ws_name, )).fetchall()
        if len(railroad_code_select) != 1:  # If cannot find railroad with such sname
            print(f"Railroad with sname {ws_name} was not found or found in several versions. Worksheet won't be added")
            return ''  # Or if SELECT contains more than 1 element - return an empty string

        railroad_code = railroad_code_select[0][0]

    station_code_query = """SELECT code FROM r_transportation_railroad_stations 
                            WHERE name = (?) AND railroad_code = (?) AND type = 'РП'"""

    station_code_select = cursor.execute(station_code_query, (station_name, railroad_code)).fetchall()

    if len(station_code_select) != 1:  # If SELECT is empty - no stations with such name / (name + railroad) were found
        print(f"\n! Station with name {station_cell} was not found or found in several versions. It will not be added\n")
        return ''  # Or if SELECT contains more than 1 element - return an empty string
    return station_code_select[0][0]  # If there is only one element in the SELECT - return it. It's the station code


def get_distance(distance_cell: str) -> int:
    """
    Read a Kniga_3...xls cell with distance and return integer of the distance or -1 if "nan"
    :param distance_cell: Cell of the worksheet from Kniga_3...xls cell with distance
    :return: Distance or -1 if "nan" in the given cell
    """
    if distance_cell == "nan":
        return -1
    distance_digits = []
    for char in distance_cell:
        if char in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:
            distance_digits.append(char)
    if len(distance_digits) == 0:
        return -1
    return int(''.join(distance_digits))


def get_code_distances_table(cursor: sqlite3.Cursor, transit_table: List[List[str]], ws_name: str) -> List[List[str]]:
    """
    :param cursor: cursor to the railroads.db
    :param transit_table: List of lists of strings with station names at first row and distances between stations
    :return: List of lists with station ids as first row and first column and distances between stations
    :param ws_name: Name of the worksheet in the Kniga_3...xls (used to find station by name)
    Distance gets value -1 if stations are not connected
    """

    code_distances_table = [['']]
    for i in range(1, len(transit_table[0])):  # Form the first row - station codes (First element is always "nan")
        station_code = station_code_by_name(cursor, transit_table[0][i], ws_name)
        code_distances_table[0].append(station_code)

    for i in range(1, len(transit_table)):  # Starts from the second row because the first one is station row
        station_code = station_code_by_name(cursor, transit_table[i][0], ws_name)
        code_distances_table.append([station_code])  # Add list with station code from first column as first element
        for k in range(1, len(transit_table[i])):  # Add distances for all columns at this row
            code_distances_table[i].append(get_distance(transit_table[i][k]))
    return code_distances_table


def add_kniga3(cursor: sqlite3.Cursor, path_to_kniga3: str,
               unused_worksheets: Tuple[str, str] = ("Общие положения", "Вводные положения")):
    """
    Reads Kniga_3_...xls from РЖД and insert or update all data in railroads.db
    :param cursor: cursor to the railroads.db
    :param path_to_kniga3: path to Kniga_3_...xls
    :param unused_worksheets: "Общие положения", "Вводные положения" and other no data storing worksheets
    :return: None
    """
    worksheets = list(read_excel(path_to_kniga3, sheet_name=None).keys())
    for worksheet in worksheets:
        if worksheet not in unused_worksheets:
            transit_worksheet = read_excel(path_to_kniga3, sheet_name=worksheet, header=None, index_col=False)
            # For some reason not all worksheet names match with r_transportation_railroads sname column
            if worksheet == "Молд":
                insert_transit_distances(cursor, transit_worksheet, "Млд")
            elif worksheet == "Каз":
                insert_transit_distances(cursor, transit_worksheet, "Кзх")
            elif worksheet == "Груз":
                insert_transit_distances(cursor, transit_worksheet, "Грз")
            elif worksheet == "Узб":
                insert_transit_distances(cursor, transit_worksheet, "Узбк")
            elif worksheet == "Азер":
                insert_transit_distances(cursor, transit_worksheet, "Азерб")
            elif worksheet == "Кирг":
                insert_transit_distances(cursor, transit_worksheet, "Кырг")
            elif worksheet == "Турк":
                insert_transit_distances(cursor, transit_worksheet, "Трк")
            else:
                insert_transit_distances(cursor, transit_worksheet, worksheet)
            print(f"Kniga_3 {worksheet} complete")

    return None


if __name__ == "__main__":
    path_to_database = "railroads.db"
    connection = sqlite3.connect(path_to_database)
    db_cursor = connection.cursor()

    path_to_kniga3 = "C:/Users/User/Desktop/ЖД/Kniga_3_2019-10-09.xls"
    add_kniga3(db_cursor, path_to_kniga3)

    connection.commit()
    db_cursor.execute("VACUUM")
    print("Complete")
