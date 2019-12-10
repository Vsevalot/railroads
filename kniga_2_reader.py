#! -*- encoding: utf-8 -*-
import pandas as pd
from typing import List, Dict
import sqlite3
from references import update_references
from table_generating import create_tables

BIG_TYPE_CODE: str = "РП"  # Big stations - Kniga_2 РП
SMALL_TYPE_CODE: str = "ОП"  # Small stations - Kniga_2 ОП


def get_railroad_code(railroad_cell: str) -> str:
    """
    Reads an excel railroad cell and return railroad id from r_transportation_railroads if found else -1
    :param railroad_cell: Railroad cell from excel worksheet ("76 Сверд (Р)")
    :return: railroad code
    """
    code: str = ''
    for i in range(len(railroad_cell)):
        if railroad_cell[i] in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:  # Search for the first digit of code
            for k in range(i + 1, len(railroad_cell)):
                if railroad_cell[k] not in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:  # Search for the last
                    code = railroad_cell[i:k]
                    break
        if code != '':  # If code has been found
            break
    return code


def get_operation_codes(operations_cell: str) -> List[str]:
    """
    Reads an excel operations cell and return list of operations codes from the r_transportation_operations table
    :param operations_cell: Operations cell from excel worksheet ("О 1,3,4,6,8,8н, 9,10,10н")
    :return: List of operations
    """
    operations = []
    operation = ''
    for char in operations_cell:
        if char != ' ' and char != ',':
            operation += char
        elif operation != '':
            if operation != "nan":
                operations.append(operation)
            operation = ''
    if operation != '' and operation != "nan":
        operations.append(operation)
    return operations


def repair_string(string: str) -> str:
    """
    Remove extra spaces and \t, \n from the given string
    :param string: A cell from excel worksheet
    :return: Clean given string
    """
    repaired = []
    for i in range(len(string) - 1):
        if string[i] == ',' and string[i + 1] != ' ':
            repaired.append(',')
            repaired.append(' ')
        if string[i] == ' ' and string[i + 1] == ' ':
            continue
        if string[i] == '\n' or string[i] == '\t':
            repaired.append(' ')
            continue
        repaired.append(string[i])
    if string[-1] != ' ' and string[-1] != '\t' and string[-1] != '\n':
        repaired.append(string[-1])
    return ''.join(repaired)


def repair_row(table_row: List[str]) -> List[str]:
    """
    Make each element of the given excel row clean (without extra spaces, \t and \n)
    :param table_row: A row of excel cells - list of str
    :return: Clean row
    """
    for i in range(len(table_row)):
        table_row[i] = repair_string(table_row[i])
    return table_row


def is_row_empty(row: List[str]) -> bool:
    """
    Empty row contains only "nan" values, rows like ["nan", "nan", "nan", some value, "nan", "nan"...]
    are parts of previous rows
    :param row: A row of excel cells - list of str
    :return: True if row is empty
    """
    for cell in row:
        if cell != "nan":
            return False
    return True


def repair_table(table_list: List[List[str]]) -> List[List[str]]:
    """
    Railroad's excel sometimes store information about one element if several rows, so the first step is collecting
    such data to one row and the second step is removing extra symbols (without extra spaces, \t and \n)
    :param table_list: A table of excel cells - list of list of str
    :return: Clean table
    """
    for i in range(len(table_list) - 1, -1, -1):  # From the bottom row up to top
        if table_list[i][0] == "nan":  # If the first column of the row is "nan" - this row is a part of previous one
            for k in range(len(table_list[i])):
                if table_list[i][k] != "nan":  # If cell contain any data - add row data to previous row
                    if table_list[i - 1][k][-1] == '-':  # "Москва-Пассажирская-Киевская" should be write without spaces
                        table_list[i - 1][k] = "%s%s" % (table_list[i - 1][k], table_list[i][k])
                    else:  # All other rows should contain a space between parts "Дупленская (обп)"
                        table_list[i - 1][k] = "%s %s" % (table_list[i - 1][k], table_list[i][k])
            continue
        table_list[i] = repair_row(table_list[i])
    table_list[0] = repair_row(table_list[0])
    return [row for row in table_list if row[0] != "nan" or is_row_empty(row)]


def get_station_data(worksheet: pd.DataFrame) -> List[List[str]]:
    """
    Converts pandas DataFrame to List[List[str]] with stations data
    :param worksheet: pandas DataFrame of stations data
    :return: List[List[str]] with stations data
    """
    data_list = [[str(cell) for cell in ndarray] for ndarray in worksheet.values]  # Because I can't change np._str -_-

    data_first_row: int = -1
    for i in range(len(data_list)):
        if '№' in data_list[i][0]:
            data_first_row = i + 1
            break
    if data_first_row == -1:
        print("Start of the small stations table not found")
        exit(-1)

    data_last_row: int = -1
    for i in range(len(data_list) - 1, -1, -1):
        if '_' in data_list[i][0]:
            data_last_row = i
            break

    if data_last_row == -1:
        print("End of the small stations table not found")
        exit(-1)

    return repair_table(data_list[data_first_row: data_last_row])


def station_exists(cursor: sqlite3.Cursor, station_code: str) -> bool:
    """
    Check if the station with given code exists
    :param cursor: Cursor to the railroads.db
    :param code: Station's code
    :return: True if station in the table else False
    """
    count_query = "SELECT COUNT(*) FROM r_transportation_railroad_stations WHERE code=(?)"
    return True if cursor.execute(count_query, (station_code,)).fetchall()[0][0] else False


def get_table_date(station_worksheet: pd.DataFrame) -> str:
    """
    Looks for the table creation date (should be a first row in the table) and return SQL format date if found
    In my example date was written as "Среда 09 окт.2019г.  17:59"
    :param station_worksheet: pandas DataFrame object of worksheet from Kniga_2...xls
    :return: string of date like "2019-10-09" or ''
    """
    month_dict = {"янв": "01", "фев": "02", "мар": "03", "апр": "04", "май": "05", "июн": "06",
                  "июл": "07", "авг": "08", "сен": "09", "окт": "10", "ноя": "11", "дек": "12"}

    date_cell = station_worksheet.values[0][0]
    date_elements = date_cell.split(' ')  # ["Среда", "09", "окт.2019г.", '', "17:59"]

    day = date_elements[1]

    month, year, empty = date_elements[2].split('.')  # "окт.2019г." -> ["окт", "2019", '']

    if month in month_dict:
        month = month_dict[month]
        return f"{year[:-1]}-{month}-{day}"
    return ''


def get_actuality_column(station_table: List[List[str]]) -> List[bool]:
    """
    Generates column of station actuality. The station is actual if it's the only station with such name OR
    It's a station on a Russian railroad (contains "(Р)" in the railroad column)
    :param station_table:
    :return:
    """
    actuality = [True] * len(station_table)
    i = 1
    while i < len(station_table):
        if station_table[i][1] == station_table[i - 1][1]:  # If the same name for different stations
            k = i - 1
            while station_table[k][1] == station_table[i][1]:
                if "(Р)" not in station_table[k][3]:  # [3] is the railroad column of the worksheet
                    actuality[k] = False  # So if it is not a Russian railroad - set it's actuality to False
                k += 1
            i = k - 1  # When we found a different named station we should jump back for a step to continue the loop
        i += 1
    return actuality


def insert_stations(cursor: sqlite3.Cursor, station_table: List[List[str]], 
                    actuality_column: List[bool], station_type: str) -> None:
    """
    Insert all worksheet's station data to r_transportation_railroad_stations table
    :param cursor: Cursor of railroads.db
    :param station_table: Table of worksheet's data
    :param actuality_column: List with values True of False for each station
    :param station_type: "ОП" or "РП"
    :return: None
    """
    insert_station_query = """
    INSERT INTO r_transportation_railroad_stations (actuality, name, code, railroad_code, type)
    VALUES (?, ?, ?, ?, ?)"""
    update_station_query = """
    UPDATE r_transportation_railroad_stations 
    SET actuality = (?), name = (?), railroad_code = (?), type = (?)
    WHERE code = (?)"""

    code_column = -1
    if station_type == SMALL_TYPE_CODE:
        code_column = 4
    elif station_type == BIG_TYPE_CODE:
        code_column = 5

    for i in range(len(station_table)):
        station_name = station_table[i][1]
        station_code = station_table[i][code_column]
        actuality = actuality_column[i]
        railroad_code = get_railroad_code(station_table[i][3])

        if station_exists(cursor, station_code):
            cursor.execute(update_station_query, (actuality, station_name, railroad_code, station_type, station_code))
        else:
            cursor.execute(insert_station_query, (actuality, station_name, station_code, railroad_code, station_type))


def insert_operations(cursor: sqlite3.Cursor, station_table: List[List[str]], code_column: int = 4) -> None:
    """
    Insert all worksheet stations' operations data to r_transportation_station_operations table
    :param cursor: Cursor of railroads.db
    :param station_table: Table of worksheet's data
    :param code_column: Index of the column with station code (4 for the "ОП" and 5 for the "РП")
    :return: None
    """
    insert_operations_query = """
    INSERT OR REPLACE INTO r_transportation_station_operations (station_code, operation_code)
    VALUES (?, ?)"""

    for i in range(len(station_table)):
        station_code = station_table[i][code_column]
        station_operations = get_operation_codes(station_table[i][2])
        for operation in station_operations:
            cursor.execute(insert_operations_query, (station_code, operation))


def insert_stations_info(cursor: sqlite3.Cursor, station_worksheet: pd.DataFrame, station_type: str) -> None:
    """
    Insert data from a station table to the corresponding tables
    :param cursor: Cursor to the railroads.db
    :param station_worksheet: pandas DataFrame object with stations table
    :param station_type: "ОП" or "РП"
    :return: None
    """
    station_table = get_station_data(station_worksheet)
    actuality_column = get_actuality_column(station_table)

    code_column: int
    if station_type == SMALL_TYPE_CODE:
        code_column = 4
    elif station_type == BIG_TYPE_CODE:
        code_column = 5
    else:
        print(f"Unknown station type: {station_type}")
        return

    insert_stations(cursor, station_table, actuality_column, station_type)
    insert_operations(cursor, station_table, code_column)
    if code_column == 5:  # If code column is 5 - this is the worksheet with transit column
        insert_transit_distances(cursor, station_table)


def get_transit_dict(transit_distances_cell: str, code_from: str) -> Dict[str, int]:
    """
    Parses transit distances and return dict with station codes as keys and distances to them as values.
    In case station is a transit point - return dictionary with it's code and distance 0.
    If cell is empty ("nan") returns empty dict
    :param transit_distances_cell: transit distances cell of the "РП" worksheet
    :param code_from: code of the current station, required for the transit point cases
    :return: Dictionary with distances to transit points
    """
    if transit_distances_cell == "nan":
        return {}
    elif transit_distances_cell == "ТП":
        return {code_from: 0}
    else:
        transit_distances = transit_distances_cell.split(', ')  # "917103 Новый Ургал - 2090км, 927105 Лена - 489км" ->
        transit_dict: Dict[str, int] = {}  # -> ["917103 Новый Ургал - 2090км", "927105 Лена - 489км"]
        for transit in transit_distances:
            spit_transit = transit.split(' ')  # "917103 Новый Ургал - 2090км" ->["917103","Новый","Ургал","-","2090км"]
            code_to = spit_transit[0]
            distance = int(spit_transit[-1][:-2])  # "2090км" -> 2090
            transit_dict[code_to] = distance
        return transit_dict


def insert_transit_distances(cursor: sqlite3.Cursor, station_table: List[List[str]]) -> None:
    """
    Insert all transit distances to r_transportation_transit_distances
    :param cursor: Cursor to the railroads.db
    :param station_table: Table of station data (from Kniga_2...xls "РП")
    :return: None
    """
    insert_transit_query = """
    INSERT OR REPLACE INTO r_transportation_transit_distances (code_from, code_to, transit_distance) 
    VALUES (?, ?, ?)"""

    for i in range(len(station_table)):
        code_from = station_table[i][5]

        transit_dict = get_transit_dict(station_table[i][4], code_from)
        for code_to in transit_dict:
            cursor.execute(insert_transit_query, (code_from, code_to, transit_dict[code_to]))  # Station A conn to B
            cursor.execute(insert_transit_query, (code_to, code_from, transit_dict[code_to]))  # And B also conn to A
    return


def add_kniga2(cursor: sqlite3.Cursor, path_to_book2: str):
    """
    Insert or ipdate all data from Kniga_2...xls to railroads.db to tables r_transportation_railroad_stations,
    r_transportation_station_operations and r_transportation_transit_distances
    :param cursor: Cursor to the railroads.db
    :param path_to_book2: path to Kniga_2...xls
    :return: None
    """
    small_station_worksheet = pd.read_excel(path_to_book2, sheet_name="ОП", header=None, index_col=False)
    insert_stations_info(cursor, small_station_worksheet, station_type=SMALL_TYPE_CODE)
    big_station_worksheet = pd.read_excel(path_to_book2, sheet_name="РП", header=None, index_col=False)
    insert_stations_info(cursor, big_station_worksheet, station_type=BIG_TYPE_CODE)


if __name__ == "__main__":
    path_to_database = "railroads.db"
    path_to_book2 = "C:/Users/User/Desktop/ЖД/Kniga_2_2019-10-09.xls"
    connection = sqlite3.connect(path_to_database)
    db_cursor = connection.cursor()

    update_references(connection)
    create_tables(db_cursor)
    add_kniga2(db_cursor, path_to_book2)
    connection.commit()
    db_cursor.execute("VACUUM")
    print("Kniga2 has been added")
