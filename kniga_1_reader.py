#! -*- encoding: utf-8 -*-
from pandas import read_excel
import sqlite3
from typing import List, Tuple
from kniga_2_reader import repair_table


def get_parts_table(railroad_worksheet) -> List[List[str]]:
    """
    Takes data from pandas DataFrame and return List[List[str]] with table data
    :param railroad_worksheet: pandas DataFrame with railroad parts
    :return:
    """
    data_list = [[str(cell) for cell in ndarray] for ndarray in railroad_worksheet.values]

    first_row = 6  # Actual data starts from the 7th row of the worksheet
    last_row = - 1  # Default value. If it will be unchanged - end of the table not found
    for i in range(len(data_list) - 1, -1, -1):
        if '_' in data_list[i][0]:
            last_row = i
            break

    """
    ['№ п/п', 'Коды', 'от станции', 'до ст. Красный Луч', 'до ст. Блок-пост 16 км', 'nan']
    [' 1.', '502602 .', 'Красный Луч', ' 0 км', ' 2 км', 'nan']
    [' 2.', '502655 .', 'Блок-пост 16 км', ' 2 км', ' 0 км', 'nan']
    ['nan', 'nan', 'nan', 'nan', 'nan', 'nan']
    ['nan', 'nan', 'nan', 'nan', 'nan', 'nan']
    ['РАССТОЯНИЯ ДО ГОСУДАРСТВЕННОЙ ГРАНИЦЫ', 'nan', 'nan', 'nan', 'nan', 'nan']
    ['nan', 'nan', 'nan', 'nan', 'nan', 'nan']
    ['№ п/п', 'Коды', 'Станции', 'Расстояние', 'nan', 'nan']
    [' 1.', '508900 .', 'Граковка (эксп.)', ' 4 км', 'nan', 'nan']
    [' 2.', '487205 .', 'Квашино (эксп.)', ' 8 км', 'nan', 'nan']
    [' 3.', '500700 .', 'Красная Могила (эксп.)', ' 9 км', 'nan', 'nan']
    [' 4.', '485303 .', 'Мариуполь-Порт (перев.)', ' 0 км', 'nan', 'nan']
    [' 5.', '484902 .', 'Мариуполь-Порт (эксп.)', ' 0 км', 'nan', 'nan']
    [' 6.', '499407 .', 'Новозолотаревка (эксп.)', ' 0 км', 'nan', 'nan']
    [' 7.', '507409 .', 'Ольховая (эксп.)', ' 11 км', 'nan', 'nan']
    """
    for i in range(last_row - 1, -1, -1):
        if data_list[i][0] == "РАССТОЯНИЯ ДО ГОСУДАРСТВЕННОЙ ГРАНИЦЫ":  # We cant use this information cos there is only
            last_row = i - 2  # One distance - from border to a station. And no "border" station so this info is useless
            break  # If found - move two rows above and cut there

    if last_row == -1:
        print(f"End of the table {data_list[4][0]} not found!")

    return data_list[first_row: last_row]


def split_railroad_parts(parts_table: List[List[str]]) -> List[List[List[str]]]:
    parts = []
    part_first = 0  # First line of a part
    for i in range(len(parts_table)):
        if parts_table[i][0] == "nan":
            parts.append(parts_table[part_first: i])
            part_first = i + 1
    return parts


def get_part_info(part_label: str) -> Tuple[str, str]:
    """
    Search for the part code and part name
    :param part_label: String as a railroad part label. Something like:
    ["2) участок 55-002 "БАЛАДЖАРЫ - АЛЯТ" (Основной тарифный участок)", "nan", "nan", "nan", "nan"]
    :return: Tuple with (part code, part name)
    """
    part_code: str
    part_name: str
    for i in range(len(part_label)):
        if part_label[i] == 'к':  # The last letter of "участок" in "2) участок 55-002..."
            part_code = part_label[i + 2: i + 8]  # Part code begins 2 chars after and took 6 chars
            part_name = part_label[i + 9:]
            break
    return part_code, part_name


def repair_station_code(station_code_cell: str) -> str:
    """
    Reads station code cell and returns digits only
    :param station_code_cell: Station code cell sometimes contains values like "023202 ."
    :return: code only string
    """
    code_digits = []
    for char in station_code_cell:
        if char in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:
            code_digits.append(char)
    return ''.join(code_digits)


def repair_distance(distance_cell: str) -> int:
    """
    Reads distance cell and returns distance in integer from
    :param distance_cell: cell with distance value sometimes contains values like "   2  км "
    :return: integer value of distance
    """
    distance_digits = []
    for char in distance_cell:
        if char in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:
            distance_digits.append(char)
    return int(''.join(distance_digits))


def ger_regular_values(cursor: sqlite3.Cursor, railroad_part: List[List[str]],
                       part_code: str) -> List[Tuple[str, str, str, int]]:
    """
    Reads regular railroad part table and generate query values to insert in r_transportation_railroad_part_distances
    Regular part - with two columns of distances
    :param cursor: Cursor to the railroads.db
    :param railroad_part: List of Lists of strings with first element as label something like:
    ["1.", "230008 .", "Орехово-Зуево", "0 км", "36 км"] - A regular railroad part row (has two distance columns)
     with distance to the first transit point as 4th column and distance to the second transit point as 5th column
    :param part_code: code of the given railroad part in r_transportation_railroad_parts
    :return: List of (part code, station from code, station to code, distance)
    """
    if len(railroad_part) < 3:  # If part has two or less stations there is no sense to insert anything
        return []  # Because it's just two transit points and this information is in Kniga_3...xls

    select_id_query = "SELECT * FROM r_transportation_railroad_stations WHERE code = (?)"
    first_tp_code: str = repair_station_code(railroad_part[0][1])
    last_tp_code: str = repair_station_code(railroad_part[-1][1])

    first_tp_select = cursor.execute(select_id_query, (first_tp_code, )).fetchall()
    if len(first_tp_select) == 0:  # If SELECT returned (): tp with such code wasn't found - use second row as first row
        print(f"Station with code {first_tp_code} has not been found! It will not be added to part distances.")
        return ger_regular_values(cursor, railroad_part[1:], part_code)
    
    last_tp_select = cursor.execute(select_id_query, (last_tp_code, )).fetchall()
    if len(last_tp_select) == 0:  # If SELECT returned (): tp with such code wasn't found - use pre last row as last row
        print(f"Station with code {last_tp_code} has not been found! It will not be added to part distances.")
        return ger_regular_values(cursor, railroad_part[:-1], part_code)
    values = []

    """
    First and last rows are trivial - distance from tp1 to tp1 = 0, distance from tp1 to tp2 already in table
    r_transportation_transit_distances. So add only stations between transit points: 1, len(railroad_part) - 1
    """
    for i in range(1, len(railroad_part) - 1):
        station_code = repair_station_code(railroad_part[i][1])
        station_select = cursor.execute(select_id_query, (station_code, )).fetchall()
        if len(station_select) == 0:  # If SELECT returned (): station with such code wasn't found - pass it
            print(f"Station with code {station_code} has not been found! It will not be added to part distances.")
            continue
        tp1_distance = repair_distance(railroad_part[i][3])  # Distance to the first tp is in the 4th column
        tp2_distance = repair_distance(railroad_part[i][4])  # Distance to the second tp is in the 5th column
        values.append((part_code, station_code, first_tp_code, tp1_distance))
        values.append((part_code, station_code, last_tp_code, tp2_distance))
    return values


def get_irregular_values(cursor: sqlite3.Cursor, railroad_part: List[List[str]],
                         part_code: str) -> List[Tuple[str, str, str, int]]:
    """
    Reads irregular railroad part table and generate query values to insert in r_transportation_railroad_part_distances
    Regular part - with three columns of distances
    :param cursor: Cursor to the railroads.db
    :param railroad_part: List of Lists of strings with first element as label something like:
    ["1.", "230008 .", "Орехово-Зуево", "0 км", "36 км", "666 км"] - An irregular railroad part row (has three distance
    columns) with distance to the station which is included in a regular part as 4th column (any path at this railroad
    part will lay trough this station - main station / ms) distance to the first transit point as 5th column and
    distance to the second transit point as 6th column
    :param part_code: code of the given railroad part in r_transportation_railroad_parts
    return: List of (part code, station from code, station to code, distance)
    """
    select_id_query = "SELECT * FROM r_transportation_railroad_stations WHERE code = (?)"
    
    main_station_code = repair_station_code(railroad_part[0][1])  # Main station - the start of the branch
    main_station_select = cursor.execute(select_id_query, (main_station_code, )).fetchall()
    if len(main_station_select) == 0:  # If SELECT returned (): station with such code wasn't found - pass it
        print(f"""\n! Station with code {main_station_code} has not been found! 
                  Railroad part with such first station will be passed.""")
        return []  # Return empty list to not insert any values

    values = []
    for i in range(1, len(railroad_part)):  # First row is trivial - distance from main_station to main_station is 0
        station_code = repair_station_code(railroad_part[i][1])
        station_select = cursor.execute(select_id_query, (station_code,)).fetchall()
        if len(station_select) == 0:  # If SELECT returned (): station with such code wasn't found - pass it
            print(f"\n! Station with code {station_code} has not been found! It will not be added to part distances.\n")
            continue
        main_station_distance = repair_distance(railroad_part[i][3])  # Distance to the ms is in the 4th column
        values.append((part_code, station_code, main_station_code, main_station_distance))  # From A to B
        values.append((part_code, main_station_code, station_code, main_station_distance))  # From B to A
    return values


def get_query_values(cursor: sqlite3.Cursor, railroad_part: List[List[str]],
                     part_code: str) -> List[Tuple[str, str, str, int]]:
    """
    Reads railroad part table and generate query values to insert in r_transportation_railroad_part_distances
    :param cursor: Cursor to the railroads.db
    :param railroad_part: List of Lists of strings with first element as label something like:
    ["1.", "230008 .", "Орехово-Зуево", "0 км", "36 км"] - A regular railroad part row (has two distance columns)
     with distance to the first transit point as 4th column and distance to the second transit point as 5th column
    OR
    ["1.", "230008 .", "Орехово-Зуево", "0 км", "36 км", "666 км"] - An irregular railroad part row (has three distance
    columns) with distance to the station which is included in a regular part as 4th column (any path at this railroad
    part will lay trough this station) distance to the first transit point as 5th column and
    distance to the second transit point as 6th column
    :param part_code: code of the given railroad part in r_transportation_railroad_parts
    :return: list of tuples with (part code, code from station, code to station, distance)
    """
    if len(railroad_part[1]) == 5 or railroad_part[1][-1] == "nan":  # Third columns of distance is empty - regular part
        return ger_regular_values(cursor, railroad_part[2:], part_code)  # Second
    else:  # Third columns of distance is not empty - irregular part
        return get_irregular_values(cursor, railroad_part[2:], part_code)


def insert_part(cursor: sqlite3.Cursor, railroad_part: List[List[str]]) -> None:
    """
    Insert all data about the given railroad part to the cursor's database
    :param cursor: Cursor to the railroads.db
    :param railroad_part: List of Lists of strings with first element as label something like:
    ["2) участок 55-002 "БАЛАДЖАРЫ - АЛЯТ" (Основной тарифный участок)", "nan", "nan", "nan", "nan"]
    :return:
    """
    part_code, part_name = get_part_info(railroad_part[0][0])
    railroad_code = part_code[:2]

    part_exists_query = "SELECT * FROM r_transportation_railroad_parts WHERE code = (?)"
    part_exists_select = cursor.execute(part_exists_query, (part_code,)).fetchall()
    if len(part_exists_select) == 0:  # If part such code not exists in the railroads.db
        insert_part_query = """INSERT INTO r_transportation_railroad_parts (code, name, railroad_code) 
                               VALUES (?, ?, ?)"""
        cursor.execute(insert_part_query, (part_code, part_name, railroad_code))
    else:  # If part exists - the only field that should be updated - name (code, id and railroad should not change)
        update_part_query = "UPDATE r_transportation_railroad_parts SET name = (?) WHERE code = (?)"
        cursor.execute(update_part_query, (part_name, part_code))

    insert_distance_query = """INSERT OR REPLACE INTO r_transportation_railroad_part_distances 
    (part_code, code_from, code_to, distance_between_stations) VALUES (?, ?, ?, ?)"""
    values = get_query_values(cursor, railroad_part, part_code)
    for value in values:
        cursor.execute(insert_distance_query, value)


def insert_railroad_parts(cursor: sqlite3.Cursor, railroad_worksheet) -> None:
    """
    Inserts all railroad parts from Kniga_1...xls to the railroads.db
    :param cursor: Cursor to the railroads.db
    :param railroad_worksheet: Worksheet with railroad parts: 'Азерб', 'Бел', 'В-Сиб (Р)'...
    :return:
    """
    parts_table: List[List[str]] = repair_table(get_parts_table(railroad_worksheet))

    railroad_parts: List[List[List[str]]] = split_railroad_parts(parts_table)

    for part in railroad_parts:
        insert_part(cursor, part)
    return


def add_kniga1(cursor: sqlite3.Cursor, path_to_kniga1: str,
               unused_worksheets: Tuple[str, str] = ("Общие положения", "Вводные положения")):
    """
    Reads Kniga_1_...xls from РЖД and insert or update all data in railroads.db
    :param cursor: cursor to the railroads.db
    :param path_to_kniga1:
    :param unused_worksheets: path to Kniga_1_...xls
    :return: None
    """
    worksheets = list(read_excel(path_to_kniga1, sheet_name=None).keys())
    for worksheet in worksheets:
        if worksheet not in unused_worksheets:
            railroad_worksheet = read_excel(path_to_kniga1, sheet_name=worksheet, header=None, index_col=False)
            insert_railroad_parts(cursor, railroad_worksheet)
            print(f"Kniga_1 {worksheet} complete")


if __name__ == "__main__":
    path_to_database = "railroads.db"
    connection = sqlite3.connect(path_to_database)
    db_cursor = connection.cursor()

    path_to_kniga1 = "C:/Users/User/Desktop/ЖД/Kniga_1_2019-10-09.xls"
    add_kniga1(db_cursor, path_to_kniga1)

    connection.commit()
    db_cursor.execute("VACUUM")
    print("Complete")
