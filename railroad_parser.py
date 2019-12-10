#! -*- encoding: utf-8 -*-
import sqlite3
from references import update_references
from table_generating import create_tables
from kniga_1_reader import add_kniga1
from kniga_2_reader import add_kniga2
from kniga_3_reader import add_kniga3
from datetime import date
import os


HELP = """
  This script parses Kniga_1...xls, Kniga_2...xls, Kniga_3...xls 
  from current directory, inserts all data to railroads.db 
  and generates .spr files for each database table
   
  !!! Notice that folder "Справочники" is required with next
      files insisde:
      tp0003.spr - r_transportation_railroads, an xml file 
      with railroad reference
      tp0005.spr - r_transportation_railroad_operations, an xml file
      with station operations reference
  
  Данный скрипт парсит Kniga_1...xls, Kniga_2...xls, Kniga_3...xls 
  из текущей директроии, добавляет данные в railroads.db
  и генерирует .spr файлы для каждой таблице в базе
  
  !!! Обратите внимание, что папка "Справочники" необходима
  для работы, со следующими файламиЖ
      tp0003.spr - r_transportation_railroads, xml файл 
      со справочником по железным дорогам
      tp0005.spr - r_transportation_railroad_operations, xml файл 
      со справочником по станционным операциям
"""


def write_reference(data_dict: dict, columns_dict: dict, xml_name: str, table_name: str) -> bool:
    """
    Writes xml file from two dictionary of columns and dictionary of data from SQL table
    :param data_dict: Dictionary with column name as key and list of values as value
    :param columns_dict: Dictionary with column name as key and dictionary with column type, caption as value
    :param xml_name: Name of the generating xml
    :param table_name: Name for the rTable field of the generating xml
    :return:
    """
    try:
        with open("%s" % xml_name, 'w', encoding="utf-8") as file:
            file.close()
    except:
        return False

    with open("%s" % xml_name, 'w', encoding="utf-8") as file:
        current_date = ('%s' % date.today()).split('-')
        current_date = '.'.join([current_date[2], current_date[1], current_date[0]])
        header = f"""<?xml version="1.1"?>
<reference>
  <rTable>{table_name}</rTable>
  <rName></rName>
  <rDate>{current_date}</rDate>\n"""
        file.write(header)
        records_number = len(data_dict[list(data_dict.keys())[0]])
        file.write(f"  <rRecords>{records_number}</rRecords>\n")
        file.write("  <ColumnsList>\n")
        for column in columns_dict:
            column_type = columns_dict[column]["type"]
            caption = columns_dict[column]["caption"]
            file.write(f'    <column name="{column}" type="{column_type}" caption="{caption}"/>\n')
        file.write("  </ColumnsList>\n  <RecordsList>\n")

        row_blank = ' '.join(["%s=\"{}\"" % column for column in columns_dict])
        for i in range(records_number):
            row = row_blank.format(*[data_dict[column][i] for column in data_dict])
            file.write("    <record %s/>\n" % row)
        file.write("  </RecordsList>\n</reference>")

        file.close()
        return True


def get_columns_dict(cursor: sqlite3.Cursor, table_name: str) -> dict:
    """
    Generates dictionary with a table column name in database as key and dict {"type": TYPE, "caption": ''} as value
    :param cursor: Cursor to a database
    :param table_name: name of the table in the database
    :return: dictionary with column name as keys and property dict as values
    """
    columns_info = cursor.execute(f"PRAGMA table_info({table_name})").fetchall()
    info_dict = {}
    for column_info in columns_info:
        name = column_info[1]
        column_type = column_info[2]
        if name == "code":
            column_type = f"{column_type} PRIMARY KEY"
        elif name == "railroad_code":
            column_type = f"{column_type} NOT NULL REFERENCES r_transportation_railroads(code)"
        elif name == "part_code":
            column_type = f"{column_type} NOT NULL REFERENCES r_transportation_railroad_parts(code)"
        elif name == "code_from":
            column_type = f"{column_type} NOT NULL REFERENCES r_transportation_railroad_stations(code)"
        elif name == "code_to":
            column_type = f"{column_type} NOT NULL REFERENCES r_transportation_railroad_stations(code)"
        elif name == "operation_code":
            column_type = f"{column_type} NOT NULL REFERENCES r_transportation_operations(code)"
        info_dict[name] = {"type": column_type, "caption": ''}
    return info_dict


def get_data_dict(cursor: sqlite3.Cursor, table_name: str) -> dict:
    """
    Generates dictionary of table data with column names as key and list of column data as values
    :param cursor: Cursor to a database
    :param table_name: name of the table in the database
    :return: dictionary with column name as keys and list of column data as values
    """
    columns_dict = get_columns_dict(cursor, table_name)
    data = cursor.execute(f"SELECT * FROM {table_name}").fetchall()

    data_dict = {column: [] for column in columns_dict}
    for i in range(len(data)):
        k = 0
        for column in data_dict:
            data_dict[column].append(data[i][k])
            k += 1
    return data_dict


def generate_xml(cursor: sqlite3.Cursor) -> None:
    """
    Generates an xml files for each table in the railroads.db
    :param cursor: Cursor to the railroads.db
    :return:
    """
    if not os.path.exists("references"):
        os.mkdir("references")

    tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'table_info'").fetchall()
    tables = [table_info[0] for table_info in tables]
    for table in tables:
        columns_dict = get_columns_dict(cursor, table)  # Dictionary with columns' names as keys and property dict
        data_dict = get_data_dict(cursor, table)  # Dictionary with columns' names as keys and columns' data lists
        write_reference(data_dict, columns_dict, f"references/{table}.spr", table)
        print(f"{table}.spr created")
    return


def generate_database(path_to_database: str, path_to_kniga1: str, path_to_kniga2: str, path_to_kniga3: str):
    """
    Parses three xls books of railroad open data and create/updates tables in database from given path
    :param path_to_database: path to database where tables should be created
    :param path_to_kniga1: path to Kniga_1...xls file
    :param path_to_kniga2: path to Kniga_2...xls file
    :param path_to_kniga3: path to Kniga_3...xls file
    :return:
    """
    connection = sqlite3.connect(path_to_database)
    db_cursor = connection.cursor()

    update_references(connection)
    create_tables(db_cursor)

    add_kniga2(db_cursor, path_to_kniga2)  # Read kniga2 first because it contains all stations
    connection.commit()
    print("Kniga_2 data has been inserted\n")

    add_kniga1(db_cursor, path_to_kniga1)
    connection.commit()
    print("Kniga_1 data has been inserted\n")

    add_kniga3(db_cursor, path_to_kniga3)
    connection.commit()
    db_cursor.execute("VACUUM")
    connection.commit()
    print("Kniga_3 data has been inserted\n")
    print("Complete")


if __name__ == "__main__":
    print(HELP)

    current_folder = os.path.dirname(os.path.realpath(__file__))
    file_list = os.listdir(current_folder)
    xls_files = [file for file in file_list if file.split('.')[-1] == "xls"]

    path_to_kniga1 = ""
    path_to_kniga2 = ""
    path_to_kniga3 = ""

    for xls in xls_files:
        if xls[:8] == "Kniga_1_":
            path_to_kniga1 = xls
        elif xls[:8] == "Kniga_2_":
            path_to_kniga2 = xls
        elif xls[:8] == "Kniga_3_":
            path_to_kniga3 = xls

    if path_to_kniga1 == '' or path_to_kniga2 == '' or path_to_kniga3 == '':
        print("  Not all xls found! Make sure all three files Kniga_1_...xls,\n"
              "  Kniga_2_...xls, Kniga_3_...xls in the folder\n\n")
        print("  Не все xls обнаружены! Убедитесь, что файлы Kniga_1_...xls,\n"
              "  Kniga_2_...xls, Kniga_3_...xls находятся в директории\n\n")
        input("Press any key to continue")
    else:
        if not os.path.exists(os.path.join(current_folder, "Справочники")):
            print("  Folder \"Справочники\" was not found!\n"
                  "  Make sure the folder is the same directory as script\n\n")
            input("Press any key to continue")
        elif os.path.exists("Справочники/tp0003.spr"):
            print("  Reference tp0003.spr - r_transportation_railroads was not found!\n"
                  "  Make sure the file is the Справочники folder\n\n")
            input("Press any key to continue")
        elif os.path.exists("Справочники/tp0005.spr"):
            print("  Reference tp0005.spr - r_transportation_railroad_operations was not found!\n"
                  "  Make sure the file is the Справочники folder\n\n")
            input("Press any key to continue")
        else:
            path_to_database = "railroads.db"

            generate_database(path_to_database, path_to_kniga1, path_to_kniga2, path_to_kniga3)

            connection = sqlite3.connect(path_to_database)
            db_cursor = connection.cursor()

            generate_xml(db_cursor)
            input("\nComplete.")
