# -*- coding: utf-8 -*-
import parse_xml11
from typing import List
from datetime import date
import os
import sqlite3


def field_parser(field: dict) -> str:
    """
    Creates a sql valid field description from xml dictionary
    :param field: A xml dictionary with a field description
    :return: sql description of the field
    """
    field_type = field["type"]
    field_name = field["name"]
    tail = ''
    if field_name == "code":  # Code field is a unique field and serves as foreign key for other tables
        tail = "PRIMARY KEY NOT NULL"
    return f"{field_name} {field_type} {tail}"


def key_parser(foreign_key: dict) -> str:
    """
    Creates a sql query for a foreign key from xml dictionary
    :param foreign_key: A xml dictionary with a foreign key description
    :return: sql description of the foreign key with {} for the current table name
    """
    return f'FOREIGN KEY ({foreign_key["here"]}) REFERENCES {foreign_key["table"]} ({foreign_key["there"]})'


def create_table_query(xml_dict: dict) -> str:
    """
    Creates a sql query body from xml dictionary which can create a table with xml rTable name and specified fields and
    foreign keys
    :param xml_dict: A reference book in a xml dictionary
    :return: A body of the sql query
    """
    columns = xml_dict["ColumnsList"]["column"]
    columns = [field_parser(column) for column in columns]

    sql_query = "CREATE TABLE IF NOT EXISTS {}({})".format(xml_dict["rTable"], ', '.join(columns))
    return sql_query


def insert_values_query(xml_dict: dict) -> str:
    """
    Creates a body of sql query from xml dictionary which can insert values in a table with xml rTable name and
    specified fields
    :param xml_dict: A reference book in a xml dictionary
    :return: A body of the sql query
    """
    fields = xml_dict["ColumnsList"]["column"]
    fields = [field["name"] for field in fields]
    return "INSERT OR REPLACE INTO {} ({}) VALUES ({})".format(xml_dict["rTable"],
                                                               ', '.join(fields),
                                                               ', '.join(['?'] * len(fields)))


def get_query_values(xml_dict: dict) -> List[tuple]:
    """
    Creates a sql query from xml dictionary which inserts values in a table with xml rTable name and specified fields
    :param xml_dict: A reference book in a xml dictionary
    :return: Body of the sql query
    """
    fields = xml_dict["ColumnsList"]["column"]
    fields = [field["name"] for field in fields]
    values = [tuple(record[field] for field in fields) for record in xml_dict["RecordsList"]["record"]]
    return values


def date_from_string(date_string: str) -> date:
    """
    Converts a date in string form to a date from datetime.date
    :param date_string: Stringed date
    :return: An instance of datetime.date class
    """
    separator = '-'
    if '.' in date_string:
        separator = '.'
    elif ':' in date_string:
        separator = ':'
    split_date = date_string.split(separator)
    if len(split_date[0]) == 4:  # for YYYY:MM:DD forms
        return date(int(split_date[0]), int(split_date[1]), int(split_date[2]))
    elif len(split_date[2]) == 4:  # for DD:MM:YYYY forms
        return date(int(split_date[2]), int(split_date[1]), int(split_date[0]))
    else:
        # print("Wrong date format {}".format(date_string))
        exit(-1)


def collect_references(path_to_folder: str) -> List[str]:
    """
    Scan through folder and collect all references (xml files with *.spr names)
    :param path_to_folder: Path to folder with references/folders
    :return: list of paths to references
    """
    folder_items = [os.path.join(path_to_folder, folder) for folder in os.listdir(path_to_folder)]
    folders = [item for item in folder_items if not os.path.isfile(item)]
    references = [item for item in folder_items if os.path.isfile(item) and item.split('.')[-1] == "spr"]
    for folder in folders:
        references += collect_references(folder)
    return references


def update_references(connection, path_to_references = "Справочники") -> None:
    """
    Check if the data base contains all actual references from the path
    :param connection: Connection to the references' data base
    :param path_to_references: Path to a folder with references
    :return:
    """

    cursor = connection.cursor()

    create_table_script = "CREATE TABLE IF NOT EXISTS table_info (table_name VARCHAR(50), updating_date DATE)"
    cursor.execute(create_table_script)

    #  connection.set_trace_callback(lambda command: print(command))  # Print all database commands
    current_tables = cursor.execute("SELECT * FROM table_info").fetchall()

    current_tables = {table[0]:date_from_string(table[1]) for table in current_tables}

    references = collect_references(path_to_references)

    references = [parse_xml11.XmlDict.parse_xml(reference)["reference"] for reference in references]

    references = {r["rTable"]: {"Date": date_from_string(r["rDate"]),
                                "TableSQL": create_table_query(r),
                                "InsertSQL": insert_values_query(r),
                                "Values": get_query_values(r)}
                                 for r in references}

    for reference in references:
        if reference in current_tables:  # If a reference already exists
            if references[reference]["Date"] <= current_tables[reference]:
                # print("{} is up to date".format(reference))
                continue  # Do nothing if the reference is up to date
            else:  # Update the reference using INSERT OR UPDATE
                cursor.executemany(references[reference]["InsertSQL"], references[reference]["Values"])
                connection.commit()
                cursor.execute("UPDATE table_info SET updating_date='{}' "
                               "WHERE table_name='{}'".format(references[reference]["Date"], reference))
                connection.commit()
                # print("{} has been updated".format(reference))
        else:
            cursor.execute(references[reference]["TableSQL"])
            cursor.executemany(references[reference]["InsertSQL"], references[reference]["Values"])
            connection.commit()
            cursor.execute("INSERT INTO table_info (table_name, updating_date) "
                           "VALUES ('{}', '{}')".format(reference, references[reference]["Date"]))
            print("{} has been added".format(reference))
            connection.commit()



if __name__ == "__main__":
    path_to_db = "test.db"
    connection = sqlite3.connect(path_to_db)

    path_to_references = "D:/work/MyPyProjects/amazing_app/Справочники"
    update_references(connection, path_to_references)

    print("You are breathtaking!")
