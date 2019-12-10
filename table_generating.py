#! -*- encoding: utf-8 -*-
import sqlite3


def create_tables(cursor: sqlite3.Cursor):
    create_railroad_stations_query = """
    CREATE TABLE IF NOT EXISTS [r_transportation_railroad_stations](  -- Table with all stations
        [actuality] BOOL NOT NULL DEFAULT 1,
        [code] VARCHAR(6) PRIMARY KEY NOT NULL,
        [name] VARCHAR(60) NOT NULL,
        [railroad_code] VARCHAR(3) REFERENCES r_transportation_railroads([code]) ON DELETE CASCADE NOT NULL,
        [type] VARCHAR(2) NOT NULL);"""
    cursor.execute(create_railroad_stations_query)

    create_station_operations_query = """
    CREATE TABLE IF NOT EXISTS [r_transportation_station_operations](  -- Table with operations allowed for a station
    [station_code] VARCHAR(6) REFERENCES r_transportation_railroad_stations([code]) ON DELETE CASCADE,
    [operation_code] VARCHAR(3) REFERENCES r_transportation_operations([code]) ON DELETE CASCADE);
    CREATE UNIQUE INDEX IF NOT EXISTS [duplicate_preventing_operations]  -- Index preventing adding duplicates of operation to the same station
    ON [r_transportation_station_operations](
    [station_code], 
    [operation_code]);
    """
    cursor.executescript(create_station_operations_query)

    create_transit_distances_query = """
    CREATE TABLE IF NOT EXISTS [r_transportation_transit_distances](  -- Table of distances between stations to transit points / transit points
        [code_from] VARCHAR(6) REFERENCES r_transportation_railroad_stations([code]) ON DELETE CASCADE, 
        [code_to] VARCHAR(6) REFERENCES r_transportation_railroad_stations([code]) ON DELETE CASCADE, 
        [transit_distance] INTEGER);
    CREATE UNIQUE INDEX IF NOT EXISTS [duplicate_preventing_transit]  -- Index preventing adding duplicates of distance between the same stations
    ON [r_transportation_transit_distances](
    [code_from],  
    [code_to]);
    """
    cursor.executescript(create_transit_distances_query)

    create_railroad_parts_query = """
    CREATE TABLE IF NOT EXISTS [r_transportation_railroad_parts](  -- Table of railroad parts
        [code] VARCHAR(6) PRIMARY KEY NOT NULL,
        [name] VARCHAR(100) NOT NULL,
        [railroad_code] VARCHAR(3) REFERENCES r_transportation_railroads([code]) ON DELETE CASCADE);"""
    cursor.execute(create_railroad_parts_query)

    create_railroad_part_distances_query = """
    CREATE TABLE IF NOT EXISTS [r_transportation_railroad_part_distances](  -- Table of distances between two stations of one railroad part
        [part_code] VARCHAR(6) REFERENCES r_transportation_railroad_parts([code]) ON DELETE CASCADE,
        [code_from] VARCHAR(6) REFERENCES r_transportation_railroad_stations([code]) ON DELETE CASCADE, 
        [code_to] VARCHAR(6) REFERENCES r_transportation_railroad_stations([code]) ON DELETE CASCADE, 
        [distance_between_stations] INTEGER);
    CREATE UNIQUE INDEX IF NOT EXISTS [duplicate_preventing_part_distances]  -- Index preventing adding duplicates of distance between the same stations
    ON [r_transportation_railroad_part_distances](
    [code_from],  
    [code_to]);"""
    cursor.executescript(create_railroad_part_distances_query)


if __name__ == '__main__':
    path_to_database = "railroads.db"
    connection = sqlite3.connect(path_to_database)
    cursor = connection.cursor()
    create_tables(cursor)
