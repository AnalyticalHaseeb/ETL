
import os, sys
from datetime import datetime
from Historical_data_load import *

def truncate_staging_tables(cursor, conn):
    """
    Description:
            This function is responsible for truncating all the
            staging database tables specified in truncate_Table_queries list

    Arguments:
            cur: the cursor object.
            conn: connection to the database.

    Returns:
            Nothing
    """

    try:

        for query in truncate_table_queries:
            cursor.execute(query)
        conn.commit()

    except ValueError as ex:
        print('Error: truncate table error')
        print(ex)


def merge_staging_with_dwh(cursor, conn):
    """
    Description:
            This function is responsible for truncating all the
            staging database tables specified in truncate_Table_queries list

    Arguments:
            cur: the cursor object.
            conn: connection to the database.

    Returns:
            Nothing
    """

    try:

        for query in dwh_merge_queries:
            cursor.execute(query)
        conn.commit()


    except ValueError as ex:
        print('Error: merge table error')
        print(ex)


def main():
    print(os.path.dirname(sys.executable))
    # Connect to SQL Server

    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=T5GSQLPBDEV;'
                          'Database=T5G_DEV;'
                          'Trusted_Connection=yes;')
    cursor = conn.cursor()




    print('ETL started at: ' +str(datetime.datetime.now()))
    print('Starting Truncate table')
    truncate_staging_tables(cursor, conn)

    print('Starting Staging Schema Insertion')
    staging_insert_main(cursor, conn)

    print('Running Merge Statements')
    merge_staging_with_dwh(cursor, conn)

    print('Running Historical Statements')
    #Historical_Data_load_method(cursor, conn)

    print('ETL Finished at: ' + str(datetime.datetime.now()))

if __name__ == "__main__":
    main()
