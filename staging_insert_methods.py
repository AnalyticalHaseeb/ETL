import pandas as pd
import re
import datetime
from dateutil.parser import parse
from datetime import date
import warnings
from staging_queries import *
from termcolor import colored
from colorama import Fore
from colorama import Style

def convert_check(val):
    try:
        float(val)
    except ValueError as ex:
        val = None

    return val


def get_trailing_number(val):
    try:
        chars_before_slash = val[:val.index("/")]
        result = get_trailing_number(chars_before_slash)
    except ValueError as ex:
        m = re.search(r'\d+$', val)
        result = int(m.group()) if m else None

    return result


def staging_dl_ri(insert_query, Path, sheet, skip_rows, cursor, conn):
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        data = pd.read_excel(pd.ExcelFile(Path), sheet, skiprows=skip_rows)

    cur_timestamp = datetime.datetime.now()
    for row in data.itertuples():
        cursor.execute(insert_query,
                       row[1],
                       str(row[2]),
                       str(row[3]),
                       get_trailing_number(row[3]),
                       convert_check(row[4]),
                       convert_check(row[5]),
                       convert_check(row[6]),
                       convert_check(row[7]),
                       convert_check(row[8]),
                       convert_check(row[9]),
                       cur_timestamp

                       )
    conn.commit()


def staging_usm_erab_accessibility(src, insert_query, Path, sheet, skip_rows, cursor, conn):
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        data = pd.read_excel(pd.ExcelFile(Path), sheet, skiprows=skip_rows)

    cur_timestamp = datetime.datetime.now()
    for row in data.itertuples():
        row_6 = None
        if src == 'kimbell':
            row_6 = convert_check(row[6])
        else:
            row_6 = None
        cursor.execute(insert_query,
                       row[1],
                       str(row[2]),
                       str(row[3]),
                       get_trailing_number(row[3]),
                       convert_check(row[4]),
                       convert_check(row[5]),
                       row_6,
                       cur_timestamp

                       )
    conn.commit()


def staging_usm_air_mac_packet(insert_query, Path, sheet, skip_rows, cursor, conn):
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        data = pd.read_excel(pd.ExcelFile(Path), sheet, skiprows=skip_rows)

    cur_timestamp = datetime.datetime.now()
    for row in data.itertuples():
        cursor.execute(insert_query,
                       row[1],
                       str(row[2]),
                       str(row[3]),
                       get_trailing_number(row[3]),
                       convert_check(row[4]),
                       convert_check(row[5]),
                       convert_check(row[6]),
                       convert_check(row[7]),
                       convert_check(row[8]),
                       convert_check(row[9]),
                       convert_check(row[10]),
                       convert_check(row[11]),
                       convert_check(row[12]),
                       convert_check(row[13]),
                       convert_check(row[14]),
                       convert_check(row[15]),
                       convert_check(row[16]),
                       convert_check(row[17]),
                       convert_check(row[18]),
                       cur_timestamp

                       )
    conn.commit()


def staging_usm_rrc_connection_establishment(src, insert_query, Path, sheet, skip_rows, cursor, conn):
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        data = pd.read_excel(pd.ExcelFile(Path), sheet, skiprows=skip_rows)

    cur_timestamp = datetime.datetime.now()
    for row in data.itertuples():

        row_16 = None
        if src == 'kimbell':
            row_6 = convert_check(row[6])
        else:
            row_16 = None

        cursor.execute(insert_query,
                       row[1],
                       str(row[2]),
                       str(row[3]),
                       get_trailing_number(row[3]),
                       convert_check(row[4]),
                       convert_check(row[5]),
                       convert_check(row[6]),
                       convert_check(row[7]),
                       convert_check(row[8]),
                       convert_check(row[9]),
                       convert_check(row[10]),
                       convert_check(row[11]),
                       convert_check(row[12]),
                       convert_check(row[13]),
                       convert_check(row[14]),
                       convert_check(row[15]),
                       row_16,
                       cur_timestamp

                       )
    conn.commit()


def staging_usm_availability(source, insert_query, Path, sheet, skip_rows, cursor, conn):
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        data = pd.read_excel(pd.ExcelFile(Path), sheet, skiprows=skip_rows)
    cur_timestamp = datetime.datetime.now()
    for row in data.itertuples():
        if source == "Kimbell":
            seven = convert_check(row[7])
            eight = convert_check(row[8])
            nine = convert_check(row[9])
        else:
            seven = None
            eight = None
            nine = None
        cursor.execute(insert_query,
                       row[1],
                       str(row[2]),
                       str(row[3]),
                       get_trailing_number(row[3]),
                       convert_check(row[4]),
                       convert_check(row[5]),
                       convert_check(row[6]),
                       seven,
                       eight,
                       nine,
                       cur_timestamp

                       )
    conn.commit()


def staging_usm_s1_connection_establishment(insert_query, Path, sheet, skip_rows, cursor, conn):
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        data = pd.read_excel(pd.ExcelFile(Path), sheet, skiprows=skip_rows)
    cur_timestamp = datetime.datetime.now()
    for row in data.itertuples():
        cursor.execute(insert_query,
                       row[1],
                       str(row[2]),
                       str(row[3]),
                       get_trailing_number(row[3]),
                       convert_check(row[4]),
                       convert_check(row[5]),
                       convert_check(row[6]),
                       convert_check(row[7]),
                       convert_check(row[8]),
                       convert_check(row[9]),
                       convert_check(row[10]),
                       cur_timestamp

                       )
    conn.commit()


def staging_usm_scell_added_information_kimbell(insert_query, Path, sheet, skip_rows, cursor, conn):
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        data = pd.read_excel(pd.ExcelFile(Path), sheet, skiprows=skip_rows)
    cur_timestamp = datetime.datetime.now()
    for row in data.itertuples():

        cursor.execute(insert_query,
                       row[1],
                       str(row[2]),
                       str(row[3]),
                       get_trailing_number(row[3]),
                       convert_check(row[4]),
                       convert_check(row[5]),
                       convert_check(row[6]),
                       convert_check(row[7]),
                       convert_check(row[8]),
                       convert_check(row[9]),
                       convert_check(row[10]),
                       convert_check(row[11]),
                       convert_check(row[12]),
                       convert_check(row[13]),
                       convert_check(row[14]),
                       convert_check(row[15]),
                       convert_check(row[16]),
                       cur_timestamp

                       )
    conn.commit()

def staging_usm_scell_added_information_samuel(insert_query, Path, sheet, skip_rows, cursor, conn):
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        data = pd.read_excel(pd.ExcelFile(Path), sheet, skiprows=skip_rows)
    cur_timestamp = datetime.datetime.now()
    for row in data.itertuples():

        cursor.execute(insert_query,
                       row[1],
                       str(row[2]),
                       str(row[3]),
                       get_trailing_number(row[3]),
                       None,
                       convert_check(row[4]),
                       convert_check(row[5]),
                       convert_check(row[6]),
                       convert_check(row[7]),
                       convert_check(row[8]),
                       convert_check(row[9]),
                       convert_check(row[10]),
                       convert_check(row[11]),
                       convert_check(row[12]),
                       convert_check(row[13]),
                       convert_check(row[14]),
                       convert_check(row[15]),
                       cur_timestamp

                       )
    conn.commit()


def staging_imsi_data(insert_query, Path, cursor, conn):
    data = pd.read_csv(Path)
    cur_timestamp = datetime.datetime.now()
    for row in data.itertuples():
        cursor.execute(insert_query,
                       parse(row[1]),
                       str(row[2]),
                       (row[3]).replace(",", ""),
                       cur_timestamp

                       )
    conn.commit()


def staging_unique_users(insert_query, Path, cursor, conn):
    data = pd.read_csv(Path)
    cur_timestamp = datetime.datetime.now()

    for row in data.itertuples():
        cursor.execute(insert_query,
                       parse(row[1]),
                       str(row[2]).replace(",", ""),
                       str(row[3]).replace(",", ""),
                       cur_timestamp
                       )


def staging_BlinQ(insert_query, Path, cursor, conn):
    data = pd.read_csv(Path)
    cur_timestamp = datetime.datetime.now()

    for row in data.itertuples():
        cursor.execute(insert_query,
                       str(row[0]),
                       str(row[1]),
                       str(row[2]),
                       str(row[3]),
                       str(row[4]),
                       str(row[5]),
                       str(row[6]),
                       str(row[7]),
                       str(row[8]),
                       str(row[9]),
                       str(row[10]),
                       str(row[11]),
                       str(row[12]),
                       str(row[13]),
                       str(row[14]),
                       str(row[15]),
                       str(row[16]),
                       str(row[17]),
                       str(row[18]),
                       str(row[19]),
                       str(row[20]),
                       str(row[21]),
                       str(row[22]),
                       str(row[23]),
                       str(row[24]),
                       str(row[25]),
                       str(row[26]),
                       str(row[27]),
                       str(row[28]),
                       str(row[29]),
                       str(row[30]),
                       str(row[31]),
                       str(row[32]),
                       str(row[33]),
                       str(row[34]),
                       str(row[35]),
                       str(row[36]),
                       str(row[37]),
                       str(row[38]),
                       str(row[39]),
                       str(row[40]),
                       str(row[41]),
                       str(row[42]),
                       str(row[43]),
                       str(row[44]),
                       str(row[45]),
                       str(row[46]),
                       str(row[47]),
                       str(row[48]),
                       str(row[49]),
                       str(row[50]),
                       str(row[51]),
                       str(row[52]),
                       str(row[53]),
                       str(row[54]),
                       str(row[55]),
                       str(row[56]),
                       str(row[57]),
                       str(row[58]),
                       str(row[59]),
                       str(row[60]),
                       str(row[61]),
                       str(row[62]),
                       str(row[63]),
                       str(row[64]),
                       str(row[65]),
                       str(row[66]),
                       str(row[67]),
                       str(row[68]),
                       str(row[69]),
                       str(row[70]),
                       str(row[71]),
                       str(row[72]),
                       str(row[73]),
                       str(row[74]),
                       str(row[75]),
                       str(row[76]),
                       str(row[77]),
                       str(row[78]),
                       str(row[79]),
                       str(row[80]),
                       str(row[81]),
                       str(row[82]),
                       str(row[83]),
                       str(row[84]),
                       str(row[85]),
                       str(row[86]),
                       str(row[87]),
                       str(row[88]),
                       str(row[89]),
                       str(row[90]),
                       str(row[91]),
                       str(row[92]),
                       str(row[93]),
                       str(row[94]),
                       str(row[95]),
                       str(row[96]),
                       str(row[97]),
                       str(row[98]),
                       str(row[99]),
                       str(row[100]),
                       cur_timestamp
                       )

    conn.commit()


def staging_Bec_Export(insert_query, Path, cursor, conn):
    data = pd.read_csv(Path)
    cur_timestamp = datetime.datetime.now()
    cur_Date = date.today()

    for row in data.itertuples():
        cursor.execute(insert_query,

                       str(row[1]),
                       str(row[2]),
                       str(row[3]),
                       str(row[4]),
                       str(row[5]),
                       str(row[6]),
                       str(row[7]),
                       str(row[8]),
                       str(row[9]),
                       str(row[10]),
                       str(row[11]),
                       str(row[12]),
                       str(row[13]),
                       str(row[14]),
                       str(row[15]),
                       str(row[16]),
                       str(row[17]),
                       str(row[18]),
                       str(row[19]),
                       str(row[20]),
                       str(row[21]),
                       str(row[22]),
                       str(row[23]),
                       str(row[24]),
                       str(row[25]),
                       str(row[26]),
                       str(row[27]),
                       str(row[28]),
                       str(row[29]),
                       str(row[30]),
                       str(row[31]),
                       str(row[32]),
                       str(row[33]),
                       str(row[34]),
                       str(row[35]),
                       str(row[36]),
                       str(row[37]),
                       str(row[38]),
                       str(row[39]),
                       str(row[40]),
                       str(row[41]),
                       str(row[42]),
                       str(row[43]),
                       str(row[44]),
                       str(row[45]),
                       str(row[46]),
                       str(row[47]),
                       str(row[48]),
                       str(row[49]),
                       str(row[50]),
                       str(row[51]),
                       str(row[52]),
                       str(row[53]),
                       str(row[54]),
                       str(row[55]),
                       cur_timestamp
                       )

    conn.commit()
    cursor.execute('exec [staging].[Bec_Export_cases_for_nulls]')
    conn.commit()


def staging_insert_main(cursor, conn):
    #############################################################################

    print('staging_usm_availability_kimbel')

    try:
        staging_usm_availability("Kimbel", insert_availability,
                                 path_availability_kimbel,
                                 sheet_name,
                                 skip_rows_kimbel,
                                 cursor,
                                 conn
                                 )
    except OSError as ex:

        print('**********************   Error: File Not Found *****************\n')



    print('staging_usm_availability_Samuel')

    try:
        staging_usm_availability('Samuel', insert_availability,
                                 path_availability_Samuel,
                                 sheet_name,
                                 skip_rows_samuel,
                                 cursor,
                                 conn
                                 )
    except OSError as ex:

        print('**********************   Error: File Not Found *****************\n')


#############################################################################

    print("staging_dl_ri_Kimbel")
    try:
        staging_dl_ri(insert_USM_DL_RI,
                      path_usm_dl_ri_kimbel,
                      sheet_name,
                      skip_rows_kimbel,
                      cursor,
                      conn)
    except OSError as ex:

        print('**********************   Error: File Not Found *****************\n')


    print("staging_dl_ri_Samuel")
    try:
        staging_dl_ri(insert_USM_DL_RI,
                      path_usm_dl_ri_samuel,
                      sheet_name,
                      skip_rows_samuel,
                      cursor,
                      conn)
    except OSError as ex:

        print('**********************   Error: File Not Found *****************\n')


##############################################################################

    print('staging_usm_erab_accessibility_Kimbell')
    try:
        staging_usm_erab_accessibility('Kimbell', insert_usm_erab_accessibility,
                                       path_usm_erab_accessibility_kimbel,
                                       sheet_name,
                                       skip_rows_kimbel,
                                       cursor,
                                       conn)
    except OSError as ex:

        print('**********************   Error: File Not Found *****************\n')


    print('staging_usm_erab_accessibility_samuel')
    try:
        staging_usm_erab_accessibility('Samuel', insert_usm_erab_accessibility,
                                       path_usm_erab_accessibility_samuel,
                                       sheet_name,
                                       skip_rows_samuel,
                                       cursor,
                                       conn)
    except OSError as ex:

        print('**********************   Error: File Not Found *****************\n')


    ##############################################################################

    print('staging_usm_air_mac_packet_kimbel')
    try:
        staging_usm_air_mac_packet(insert_air_mac_packet,
                                   path_air_mac_packet_kimbel,
                                   sheet_name,
                                   skip_rows_kimbel,
                                   cursor,
                                   conn
                                   )
    except OSError as ex:

        print('**********************   Error: File Not Found *****************\n')


    print('staging_usm_air_mac_packet_samuel')
    try:
        staging_usm_air_mac_packet(insert_air_mac_packet,
                                   path_air_mac_packet_Samuel,
                                   sheet_name,
                                   skip_rows_samuel,
                                   cursor,
                                   conn
                                   )
    except OSError as ex:

        print('**********************   Error: File Not Found *****************\n')


#############################################################################

    print('staging_usm_rrc_connection_establishment_kimbel')
    try:
        staging_usm_rrc_connection_establishment('Kimbell', insert_rrc_connection_establishment,
                                                 path_rrc_connection_establishment_kimbel,
                                                 sheet_name,
                                                 skip_rows_kimbel,
                                                 cursor,
                                                 conn
                                                 )
    except OSError as ex:

        print('**********************   Error: File Not Found *****************\n')


    print('staging_usm_rrc_connection_establishment_samuel')
    try:
        staging_usm_rrc_connection_establishment('Samuel', insert_rrc_connection_establishment,
                                                 path_rrc_connection_establishment_Samuel,
                                                 sheet_name,
                                                 skip_rows_samuel,
                                                 cursor,
                                                 conn
                                                 )
    except OSError as ex:

        print('**********************   Error: File Not Found *****************\n')


##############################################################################

    print('staging_usm_s1_connection_establishment_kimbel')
    try:
        staging_usm_s1_connection_establishment(insert_s1_connection_establishment,
                                                path_s1_connection_establishment_kimbell,
                                                sheet_name,
                                                skip_rows_kimbel,
                                                cursor,
                                                conn
                                                )

    except OSError as ex:

        print('**********************   Error: File Not Found ******************\n')


    print('staging_usm_s1_connection_establishment_Samuel')
    try:
        staging_usm_s1_connection_establishment(insert_s1_connection_establishment,
                                                path_s1_connection_establishment_samuel,
                                                sheet_name,
                                                skip_rows_samuel,
                                                cursor,
                                                conn
                                                )  #
    except OSError as ex:

        print('**********************   Error: File Not Found ******************\n')


#############################################################################


    print('staging_usm_scell_added_information_Kimbel')
    try:
        staging_usm_scell_added_information_kimbell(insert_Scell_added_information,
                                            path_scell_added_information_kimbell,
                                            sheet_name,
                                            skip_rows_kimbel,
                                            cursor,
                                            conn
                                            )
    except OSError as ex:

        print('**********************   Error: File Not Found *****************\n')


    print('staging_usm_scell_added_information_Samuel')
    try:
        staging_usm_scell_added_information_samuel(insert_Scell_added_information,
                                            path_scell_added_information_samuel,
                                            sheet_name,
                                            skip_rows_samuel,
                                            cursor,
                                            conn
                                            )
    except OSError as ex:

        print('**********************   Error: File Not Found *****************\n')


#############################################################################

    print('staging_imsi_data')
    try:
        staging_imsi_data(insert_imsi_data,
                          path_imsi_Data,
                          cursor,
                          conn
                          )
    except OSError as ex:

        print('**********************   Error: File Not Found *****************\n')


    print('staging_unique_users')
    try:
        staging_unique_users(insert_unqiue_users_data,
                             path_unique_users,
                             cursor,
                             conn
                             )
    except OSError as ex:

        print('**********************   Error: File Not Found *****************\n')


    # print('staging_BlinQ')
    # try:
    #     staging_BlinQ(insert_BlinQ,
    #                   path_blinq,
    #                   cursor,
    #                   conn
    #                   )
    # except OSError as ex:
    #
    #     print('**********************   Error: File Not Found *****************\n')
    #
    #
    # print('staging_Bec_Export')
    # try:
    #     staging_Bec_Export(insert_Bec_Export,
    #                        path_Bec_Export,
    #                        cursor,
    #                        conn
    #                        )
    # except OSError as ex:
    #
    #     print('**********************   Error: File Not Found *****************\n')
