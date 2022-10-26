


skip_rows_kimbel = 12
skip_rows_samuel = 11
sheet_name = "Performance Statistics"

txt_file_path =r"C:\\SQLPub\\log.txt"

#### Paths
path_etl_archive = r"C:\\SQLPub\\ARCHIVE\\"
path_core_data = r"C:\\SQLPub\\CORE\\"
path_kimbel = r"C:\\SQLPub\\USM\\Kimball\\"
path_samuel = r"C:\\SQLPub\\USM\\Adamson.Samule\\"

dl_ri_file_name ="DL_RI.xlsx"
path_usm_dl_ri_kimbel = path_kimbel + dl_ri_file_name
path_usm_dl_ri_samuel = path_samuel + dl_ri_file_name

accessibility_file_name="Accessibility.xlsx"
path_usm_erab_accessibility_kimbel = path_kimbel + accessibility_file_name
path_usm_erab_accessibility_samuel = path_samuel + accessibility_file_name

air_mac_packet_file_name="Air_MAC_Packet.xlsx"
path_air_mac_packet_kimbel = path_kimbel + air_mac_packet_file_name
path_air_mac_packet_Samuel = path_samuel + air_mac_packet_file_name

rrc_connection_establishment ="RRC_Connection_Establishment.xlsx"
path_rrc_connection_establishment_kimbel = path_kimbel + rrc_connection_establishment
path_rrc_connection_establishment_Samuel = path_samuel + rrc_connection_establishment

availability_file_name ="Availability.xlsx"
path_availability_kimbel = path_kimbel + availability_file_name
path_availability_Samuel = path_samuel + availability_file_name

s1_connection_establishment_file_name="S1_Connection_Establishment.xlsx"
path_s1_connection_establishment_kimbell = path_kimbel + s1_connection_establishment_file_name
path_s1_connection_establishment_samuel = path_samuel + s1_connection_establishment_file_name

scell_added_information_file_name="SCell_Added_Information.xlsx"
path_scell_added_information_kimbell = path_kimbel + scell_added_information_file_name
path_scell_added_information_samuel = path_samuel + scell_added_information_file_name

imsi_file_name="Customer_DISD_IMSI.csv"
path_imsi_Data = path_core_data + imsi_file_name

unique_users_file_name= "Unique_Users.csv"
path_unique_users = path_core_data + unique_users_file_name

bling_file_name="BLINQ.csv"
path_blinq = "C:\\SQLPub\\CPE\\" + bling_file_name

bec_file_name="BEC_Report.csv"
path_Bec_Export = "C:\\SQLPub\\CPE\\"+ bec_file_name
