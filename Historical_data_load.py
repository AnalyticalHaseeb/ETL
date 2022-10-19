import pandas as pd
import pyodbc
import re
import datetime
from staging_insert_methods import *


def usm_kpi_cell_level_s1connestab(path, cursor, conn):
    s1_conn_estab = pd.read_excel(path, r'S1ConnEstab')

    cur_timestamp = datetime.datetime.now()

    for row in s1_conn_estab.itertuples():
        cursor.execute('''
                   INSERT INTO [hist].[USM_KPI_CELL_LEVEL_S1ConnEstab]
                   ([Date]
                   ,[Time]
                  , [NE]
                   ,[Index]
                   ,[cellid]
                   ,[S1ConnEstabAtt]
                   ,[S1ConnEstabSucc]
                   ,[S1ConnEstabFail_CpCcFail]
                   ,[S1ConnEstabFail_S1apCuFail]
                   ,[S1ConnEstabFail_S1apLinkFail]
                  ,[S1ConnEstabFail_S1apSigFail]
                   ,[S1ConnEstabFail_S1apSigTo]
                   ,created_at
                   ,last_updated_at
                   )
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                          ''',
                       row[1],
                       str(row[2]),
                       str(row[3]),
                       str(row[4]),
                       get_trailing_number(row[4]),
                       convert_check(row[5]),
                       convert_check(row[6]),
                       convert_check(row[7]),
                       convert_check(row[8]),
                       convert_check(row[9]),
                       convert_check(row[10]),
                       convert_check(row[11]),
                       cur_timestamp,
                       cur_timestamp

                       )
    conn.commit()
    cursor.execute('''
insert into dwh.[USM_KPI_CELL_LEVEL_S1ConnEstab]
select
       [Date]
      ,[Time]
      ,[NE]
      ,[Index]
      ,max([cellid])
      ,max([S1ConnEstabAtt])
      ,max([S1ConnEstabSucc])
      ,max([S1ConnEstabFail_CpCcFail])
      ,max([S1ConnEstabFail_S1apCuFail])
      ,max([S1ConnEstabFail_S1apLinkFail])
      ,max([S1ConnEstabFail_S1apSigFail])
      ,max([S1ConnEstabFail_S1apSigTo])
      ,max([created_at])
      ,max([last_updated_at])

from  HIST.[USM_KPI_CELL_LEVEL_S1ConnEstab]
where concat( [Date],[Time],[NE],[Index])
      not in ( select  concat(  [Date],[Time],[NE],[Index]) from dwh.[USM_KPI_CELL_LEVEL_S1ConnEstab]
	   )
group by
       [Date]
      ,[Time]
      ,[NE]
      ,[Index]
                        ''')
    conn.commit()


def usm_kpi_cell_level_dl_ri(usm_source, cursor, conn):
    dl_ri_data = pd.read_excel(usm_source, r'DL RI')
    cur_timestamp = datetime.datetime.now()

    for row in dl_ri_data.itertuples():
        cursor.execute('''
                   INSERT INTO [hist].[USM_KPI_CELL_LEVEL_DL_RI]
           ([Date]
           ,[Time]
           ,[NE]
           ,[Index]
           ,cellid
           ,[DLReceivedRI0]
           ,[DLReceivedRI1]
           ,[DLReceivedRI2]
           ,[DLReceivedRI3]
           ,[DLReceivedRI4]
           ,[DLReceivedRIAvg]
           ,[created_at]
           ,[last_updated_at]
      )
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                          ''',
                       row[1],
                       str(row[2]),
                       str(row[3]),
                       str(row[4]),
                       get_trailing_number(row[4]),
                       convert_check(row[5]),
                       convert_check(row[6]),
                       convert_check(row[7]),
                       convert_check(row[8]),
                       convert_check(row[9]),
                       convert_check(row[10]),
                       cur_timestamp,
                       cur_timestamp
                       )
    conn.commit()
    cursor.execute('''insert into  [T5G_DEV].[dwh].[USM_KPI_CELL_LEVEL_DL_RI]
SELECT [Date]
      ,[Time]
      ,[NE]
      ,[Index]
      ,[cellid]
      ,max([DLReceivedRI0])
      ,max([DLReceivedRI1])
      ,max([DLReceivedRI2])
      ,max([DLReceivedRI3])
      ,max([DLReceivedRI4])
      ,max([DLReceivedRIAvg])
      ,max([created_at])
	  ,max([last_updated_at])
  FROM [T5G_DEV].[hist].[USM_KPI_CELL_LEVEL_DL_RI]
  where concat( [Date],[Time],[NE],[Index])
      not in ( select  concat(  [Date],[Time],[NE],[Index]) from  [T5G_DEV].dwh.[USM_KPI_CELL_LEVEL_DL_RI]
	   )
group by 
	  [Date]
      ,[Time]
      ,[NE]
      ,[Index]
      ,[cellid]
''')

    conn.commit()


def usm_kpi_cell_level_erab_accessibility(usm_source, cursor, conn):
    erab_accessibility = pd.read_excel(usm_source, r'ERAB Accessibility')
    cur_timestamp = datetime.datetime.now()
    for row in erab_accessibility.itertuples():
        cursor.execute('''
            INSERT INTO [hist].[USM_KPI_CELL_LEVEL_ERAB_Accessibility]
    ([Date]
      ,[Time]
      ,[NE]
      ,[Index]
      ,[CellId]
      ,[ErabAccessibilityInit]
      ,[ErabAccessibilityAdd]
      ,[ErabConnectionFailureRate]
      ,[created_at]
      ,[last_updated_at]
      )
     VALUES (?,?,?,?,?,?,?,?,?,?)
                          ''',
                       row[1],
                       str(row[2]),
                       str(row[3]),
                       str(row[4]),
                       get_trailing_number(row[4]),
                       convert_check(row[5]),
                       convert_check(row[6]),
                       convert_check(row[7]),
                       cur_timestamp,
                       cur_timestamp

                       )
    conn.commit()
    cursor.execute('''
insert into  dwh.[USM_KPI_CELL_LEVEL_ERAB_Accessibility]

SELECT [Date]
      ,[Time]
      ,[NE]
      ,[Index]
      ,max([CellId])
      ,max([ErabAccessibilityInit])
      ,max([ErabAccessibilityAdd])
      ,max([ErabConnectionFailureRate])
      ,max([created_at])
      ,max([last_updated_at])
  FROM [T5G_DEV].[hist].[USM_KPI_CELL_LEVEL_ERAB_Accessibility]
  where concat( [Date],[Time],[NE],[Index])
      not in ( select  concat(  [Date],[Time],[NE],[Index]) from dwh.[USM_KPI_CELL_LEVEL_ERAB_Accessibility]
	   )

  group by 
       [Date]
      ,[Time]
      ,[NE]
      ,[Index]''')
    conn.commit()


def usm_kpi_cell_level_availability(usm_source, cursor, conn):
    availability = pd.read_excel(usm_source, r'Availability')
    cur_timestamp = datetime.datetime.now()
    for row in availability.itertuples():
        cursor.execute('''
            INSERT INTO [hist].[USM_KPI_CELL_LEVEL_Availability]
           (
      [Date]
      ,[Time]
      ,[NE]
      ,[Index]
      ,[cellID]
      ,[EutranCellAvailability]
      ,[ReadCellUnavailableTime]
      ,[CellAvailPmPeriodTime]
      ,[created_at]
      ,[last_updated_at]
           )
     VALUES (?,?,?,?,?,?,?,?,?,?)
                          ''',
                       row[1],
                       str(row[2]),
                       str(row[3]),
                       str(row[4]),
                       get_trailing_number(row[4]),
                       convert_check(row[5]),
                       convert_check(row[6]),
                       convert_check(row[7]),
                       cur_timestamp,
                       cur_timestamp

                       )
    conn.commit()

    conn.execute('''/****** Script for SelectTopNRows command from SSMS  ******/
insert into  [T5G_DEV].[dwh].[USM_KPI_CELL_LEVEL_Availability]

SELECT [Date]
      ,[Time]
      ,[NE]
      ,[Index]
      ,[cellID]
      ,max([EutranCellAvailability])
      ,max([ReadCellUnavailableTime])
      ,max([CellAvailPmPeriodTime])
	  ,null,
	   null,
	   null
      ,max([created_at])
      ,max([last_updated_at])
  FROM [T5G_DEV].[hist].[USM_KPI_CELL_LEVEL_Availability]
  where concat( [Date],[Time],[NE],[Index])
      not in ( select  concat(  [Date],[Time],[NE],[Index]) from   [T5G_DEV].[dwh].[USM_KPI_CELL_LEVEL_Availability]
	   )

  group by 
  [Date]
      ,[Time]
      ,[NE]
      ,[Index]
      ,[cellID]''')
    conn.commit()



def usm_kpi_cell_rrc_conn_est(usm_source, cursor, conn):
    rrc_connection_establishment = pd.read_excel(usm_source, r'RRC Connection Establishment')
    cur_timestamp = datetime.datetime.now()
    for row in rrc_connection_establishment.itertuples():
        cursor.execute('''
            INSERT INTO [hist].[USM_KPI_CELL_RRC_Conn_Est]
           ([Date]
           ,[Time]
           ,[NE]
           ,[Index]
           ,cellid
           ,[ConnEstabAtt]
           ,[ConnEstabSucc]
           ,[ConnEstabFail_CpCcTo]
           ,[ConnEstabFail_CpCcFail]
           ,[ConnEstabFail_UpMacFail]
           ,[ConnEstabFail_UpPdcpFail]
           ,[ConnEstabFail_UpRlcFail]
           ,[ConnEstabFail_RrcSigTo]
           ,[ConnEstabFail_S1apLinkFail]
           ,[ConnEstabFail_S1apSigFail]
           ,[ConnEstabReject_CpCcFail]
           ,[ConnEstabReject_CpCapaCacFail]
           ,[ConnEstabReject_S1apMmeOvld]
           ,created_at
           ,last_updated_at
           )
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                          ''',
                       row[1],
                       str(row[2]),
                       str(row[3]),
                       str(row[4]),
                       get_trailing_number(row[4]),
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
                       cur_timestamp,
                       cur_timestamp
                       )

    conn.commit()
    conn.execute('''/****** Script for SelectTopNRows command from SSMS  ******/
insert into  [T5G_DEV].[dwh].[USM_KPI_CELL_RRC_Conn_Est]

SELECT [Date]
      ,[Time]
      ,[NE]
      ,[Index]
      ,max([CellID])
      ,max([ConnEstabAtt])
      ,max([ConnEstabSucc])
      ,max([ConnEstabFail_CpCcTo])
      ,max([ConnEstabFail_CpCcFail])
      ,max([ConnEstabFail_UpMacFail])
      ,max([ConnEstabFail_UpPdcpFail])
      ,max([ConnEstabFail_UpRlcFail])
      ,max([ConnEstabFail_RrcSigTo])
      ,max([ConnEstabFail_S1apLinkFail])
      ,max([ConnEstabFail_S1apSigFail])
      ,max([ConnEstabReject_CpCcFail])
      ,max([ConnEstabReject_CpCapaCacFail])
      ,max([ConnEstabReject_S1apMmeOvld])
      ,max([Created_at])
      ,max([last_updated_at])
  FROM [T5G_DEV].[hist].[USM_KPI_CELL_RRC_Conn_Est]
   where concat( [Date],[Time],[NE],[Index])
           not in ( select  concat( [Date],[Time],[NE],[Index]) from   [T5G_DEV].[dwh].[USM_KPI_CELL_RRC_Conn_Est]
	   )

  group by 
  [Date]
      ,[Time]
      ,[NE]
      ,[Index]''')
    conn.commit()


def usm_kpi_cell_traffic_dl_ul(usm_source, cursor, conn):
    traffic_dl_ul = pd.read_excel(usm_source, r'Traffic- DL+UL')
    cur_timestamp = datetime.datetime.now()
    for row in traffic_dl_ul.itertuples():
        cursor.execute('''
           INSERT INTO [hist].[USM_KPI_CELL_Traffic_DL_UL]
           ([Date]
           ,[Time]
           ,[NE]
           ,[Index]
           ,[CellId]
           ,[AirMacULByte_Kbytes]
           ,[AirMacULTti_TTI]
           ,[AirMacULThruAvg_Kbps]
           ,[AirMacULEfctivThruAvg_Kbps]
           ,[AirMacDLByte_Kbytes]
           ,[AirMacDLTti_TTI]
           ,[AirMacDLThruAvg_Kbps]
           ,[AirMacDLEfctivThruAvg_Kbps]
           ,[AirMacULThruMin_Kbps]
           ,[AirMacULThruMax_Kbps]
           ,[AirMacDLThruMin_Kbps]
           ,[AirMacDLThruMax_Kbps]
           ,[ULIpThruVol_Kbytes]
           ,[ULIpThruTime_TTI]
           ,[ULIpThruAvg_Kbps]
           ,[created_at]
           ,[last_updated_at])
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                          ''',
                       row[1],
                       str(row[2]),
                       str(row[3]),
                       str(row[4]),
                       get_trailing_number(row[4]),
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
                       convert_check(row[19]),
                       cur_timestamp,
                       cur_timestamp

                       )

    conn.commit()
    print('step 2')
    conn.execute('''
insert into  [T5G_DEV].[dwh].[USM_KPI_CELL_Traffic_DL_UL]

SELECT [Date]
      ,[Time]
      ,[NE]
      ,[Index]
      ,max([CellId])
      ,max([AirMacULByte_Kbytes])
      ,max([AirMacULTti_TTI])
      ,max([AirMacULThruAvg_Kbps])
      ,max([AirMacULEfctivThruAvg_Kbps])
      ,max([AirMacDLByte_Kbytes])
      ,max([AirMacDLTti_TTI])
      ,max([AirMacDLThruAvg_Kbps])
      ,max([AirMacDLEfctivThruAvg_Kbps])
      ,max([AirMacULThruMin_Kbps])
      ,max([AirMacULThruMax_Kbps])
      ,max([AirMacDLThruMin_Kbps])
      ,max([AirMacDLThruMax_Kbps])
      ,max([ULIpThruVol_Kbytes])
      ,max([ULIpThruTime_TTI])
      ,max([ULIpThruAvg_Kbps])
      ,max([AirMacDLEfctivThruAvg_kbps]/1000)  as [AirMacDLEfctivThruAvg_Mbps]
      ,max([AirMacULEfctivThruAvg_kbps]/1000) [AirMacULEfctivThruAvg_Mbps]
      ,max([AirMacDLByte_kbytes]/1000000) [AirMacDLByte_Gbytes]
      ,max([created_at])
      ,max([last_updated_at])

 FROM [T5G_DEV].[hist].[USM_KPI_CELL_Traffic_DL_UL]

 where concat( [Date],[Time],[NE],[Index])
           not in ( select  concat( [Date],[Time],[NE],[Index]) from  [T5G_DEV].dwh.[USM_KPI_CELL_Traffic_DL_UL])
group by
 [Date]
,[Time]
,[NE]
,[Index]
 ''')
    conn.commit()


def ca_data(ca_source, cursor, conn):
    usm_ca_data = pd.read_excel(ca_source, r'CA Activation Information Per C')
    usm_ca_data = usm_ca_data.where(pd.notnull(usm_ca_data), '')
    # converting nan to empty string, then our float function will
    # change it to None
    cur_timestamp = datetime.datetime.now()
    for row in usm_ca_data.itertuples():
        cursor.execute('''
        INSERT INTO [hist].[ca_data]
           ([Date]
           ,[Time]
           ,[NE]
           ,[Index]
           ,[CellId]
           ,[SCellActivation_2CC]
           ,[SCellActivation_3CC]
           ,[SCellActivation_4CC]
           ,[SCellActivation_5CC]
           ,[SCellActivatedTime_2CC]
           ,[SCellActivatedTime_3CC]
           ,[SCellActivatedTime_4CC]
           ,[SCellActivatedTime_5CC]
           ,[SCellActivation_6CC]
           ,[SCellActivation_7CC]
           ,[SCellActivatedTime_6CC]
           ,[SCellActivatedTime_7CC]
           ,created_at
           ,last_updated_at
           )
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                          ''',
                       row[1],
                       str(row[2]),
                       str(row[3]),
                       str(row[4]),
                       get_trailing_number(row[4]),
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
                       cur_timestamp,
                       cur_timestamp

                       )

    conn.commit()
    conn.execute('''/****** Script for SelectTopNRows command from SSMS  ******/

insert into [dwh].[ca_data]

SELECT [Date]
      ,[Time]
      ,[NE]
      ,[Index]
      ,max([CellId])
      ,max([SCellActivation_2CC])
      ,max([SCellActivation_3CC])
      ,max([SCellActivation_4CC])
      ,max([SCellActivation_5CC])
      ,max([SCellActivatedTime_2CC])
      ,max([SCellActivatedTime_3CC])
      ,max([SCellActivatedTime_4CC])
      ,max([SCellActivatedTime_5CC])
      ,max([SCellActivation_6CC])
      ,max([SCellActivation_7CC])
      ,max([SCellActivatedTime_6CC])
      ,max([SCellActivatedTime_7CC])
      ,max([created_at])
      ,max([last_updated_at])
  FROM [T5G_DEV].[hist].[ca_data]

 where concat( [Date],[Time],[NE],[Index])
           not in ( select  concat( [Date],[Time],[NE],[Index]) from   [T5G_DEV].[dwh].[ca_data]
	   )
  group by 
	[Date]
   ,[Time]
   ,[NE]
   ,[Index]''')
    conn.commit()


def ca_data(ca_source, cursor, conn):
    usm_ca_data = pd.read_excel(ca_source, r'CA Activation Information Per C')
    usm_ca_data = usm_ca_data.where(pd.notnull(usm_ca_data), '')
    # converting nan to empty string, then our float function will
    # change it to None
    cur_timestamp = datetime.datetime.now()
    for row in usm_ca_data.itertuples():
        cursor.execute('''
        INSERT INTO [hist].[ca_data]
           ([Date]
           ,[Time]
           ,[NE]
           ,[Index]
           ,[CellId]
           ,[SCellActivation_2CC]
           ,[SCellActivation_3CC]
           ,[SCellActivation_4CC]
           ,[SCellActivation_5CC]
           ,[SCellActivatedTime_2CC]
           ,[SCellActivatedTime_3CC]
           ,[SCellActivatedTime_4CC]
           ,[SCellActivatedTime_5CC]
           ,[SCellActivation_6CC]
           ,[SCellActivation_7CC]
           ,[SCellActivatedTime_6CC]
           ,[SCellActivatedTime_7CC]
           ,created_at
           ,last_updated_at
           )
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                          ''',
                       row[1],
                       str(row[2]),
                       str(row[3]),
                       str(row[4]),
                       get_trailing_number(row[4]),
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
                       cur_timestamp,
                       cur_timestamp

                       )

    conn.commit()


def cpe_online(Path, cursor, conn):
    data = pd.read_excel(Path, r'ONLINE RATE')
    data = data.where(pd.notnull(data), '')
    cur_timestamp = datetime.datetime.now()
    for row in data.itertuples():
        cursor.execute('''
    INSERT INTO [hist].[CPE_Online_Rate]
           ([DATE]
           ,[SITE]
           ,[ONLINE]
           ,[OFFLINE]
           ,[TOTAL]
           ,[ONLINE_RATE]
           ,[Authorized]
           ,[Granted]
           ,[Other]
           ,[Total_CBSD]
           ,[created_at]
           ,[last_updated_at])
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                          ''',
                       row[1],
                       str(row[2]),
                       convert_check(row[3]),
                       convert_check(row[4]),
                       convert_check(row[5]),
                       convert_check(row[6]),
                       convert_check(row[7]),
                       convert_check(row[8]),
                       convert_check(row[9]),
                       convert_check(row[10]),
                       cur_timestamp,
                       cur_timestamp

                       )

    conn.commit()



def cpe_online(Path, cursor, conn):
    data = pd.read_excel(Path, r'ONLINE RATE')
    data = data.where(pd.notnull(data), '')
    cur_timestamp = datetime.datetime.now()
    for row in data.itertuples():
        cursor.execute('''
    INSERT INTO [hist].[CPE_Online_Rate]
           ([DATE]
           ,[SITE]
           ,[ONLINE]
           ,[OFFLINE]
           ,[TOTAL]
           ,[ONLINE_RATE]
           ,[Authorized]
           ,[Granted]
           ,[Other]
           ,[Total_CBSD]
           ,[created_at]
           ,[last_updated_at])
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                          ''',
                       str(row[1]),
                       str(row[2]),
                       convert_check(row[3]),
                       convert_check(row[4]),
                       convert_check(row[5]),
                       convert_check(row[6]),
                       convert_check(row[7]),
                       convert_check(row[8]),
                       convert_check(row[9]),
                       convert_check(row[10]),
                       cur_timestamp,
                       cur_timestamp

                       )

    conn.commit()
    conn.execute('''

insert into  [T5G_DEV].[dwh].[CPE_Online_Rate]
SELECT cast([DATE] as date)
      ,[SITE]
      ,max([ONLINE])
      ,max([OFFLINE])
      ,max([TOTAL])
      ,max([ONLINE_RATE])
      ,max([Authorized])
      ,max([Granted])
      ,max([Other])
      ,max([Total_CBSD])
      ,max([created_at])
      ,max([last_updated_at])
  FROM [T5G_DEV].[hist].[CPE_Online_Rate]

where concat(cast([DATE] as date),[site]) not in (

select  concat([date],[site]) from [T5G_DEV].[dwh].[CPE_Online_Rate]
)

and isdate([date])=1

  group by cast([DATE] as date)
         ,[SITE]

''')
    conn.commit()

def IMSI_Data(Path, cursor, conn):
    data = pd.read_excel(Path, r'IMSI_DATA')
    data = data.where(pd.notnull(data), '')
    cur_timestamp = datetime.datetime.now()

    for row in data.itertuples():
        cursor.execute('''
INSERT INTO [T5G_DEV].[hist].[IMSI_Data_usage]
           ([TIME]
           ,[IMSI]
           ,[Data_Usage]    
           ,[created_at])
     VALUES (?,?,?,?)
''',
                       parse(row[1]),
                       str(row[2]),
                       str(row[3]),
                       cur_timestamp

                       )
        conn.commit()
    conn.execute('''insert into [dwh].[IMSI_Data]
  SELECT 

       [TIME], 
	  max(cast([TIME] as date)),
      max(cast([TIME] as time)),

	  [IMSI],
	  max(day(cast([TIME] as date))),
   	  max(year(cast([TIME] as date))),
	  max( month(cast([TIME] as date))),


	  max( datepart(week, cast([TIME] as date))),

      max([Data_Usage]),
	  max(cast([Data_Usage] as float) /1000000000) as [Data_Usage_GB],
	  max(cast([Data_Usage] as float) /1000000000000) as [Data_Usage_TB],
      max(getdate()),
	  max(getdate())

  FROM [T5G_DEV].[hist].[IMSI_Data_usage]

  where concat(time,imsi) not in (
  select  concat(raw_timestamp,imsi) from [dwh].[IMSI_Data]
  )

  group by [TIME],imsi

''')


def Unique_users(Path, cursor, conn):
    data = pd.read_excel(Path, r'Unique Users (Num)')
    data = data.where(pd.notnull(data), '')
    cur_timestamp = datetime.datetime.now()

    for row in data.itertuples():
        cursor.execute('''
INSERT INTO  [hist].[unqiue_users]
           ([Time]
           ,[user_count]
           ,[unique_users]
           ,[created_at]
           ,[last_updated_at])
     VALUES (?,?,?,?,?)
''',
                       parse(row[1]),
                       convert_check(row[2]),
                       convert_check(row[3]),
                       cur_timestamp,
                       cur_timestamp

                       )
        conn.commit()
        conn.execute('''
insert into [T5G_DEV].dwh.[unqiue_users]


SELECT casT([time] as datetime2(7))

      ,max([user_count])
      ,   [unique_users]
      ,max([created_at])
      ,max([last_updated_at])

  FROM [T5G_DEV].[hist].[unqiue_users]
  where concat(casT([time] as datetime2(7)),[unique_users]) not in (

      select concat(casT([time] as datetime2(7)),[unique_users]) from  [T5G_DEV].dwh.[unqiue_users]
     )

  group by [Time],[unique_users]
''')
    conn.commit()

def Truncate_All_Table( conn):
    conn.execute(''' 
truncate table [hist].[ca_data]
truncate table [hist].[CPE_Online_Rate]
truncate table [hist].[IMSI_Data_usage]
truncate table [hist].[unqiue_users]
truncate table [hist].[USM_KPI_CELL_LEVEL_Availability]
truncate table [hist].[USM_KPI_CELL_LEVEL_DL_RI]
truncate table [hist].[USM_KPI_CELL_LEVEL_ERAB_Accessibility]
truncate table [hist].[USM_KPI_CELL_LEVEL_S1ConnEstab]
truncate table [hist].[USM_KPI_CELL_RRC_Conn_Est]
truncate table [hist].[USM_KPI_CELL_Traffic_DL_UL]
''')
    conn.commit()
def convert_check(val):
    try:
        float(val)
    except ValueError as ex:
        val = None

    return val


def Historical_Data_load_method(cursor, conn):
    # Import Excel
    #USM_file = pd.ExcelFile(r"C:\SQLPub\historical\USM_KPI_CELL_LEVEL.xlsx")

   # ca_source = pd.ExcelFile(r"C:\SQLPub\historical\CA DATA.xlsx")
    #cpe_source = pd.ExcelFile(r"C:\SQLPub\historical\CPE_ONLINE_RATE.xlsx")
    IMSI_Data_source = pd.ExcelFile(r"C:\SQLPub\historical\DISD_IMSI_DATA.xlsx")
    UniqueUsers = pd.ExcelFile(r"C:\SQLPub\historical\Unique_Users.xlsx")

    # Connect to SQL Server
    print('Truncate all tables')
    Truncate_All_Table(conn)
    # print('usm_kpi_cell_level_s1connestab')
    # usm_kpi_cell_level_s1connestab(USM_file, cursor, conn)
    # print('usm_kpi_cell_level_dl_ri')
    # usm_kpi_cell_level_dl_ri(USM_file, cursor, conn)
    # print('usm_kpi_cell_level_erab_accessibility')
    # usm_kpi_cell_level_erab_accessibility(USM_file, cursor, conn)
    # print('usm_kpi_cell_level_availability')
    # usm_kpi_cell_level_availability(USM_file, cursor, conn)
    # print('usm_kpi_cell_rrc_conn_est')
    #usm_kpi_cell_rrc_conn_est(USM_file, cursor, conn)
    #print('usm_kpi_cell_traffic_dl_ul')
    #usm_kpi_cell_traffic_dl_ul(USM_file, cursor, conn)
    # print('ca_data')
    # ca_data(ca_source, cursor, conn)
    # print('cpe_online')
    # cpe_online(cpe_source,cursor,conn)
    print('IMSI_Data')
    IMSI_Data(IMSI_Data_source,cursor,conn)
    print('Unique Users')
    Unique_users(UniqueUsers, cursor, conn)
    # print('Truncate all tables')
    #Truncate_All_Table(conn)