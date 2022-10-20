
truncate_usm_dl_ri = "truncate table [staging].[USM_DL_RI]"
truncate_usm_erab_accessibility = "truncate table [staging].[USM_ERAB_Accessibility]"
truncate_air_mac_packet = "truncate table [staging].[Air_Mac_Packet]"
truncate_rrc_connection_establishment = "truncate table  [staging].[RRC_Connection_establishment]"
truncate_availability = "truncate table  [staging].[Availability]"
truncate_s1_connection_establishment = "truncate table [staging].[S1_Connection_Establishment]"
truncate_scell_added_information = "truncate table [staging].[SCell_Added_Information]"
truncate_imsi_data = " truncate table staging.IMSI_Data_usage"
truncate_table_unique_users = "truncate table  [staging].[unique_users]"
truncate_table_BlinQ = "truncate table  [staging].[BlinQ]"
truncate_table_Bec_Export = "truncate table  [staging].[Bec_Export]"

insert_usm_erab_accessibility = '''
            INSERT INTO [T5G_DEV].[staging].[USM_ERAB_Accessibility]
           ([Time]
           ,[NE]
           ,[Index]
           ,[CellId]
           ,[ErabAccessibilityInit]
           ,[ErabAccessibilityAdd]
           ,[ErabConnectionFailureRate]
           ,created_at)
     VALUES (?,?,?,?,?,?,?,?)
                          '''

insert_USM_DL_RI = '''INSERT INTO  [staging].[USM_DL_RI]
           (
           [Time]
           ,[NE]
           ,[Index]
           ,[CellId]
           ,[DLReceivedRI0]
           ,[DLReceivedRI1]
           ,[DLReceivedRI2]
           ,[DLReceivedRI3]
           ,[DLReceivedRI4]
           ,[DLReceivedRIAvg]
           ,created_at
           )
     VALUES (?,?,?,?,?,?,?,?,?,?,?)
'''

insert_air_mac_packet = '''
INSERT INTO [staging].[Air_Mac_Packet]
           ([Time]
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
           ,created_at )
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''

insert_rrc_connection_establishment = '''
INSERT INTO [staging].[RRC_Connection_establishment]
           ([Time]
           ,[NE]
           ,[Index]
           ,[CellId]
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
           ,[created_date])
              VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
     '''

insert_availability = '''
INSERT INTO [staging].[Availability]
           ([Time]
           ,[NE]
           ,[Index]
           ,[cellID]
           ,[EutranCellAvailability]
           ,[ReadCellUnavailableTime]
           ,[CellAvailPmPeriodTime]
           ,[EutranCellAvailabilityEx]
           ,[ReadCellUnavailableTimeLock]
           ,[ReadCellUnavailableTimeES]
           ,[created_at])

          VALUES (?,?,?,?,?,?,?,?,?,?,?)
     '''
insert_s1_connection_establishment = '''
INSERT INTO [staging].[S1_Connection_Establishment]
           ([Time]
           ,[NE]
           ,[Index]
           ,[cellid]
           ,[S1ConnEstabAtt]
           ,[S1ConnEstabSucc]
           ,[S1ConnEstabFail_CpCcFail]
           ,[S1ConnEstabFail_S1apCuFail]
           ,[S1ConnEstabFail_S1apLinkFail]
           ,[S1ConnEstabFail_S1apSigFail]
           ,[S1ConnEstabFail_S1apSigTo]
           ,[created_at])
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
'''

insert_Scell_added_information = '''
INSERT INTO [staging].[SCell_Added_Information]
           ([Time]
           ,[NE]
           ,[Index]
           ,[CellId]
           ,[No_DLCaCapabilityUE]
           ,[DLCaCapabilityUE_2CC]
           ,[DLCaCapabilityUE_3CC]
           ,[SCellAdditionTime_2CC]
           ,[SCellAdditionTime_3CC]
           ,[DLCaCapabilityUE_4CC]
           ,[SCellAdditionTime_4CC]
           ,[DLCaCapabilityUE_5CC]
           ,[SCellAdditionTime_5CC]
           ,[DLCaCapabilityUE_6CC]
           ,[SCellAdditionTime_6CC]
           ,[DLCaCapabilityUE_7CC]
           ,[SCellAdditionTime_7CC]
           ,[created_at])
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)  
     '''


insert_imsi_data = '''
INSERT INTO [staging].[IMSI_Data_usage]
           ([TIME]
           ,[IMSI]
           ,[Data_Usage]    
           ,[created_at])
     VALUES (?,?,?,?)
'''



insert_unqiue_users_data = '''
INSERT INTO [staging].[unique_users]
           ([TIME]
           ,[user_count]
           ,[unique_users]
           ,[created_at])
     VALUES (?,?,?,?)
'''

insert_BlinQ = '''insert into [staging].[BlinQ]  values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
?,?,?,?,?,?,?,?,?,?,?) '''

insert_Bec_Export = '''insert into  [staging].[Bec_Export]  values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''

insert_blinq_min = '''
INSERT INTO [staging].[BlinQ_Min]
           ([deviceBaseModel]
           ,[imsi]
           ,[imei]
           ,[status]
           ,[serialId]
           ,[band]
           ,[lteNetworkOperator]
           ,[enbid]
           ,[enbidhex]
           ,[created_at])
     VALUES (?,?,?,?,?,?,?,?,?,?)

     '''
truncate_table_queries = [truncate_usm_dl_ri, truncate_usm_erab_accessibility,
                          truncate_air_mac_packet, truncate_rrc_connection_establishment,
                          truncate_availability, truncate_s1_connection_establishment,
                          truncate_scell_added_information, truncate_imsi_data,
                          truncate_table_unique_users, truncate_table_BlinQ,
                          truncate_table_Bec_Export]
