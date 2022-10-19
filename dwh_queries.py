merge_dwh_usm_kpi_cell_level_dl_ri = ''' 
MERGE [dwh].[USM_KPI_CELL_LEVEL_DL_RI] AS T
USING [staging].[USM_DL_RI]	AS S
ON left(S.[time],11) = T.[date]
and cast(concat(right(S.[time],5),':00.0000000') as time) =T.[Time]
and S.[NE]=T.[NE]
and S.[Index]=T.[Index]

WHEN NOT MATCHED BY Target THEN

insert ( [Date]
           ,[Time]
           ,[NE]
           ,[Index]
           ,Cellid
           ,[DLReceivedRI0]
           ,[DLReceivedRI1]
           ,[DLReceivedRI2]
           ,[DLReceivedRI3]
           ,[DLReceivedRI4]
           ,[DLReceivedRIAvg]
           ,created_at
           ,last_updated_at
           )
values (
			left([time],11) ,
			cast(concat(right(S.[time],5),':00.0000000') as time),
			[NE] ,
			[Index],
			cellid,
			[DLReceivedRI0],
			[DLReceivedRI1],
			[DLReceivedRI2],
			[DLReceivedRI3],
			[DLReceivedRI4],
			[DLReceivedRIAvg],
			getdate(),
			getdate()
)


WHEN MATCHED THEN UPDATE SET
	   T.[Date]=left(S.[time],11)
      ,T.[Time]= cast(concat(right(S.[time],5),':00.0000000') as time)
      ,T.[NE]=S.[NE]
      ,T.[Index]=S.[Index]
      ,T.[cellid]=S.[cellid]     
      ,T.[DLReceivedRI0]=S.[DLReceivedRI0]
      ,T.[DLReceivedRI1]=S.[DLReceivedRI1]
      ,T.[DLReceivedRI2]=S.[DLReceivedRI2]
      ,T.[DLReceivedRI3]=S.[DLReceivedRI3]
      ,T.[DLReceivedRI4]=S.[DLReceivedRI4]
      ,T.[DLReceivedRIAvg]=S.[DLReceivedRIAvg]
      ,T.last_updated_at=getdate();

'''

merge_dwh_usm_kpi_cell_level_erab_accessibility = '''

MERGE [dwh].[USM_KPI_CELL_LEVEL_ERAB_Accessibility] AS T
USING [T5G_DEV].[staging].[USM_ERAB_Accessibility]	AS S
ON left(S.[time],11) = T.[date]
and cast(concat(right(S.[time],5),':00.0000000') as time) =T.[Time]
and S.[NE]=T.[NE]
and S.[Index]=T.[Index]

WHEN NOT MATCHED BY Target THEN

insert ( 
       [Date]
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

values (
		left([time],11) 
		,cast(concat(right(S.[time],5),':00.0000000') as time)
		,[NE] 
		,[Index]
		,[CellId]
		,[ErabAccessibilityInit]
		,[ErabAccessibilityAdd]
		,[ErabConnectionFailureRate]
		,getdate()
		,getdate()
)


WHEN MATCHED THEN UPDATE SET
	   T.[Date]=left(S.[time],11)
      ,T.[Time]= cast(concat(right(S.[time],5),':00.0000000') as time)
      ,T.[NE]=S.[NE]
      ,T.[Index]=S.[Index]
	  ,T.[CellId]=S.[CellId]
      ,T.[ErabAccessibilityInit]=S.[ErabAccessibilityInit]
	  ,T.[ErabAccessibilityAdd]=S.[ErabAccessibilityAdd]
	  ,T.[ErabConnectionFailureRate]=S.[ErabConnectionFailureRate]
      ,T.last_updated_at=getdate();
'''
merge_dwh_usm_kpi_cell_traffic_dl_ul = '''

MERGE [T5G_DEV].[dwh].[USM_KPI_CELL_Traffic_DL_UL] AS T
USING [T5G_DEV].[staging].[Air_Mac_Packet]	AS S
ON left(S.[time],11) = T.[date]
and cast(concat(right(S.[time],5),':00.0000000') as time) =T.[Time]
and S.[NE]=T.[NE]
and S.[Index]=T.[Index]

WHEN NOT MATCHED BY Target THEN

insert ( 
       [Date]
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
      ,[AirMacDLEfctivThruAvg_Mbps]
      ,[AirMacULEfctivThruAvg_Mbps]
      ,[AirMacDLByte_Gbytes]
      ,[created_at]
      ,[last_updated_at]
       )

values (
		left([time],11) 
		,cast(concat(right(S.[time],5),':00.0000000') as time)
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
	    ,[AirMacDLEfctivThruAvg_Kbps]/1000
        ,[AirMacULEfctivThruAvg_Kbps]/1000
        ,[AirMacDLByte_Kbytes]/1000000
		,getdate()
		,getdate()
)


WHEN MATCHED THEN UPDATE SET
	   T.[Date]=left(S.[time],11)
      ,T.[Time]= cast(concat(right(S.[time],5),':00.0000000') as time)
      ,T.[NE]=S.[NE]
      ,T.[Index]=S.[Index]
	  ,T.[CellId]=S.[CellId]
      ,T.[AirMacULByte_Kbytes]=S.[AirMacULByte_Kbytes]
      ,T.[AirMacULTti_TTI]=S.[AirMacULTti_TTI]
      ,T.[AirMacULThruAvg_Kbps]=S.[AirMacULThruAvg_Kbps]
      ,T.[AirMacULEfctivThruAvg_Kbps]=S.[AirMacULEfctivThruAvg_Kbps]
      ,T.[AirMacDLByte_Kbytes]=S.[AirMacDLByte_Kbytes]
      ,T.[AirMacDLTti_TTI]=S.[AirMacDLTti_TTI]
      ,T.[AirMacDLThruAvg_Kbps]=S.[AirMacDLThruAvg_Kbps]
      ,T.[AirMacDLEfctivThruAvg_Kbps]=S.[AirMacDLEfctivThruAvg_Kbps]
      ,T.[AirMacULThruMin_Kbps]=S.[AirMacULThruMin_Kbps]
      ,T.[AirMacULThruMax_Kbps]=S.[AirMacULThruMax_Kbps]
      ,T.[AirMacDLThruMin_Kbps]=S.[AirMacDLThruMin_Kbps]
      ,T.[AirMacDLThruMax_Kbps]=S.[AirMacDLThruMax_Kbps]
      ,T.[ULIpThruVol_Kbytes]=S.[ULIpThruVol_Kbytes]
      ,T.[ULIpThruTime_TTI]=S.[ULIpThruTime_TTI]
      ,T.[ULIpThruAvg_Kbps]=S.[ULIpThruAvg_Kbps]
      ,T.[AirMacDLEfctivThruAvg_Mbps]=S.[AirMacDLEfctivThruAvg_Kbps]/1000
      ,T.[AirMacULEfctivThruAvg_Mbps]=S.[AirMacULEfctivThruAvg_Kbps]/1000
      ,T.[AirMacDLByte_Gbytes]=S.[AirMacDLByte_Kbytes]/1000000
      ,T.last_updated_at=getdate();


'''

merge_dwh_usm_kpi_cell_rrc_conn_est = '''

MERGE [T5G_DEV].[dwh].[USM_KPI_CELL_RRC_Conn_Est] AS T
USING [T5G_DEV].[staging].[RRC_Connection_establishment]	AS S
ON left(S.[time],11) = T.[date]
and cast(concat(right(S.[time],5),':00.0000000') as time) =T.[Time]
and S.[NE]=T.[NE]
and S.[Index]=T.[Index]

WHEN NOT MATCHED BY Target THEN

insert ( 
       [Date]
      ,[Time]
      ,[NE]
      ,[Index]
      ,[CellID]
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
      ,[Created_date]
      ,[last_updated_at]
       )

values (
		left([time],11) 
		,cast(concat(right(S.[time],5),':00.0000000') as time)
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
		,getdate()
		,getdate()
)


WHEN MATCHED THEN UPDATE SET
	   T.[Date]=left(S.[time],11)
      ,T.[Time]= cast(concat(right(S.[time],5),':00.0000000') as time)
      ,T.[NE]=S.[NE]
      ,T.[Index]=S.[Index]
	  ,T.[CellId]=S.[CellId]
      ,T.[ConnEstabAtt]=S.[ConnEstabAtt]
      ,T.[ConnEstabSucc]=S.[ConnEstabSucc]
      ,T.[ConnEstabFail_CpCcTo]=S.[ConnEstabFail_CpCcTo]
      ,T.[ConnEstabFail_CpCcFail]=S.[ConnEstabFail_CpCcFail]
      ,T.[ConnEstabFail_UpMacFail]=S.[ConnEstabFail_UpMacFail]
      ,T.[ConnEstabFail_UpPdcpFail]=S.[ConnEstabFail_UpPdcpFail]
      ,T.[ConnEstabFail_UpRlcFail]=S.[ConnEstabFail_UpRlcFail]
      ,T.[ConnEstabFail_RrcSigTo]=S.[ConnEstabFail_RrcSigTo]
      ,T.[ConnEstabFail_S1apLinkFail]=S.[ConnEstabFail_S1apLinkFail]
      ,T.[ConnEstabFail_S1apSigFail]=S.[ConnEstabFail_S1apSigFail]
      ,T.[ConnEstabReject_CpCcFail]=S.[ConnEstabReject_CpCcFail]
      ,T.[ConnEstabReject_CpCapaCacFail]=S.[ConnEstabReject_CpCapaCacFail]
      ,T.[ConnEstabReject_S1apMmeOvld]=S.[ConnEstabReject_S1apMmeOvld]
      ,T.last_updated_at=getdate();
'''

merge_dwh_usm_availability = '''

MERGE [T5G_DEV].[dwh].[USM_KPI_CELL_LEVEL_Availability] AS T
USING [T5G_DEV].[staging].[Availability]	AS S
ON left(S.[time],11) = T.[date]
and cast(concat(right(S.[time],5),':00.0000000') as time) =T.[Time]
and S.[NE]=T.[NE]
and S.[Index]=T.[Index]

WHEN NOT MATCHED BY Target THEN

insert ( 
       [Date]
      ,[Time]
      ,[NE]
      ,[Index]
	  ,[cellID]
      ,[EutranCellAvailability]
      ,[ReadCellUnavailableTime]
      ,[CellAvailPmPeriodTime]
      ,[EutranCellAvailabilityEx]
      ,[ReadCellUnavailableTimeLock]
      ,[ReadCellUnavailableTimeES]
      ,[created_at]
      ,[last_updated_at]
       )

values (
		left([time],11) 
		,cast(concat(right(S.[time],5),':00.0000000') as time)
		,[NE] 
		,[Index]
		,[CellId]
		,[EutranCellAvailability]
		,[ReadCellUnavailableTime]
		,[CellAvailPmPeriodTime]
		,[EutranCellAvailabilityEx]
	    ,[ReadCellUnavailableTimeLock]
		,[ReadCellUnavailableTimeES]
		,getdate()
		,getdate()
)


WHEN MATCHED THEN UPDATE SET
	   T.[Date]=left(S.[time],11)
      ,T.[Time]= cast(concat(right(S.[time],5),':00.0000000') as time)
      ,T.[NE]=S.[NE]
      ,T.[Index]=S.[Index]
	  ,T.[CellId]=S.[CellId]
      ,T.[EutranCellAvailability]=S.[EutranCellAvailability]
      ,T.[ReadCellUnavailableTime]=S.[ReadCellUnavailableTime]
      ,T.[CellAvailPmPeriodTime]=S.[CellAvailPmPeriodTime]
      ,T.[EutranCellAvailabilityEx]=S.[EutranCellAvailabilityEx]
      ,T.[ReadCellUnavailableTimeLock]=S.[ReadCellUnavailableTimeLock]
      ,T.[ReadCellUnavailableTimeES]=S.[ReadCellUnavailableTimeES]
      ,T.last_updated_at=getdate();

'''

merge_dwh_usm_s1_Connection_Establishment = '''

MERGE [T5G_DEV].[dwh].[USM_KPI_CELL_LEVEL_S1ConnEstab] AS T
USING [T5G_DEV].[staging].[S1_Connection_Establishment]	AS S
ON left(S.[time],11) = T.[date]
and cast(concat(right(S.[time],5),':00.0000000') as time) =T.[Time]
and S.[NE]=T.[NE]
and S.[Index]=T.[Index]

WHEN NOT MATCHED BY Target THEN

insert ( 
	   [Date]
      ,[Time]
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
      ,[created_at]
      ,[last_updated_at]
       )

values (
		left([time],11) 
		,cast(concat(right(S.[time],5),':00.0000000') as time)
		,[NE] 
		,[Index]
		,[CellId]
	  ,[S1ConnEstabAtt]
      ,[S1ConnEstabSucc]
      ,[S1ConnEstabFail_CpCcFail]
      ,[S1ConnEstabFail_S1apCuFail]
      ,[S1ConnEstabFail_S1apLinkFail]
      ,[S1ConnEstabFail_S1apSigFail]
      ,[S1ConnEstabFail_S1apSigTo]
		,getdate()
		,getdate()
)


WHEN MATCHED THEN UPDATE SET
	   T.[Date]=left(S.[time],11)
      ,T.[Time]= cast(concat(right(S.[time],5),':00.0000000') as time)
      ,T.[NE]=S.[NE]
      ,T.[Index]=S.[Index]
	  ,T.[CellId]=S.[CellId]
      ,T.[S1ConnEstabAtt]=S.[S1ConnEstabAtt]
      ,T.[S1ConnEstabSucc]=S.[S1ConnEstabSucc]
      ,T.[S1ConnEstabFail_S1apCuFail]=S.[S1ConnEstabFail_S1apCuFail]
      ,T.[S1ConnEstabFail_S1apLinkFail]=S.[S1ConnEstabFail_S1apLinkFail]
      ,T.[S1ConnEstabFail_S1apSigFail]=S.[S1ConnEstabFail_S1apSigFail]
      ,T.[S1ConnEstabFail_S1apSigTo]=S.[S1ConnEstabFail_S1apSigTo]
      ,T.last_updated_at=getdate();

'''




merge_dwh_usm_scell_added_information = '''

MERGE [dwh].[ca_data] AS T
USING [staging].[SCell_Added_Information]	AS S
ON left(S.[time],11) = T.[date]
and cast(concat(right(S.[time],5),':00.0000000') as time) =T.[Time]
and S.[NE]=T.[NE]
and S.[Index]=T.[Index]

WHEN NOT MATCHED BY Target THEN

insert ( 
	   [Date]
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
      ,[created_at]
      ,[last_updated_at]
       )

values (
		left([time],11) 
		,cast(concat(right(S.[time],5),':00.0000000') as time)
		,[NE] 
		,[Index]
		,[CellId]
      ,[DLCaCapabilityUE_2CC]
      ,[DLCaCapabilityUE_3CC]
      ,[DLCaCapabilityUE_4CC]
      ,[DLCaCapabilityUE_5CC]
      ,[SCellAdditionTime_2CC]
      ,[SCellAdditionTime_3CC]
      ,[SCellAdditionTime_6CC]
      ,[SCellAdditionTime_5CC]
      ,[DLCaCapabilityUE_6CC]
      ,[DLCaCapabilityUE_7CC]
      ,[SCellAdditionTime_6CC]
      ,[SCellAdditionTime_7CC]
		,getdate()
		,getdate()
)


WHEN MATCHED THEN UPDATE SET
	   T.[Date]=left(S.[time],11)
      ,T.[Time]= cast(concat(right(S.[time],5),':00.0000000') as time)
      ,T.[NE]=S.[NE]
      ,T.[Index]=S.[Index]
	  ,T.[CellId]=S.[CellId]
      ,T.[SCellActivation_2CC]=S.[DLCaCapabilityUE_2CC]
      ,T.[SCellActivation_3CC]=S.[DLCaCapabilityUE_3CC]
      ,T.[SCellActivation_4CC]=S.[DLCaCapabilityUE_4CC]
      ,T.[SCellActivation_5CC]=S.[DLCaCapabilityUE_5CC]
      ,T.[SCellActivatedTime_2CC]=S.[SCellAdditionTime_2CC]
      ,T.[SCellActivatedTime_3CC]=S.[SCellAdditionTime_3CC]
	  ,T.[SCellActivatedTime_4CC] = S.[SCellAdditionTime_4CC]
	  ,T.[SCellActivatedTime_5CC] = S.[SCellAdditionTime_5CC]
   	  ,T.[SCellActivation_6CC] = S.[DLCaCapabilityUE_6CC]
	  ,T.[SCellActivation_7CC] = S.[DLCaCapabilityUE_7CC]
	  ,T.[SCellActivatedTime_6CC] = S.[SCellAdditionTime_6CC]
	  ,T.[SCellActivatedTime_7CC] =S.[SCellAdditionTime_7CC]
      ,T.last_updated_at=getdate();
'''


merge_dwh_unique_users= '''

MERGE [T5G_DEV].[dwh].[unqiue_users] AS T
USING [T5G_DEV].[staging].[unique_users]	AS S
ON  S.[time] = T.[TIME]

WHEN NOT MATCHED BY Target THEN

insert ( 
	  [Time]
      ,[user_count]
      ,[unique_users]
      ,[created_at]
      ,[last_updated_at]
       )

values (
		[Time]
        ,[user_count]
        ,[unique_users]
		,getdate()
		,getdate()
)


WHEN MATCHED THEN UPDATE SET
	   T.[Time]=S.time
      ,T.[user_count]= S.[user_count]
      ,T.[unique_users]=S.[unique_users]
      ,T.last_updated_at=getdate();

'''

merge_dwh_DISD_BEC = '''
MERGE [T5G_DEV].[dwh].[DISD_BEC] AS T
USING [T5G_DEV].[staging].[Bec_Export]	AS S
ON  S.IMSI = T.IMSI
and S.[Date] =T.[Date]
and 'BEC'=T.[source_file]

WHEN NOT MATCHED BY Target THEN

insert ( 
	  [date]
      ,[RI]
      ,[DL_MCS]
      ,[CQI]
      ,[CustID]
      ,[Address]
      ,[MODEL]
      ,[customField]
      ,[ICCID]
      ,[IMSI]
      ,[IMEI]
      ,[name]
      ,[Status]
      ,[owner]
      ,[Longitude]
      ,[Latitude]
      ,[CBSD_STATUS]
      ,[CBSD_ID]
      ,[CBSD_GRANTID]
      ,[MAC]
      ,[FW]
      ,[CardFw]
      ,[BAND]
      ,[VOLT]
      ,[CURRENT]
      ,[TEMP]
      ,[System_Uptime]
      ,[IP]
      ,[RSSI]
      ,[RSSI_DIV1]
      ,[RSSI_DIV2]
      ,[RSSI_DIV3]
      ,[RSRP]
      ,[RSRP_DIV1]
      ,[RSRP_DIV2]
      ,[RSRP_DIV3]
      ,[RSRQ]
      ,[RSRQ_DIV1]
      ,[RSRQ_DIV2]
      ,[RSRQ_DIV3]
      ,[SINR]
      ,[SINR_DIV1]
      ,[SINR_DIV2]
      ,[SINR_DIV3]
      ,[NETWORK]
      ,[CELLID]
      ,[ECI]
      ,[EnodebID]
      ,[NETMODE]
      ,[PCI]
      ,[3G_4G_TX]
      ,[3G_4G_RX]
      ,[3G_4G_Total_BW]
      ,[EWAN_TX]
      ,[EWAN_RX]
      ,[EWAN_Total_BW]
	  ,created_At
	  ,last_updated_at
	  , [source_file]
       )

values (
 [date]
      ,[RI]
      ,[DL_MCS]
      ,[CQI]
      ,[CustID]
      ,[Address]
      ,[MODEL]
      ,[customField]
      ,[ICCID]
      ,[IMSI]
      ,[IMEI]
      ,[name]
      ,[Status]
      ,[owner]
      ,[Longitude]
      ,[Latitude]
      ,[CBSD_STATUS]
      ,[CBSD_ID]
      ,[CBSD_GRANTID]
      ,[MAC]
      ,[FW]
      ,[CardFw]
      ,[BAND]
      ,[VOLT]
      ,[CURRENT]
      ,[TEMP]
      ,[System_Uptime]
      ,[IP]
      ,[RSSI]
      ,[RSSI_DIV1]
      ,[RSSI_DIV2]
      ,[RSSI_DIV3]
      ,[RSRP]
      ,[RSRP_DIV1]
      ,[RSRP_DIV2]
      ,[RSRP_DIV3]
      ,[RSRQ]
      ,[RSRQ_DIV1]
      ,[RSRQ_DIV2]
      ,[RSRQ_DIV3]
      ,[SINR]
      ,[SINR_DIV1]
      ,[SINR_DIV2]
      ,[SINR_DIV3]
      ,[NETWORK]
      ,[CELLID]
      ,[ECI]
      ,[EnodebID]
      ,[NETMODE]
      ,[PCI]
      ,[3G_4G_TX]
      ,[3G_4G_RX]
      ,[3G_4G_Total_BW]
      ,[EWAN_TX]
      ,[EWAN_RX]
      ,[EWAN_Total_BW]
	  ,getdate()
	  ,getdate()
	  ,'BEC'
)


WHEN MATCHED THEN UPDATE SET
     T.[RI]=  S.[RI] 
,T.[DL_MCS]= S.[DL_MCS]
,T.[CQI]= S.[CQI]
,T.[CustID]= S.[CustID]
,T.[Address]= S.[Address]
,T.[MODEL]= S.[MODEL]
,T.[customField]= S.[customField]
,T.[ICCID]= S.[ICCID]
,T.[IMSI]= S.[IMSI]
,T.[IMEI]= S.[IMEI]
,T.[name]= S.[name]
,T.[Status]= S.[Status]
,T.[owner]= S.[owner]
,T.[Longitude]= S.[Longitude]
,T.[Latitude]= S.[Latitude]
,T.[CBSD_STATUS]= S.[CBSD_STATUS]
,T.[CBSD_ID]= S.[CBSD_ID]
,T.[CBSD_GRANTID]= S.[CBSD_GRANTID]
,T.[MAC]= S.[MAC]
,T.[FW]= S.[FW]
,T.[CardFw]= S.[CardFw]
,T.[BAND]= S.[BAND]
,T.[VOLT]= S.[VOLT]
,T.[CURRENT]= S.[CURRENT]
,T.[TEMP]= S.[TEMP]
,T.[System_Uptime]= S.[System_Uptime]
,T.[IP]= S.[IP]
,T.[RSSI]= S.[RSSI]
,T.[RSSI_DIV1]= S.[RSSI_DIV1]
,T.[RSSI_DIV2]= S.[RSSI_DIV2]
,T.[RSSI_DIV3]= S.[RSSI_DIV3]
,T.[RSRP]= S.[RSRP]
,T.[RSRP_DIV1]= S.[RSRP_DIV1]
,T.[RSRP_DIV2]= S.[RSRP_DIV2]
,T.[RSRP_DIV3]= S.[RSRP_DIV3]
,T.[RSRQ]= S.[RSRQ]
,T.[RSRQ_DIV1]= S.[RSRQ_DIV1]
,T.[RSRQ_DIV2]= S.[RSRQ_DIV2]
,T.[RSRQ_DIV3]= S.[RSRQ_DIV3]
,T.[SINR]= S.[SINR]
,T.[SINR_DIV1]= S.[SINR_DIV1]
,T.[SINR_DIV2]= S.[SINR_DIV2]
,T.[SINR_DIV3]= S.[SINR_DIV3]
,T.[NETWORK]= S.[NETWORK]
,T.[CELLID]= S.[CELLID]
,T.[ECI]= S.[ECI]
,T.[EnodebID]= S.[EnodebID]
,T.[NETMODE]= S.[NETMODE]
,T.[PCI]= S.[PCI]
,T.[3G_4G_TX]= S.[3G_4G_TX]
,T.[3G_4G_RX]= S.[3G_4G_RX]
,T.[3G_4G_Total_BW]= S.[3G_4G_Total_BW]
,T.[EWAN_TX]= S.[EWAN_TX]
,T.[EWAN_RX]= S.[EWAN_RX]
,T.[EWAN_Total_BW]= S.[EWAN_Total_BW]
,T.last_updated_at=getdate()
,T.[source_file]='BEC';
'''

merge_dwh_BlinQ ='''with blinq as(
select

       a.[Date],
	  'Trilogy5G' as CustID,
		deviceBaseModel as MODEL,
		b.imsi,
		a.imei,
		case when wlanConfig='operational' then 'Online'
			 when wlanConfig='missing' then 'Offline' end as [Status],
		'BLINQ/Trilogy5G/DISD/' as [owner],
		serialId as FW, 
		band,
		lteNetworkOperator + 'T5g' as NETWORK,
		a.wanLeaseExpires as  EnodebID,
		enbidhex as NETMODE
		,'BLINQ' as [source_file]
	  ,getdate() as created_At
	  ,getdate() as last_updated_at


from [staging].[BlinQ] a
inner join [dwh].[DISD_Ref_File] b
on a.imei=b.imei

)

MERGE [T5G_DEV].[dwh].[DISD_BEC] AS T
USING blinq	AS S
ON  S.IMSI = T.IMSI
and S.[Date] =T.[Date]
and s.[source_file]=T.[source_file]

WHEN NOT MATCHED BY Target THEN

insert ( 

	[date]
	,CustID
	,MODEL
	,IMSI
	,IMEI
	,[Status]
	,[owner]
	,FW
	,BAND
	,NETWORK
	,EnodebID
	,NETMODE
    ,created_At
    ,last_updated_at
	,[source_file]

       )

values (
		[date]
	,CustID
	,MODEL
	,IMSI
	,IMEI
	,[Status]
	,[owner]
	,FW
	,BAND
	,NETWORK
	,EnodebID
	,NETMODE
    ,created_At
    ,last_updated_at
	,[source_file]


)


WHEN MATCHED THEN UPDATE SET
T.CustID= S.CustID
,T.MODEL= S.MODEL
,T.IMEI= S.IMEI
,T.[Status]= S.[Status]
,T.[owner]= S.[owner]
,T.FW= S.FW
,T.BAND= S.BAND
,T.NETWORK= S.NETWORK
,T.EnodebID= S.EnodebID
,T.NETMODE= S.NETMODE
,T.created_At= S.created_At
,T.last_updated_at=getdate();'''



merge_dwh_cpe_online = '''

with cte as
(
SELECT 
 [date],
case when [EnodebID] =753 then 'Adamson'
	 when [EnodebID] =754 then 'Samuel'
	 when [EnodebID] =755 then 'Kimball'
     end as [site],   
case when status='Offline' then count(status)
     else 0
     end as [Offline],
case when status='Online' then count(status)
     else 0
     end as [Online]

FROM [T5G_DEV].[dwh].[DISD_BEC]
where [EnodebID] is not null
group by 
date, 
[EnodebID],
status
)

, grant_authorize_base 

as (

select 
[date],
custid,
case when CBSD_STATUS='Authorized' then count(1) end as Authorized ,
case when CBSD_STATUS='Granted' then count(1) end as Granted ,
case when CBSD_STATUS not in ('Granted','Authorized' ) then count(1) end as Other
FROM [T5G_DEV].[dwh].[DISD_BEC]
group by  [date],custid,CBSD_STATUS
) 
,grant_authorize as(
select 
[date],
custid,
sum(Authorized) Authorized,
sum(Granted) Granted,
sum(Other) Other
from grant_authorize_base
group by 
[date],custid

)
,
network as (

select 
a.[DATE],
'Network' as [SITE], 
sum(Online) as [ONLINE],
sum(offline) as [OFFLINE], 
sum(Online)+sum(offline) as [TOTAL],
cast(sum(Online) as float)/(sum(offline)+sum(Online) ) as [ONLINE_RATE],
b.[Authorized],
b.[Granted],
b.[Other],
(b.Authorized+b.Granted+b.Other) as [Total_CBSD]

from cte a

inner join grant_authorize b
on a.[date]=b.[date]

where site is not null

group by
a.[date],
b.Authorized,b.Granted,b.Other
)
,cpe_online as (

select 
[date],	
[site],	    
[Online],	
[offline],	
[Total],             	
[Online_Rate],	
[Authorized],	
[Granted],	
[Other],	
[Total_CBSD]
from network


union all


select 
[date],
site,
sum(Online) as [Online],
sum(offline) as [Offline],
sum(offline)+sum(Online) AS Total,
cast(sum(Online) as float)/(sum(offline)+sum(Online) ) as Online_Rate,
null,
null,
null,
null

from cte

group by 
[date],
site
)





MERGE [T5G_DEV].[dwh].[CPE_Online_Rate] AS T
USING cpe_online	AS S
ON  S.[date] = T.[date]
and S.[Site] =T.[Site]


WHEN NOT MATCHED BY Target THEN

insert ( 
	    [DATE]
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
      ,[last_updated_at]

       )

values (
	    [DATE]
      ,[SITE]
      ,[ONLINE]
      ,[OFFLINE]
      ,[TOTAL]
      ,[ONLINE_RATE]
      ,[Authorized]
      ,[Granted]
      ,[Other]
      ,[Total_CBSD]
      ,getdate()
      ,getdate()



)


WHEN MATCHED THEN UPDATE SET
T.[DATE]= S.[DATE]
,T.[SITE]= S.[SITE]
,T.[ONLINE]= S.[ONLINE]
,T.[OFFLINE]= S.[OFFLINE]
,T.[TOTAL]= S.[TOTAL]
,T.[ONLINE_RATE]= cast(S.[ONLINE_RATE] as decimal(18,3))
,T.[Authorized]= S.[Authorized]
,T.[Granted]= S.[Granted]
,T.[Other]= S.[Other]
,T.[Total_CBSD]= S.[Total_CBSD]
,T.[last_updated_at]=getdate();
'''


merge_dwh_IMSI ='''
MERGE  [T5G_DEV].[dwh].[IMSI_Data] AS T
USING [T5G_DEV].[staging].[IMSI_Data_usage]	AS S
ON left(S.[time],11) = T.[date]
and T.raw_timestamp =s.[Time]
and T.[IMSI]=S.imsi

WHEN NOT MATCHED BY Target THEN

insert ( 
		[raw_timestamp]
      ,[Date]
      ,[Time]
      ,[IMSI]
      ,[Day]
      ,[Year]
      ,[Month]
      ,[Week]
      ,[Data_Usage]
      ,[Data_Usage_GB]
      ,[Data_Usage_TB]
      ,[created_date]
      ,[last_updated_at]
      ,[time_formated]
       )

values (
		[TIME], 
	   cast([TIME] as date),
       cast([TIME] as time),
	   [IMSI],
	   day(cast([TIME] as date)),
   	   year(cast([TIME] as date)),
	   month(cast([TIME] as date)),
	   datepart(week,    cast([TIME] as date)),
	   [Data_Usage],
	   [Data_Usage] /1000000000,
	   [Data_Usage] /1000000000000,
       getdate(),
	   getdate(),
	   cast([Time] as datetime)
)


WHEN MATCHED THEN UPDATE SET
	   T.[raw_timestamp]=s.[TIME]
      ,T.[Date]= cast(s.[TIME] as date)
      ,T.[Time]= cast(s.[TIME] as time)
      ,T.[IMSI]=S.[IMSI]
	  ,T.[Day]=	day(cast(s.[TIME] as date))
      ,T.[Year]= year(cast(s.[TIME] as date))
	  ,T.[Month]= month(cast(s.[TIME] as date))
	  ,T.[Week]= datepart(week,    cast(s.[TIME] as date))
      ,T.[Data_Usage]=s.[Data_Usage]
	  ,T.[Data_Usage_GB]= s.[Data_Usage] /1000000000
	  ,T.[Data_Usage_TB]=s.[Data_Usage] /1000000000000
	  ,T.[last_updated_at]=getdate()
	  ,T.[time_formated] = cast(s.[Time] as datetime);

'''
dwh_merge_queries = [merge_dwh_usm_kpi_cell_level_dl_ri,
                     merge_dwh_usm_kpi_cell_level_erab_accessibility,
                     merge_dwh_usm_kpi_cell_traffic_dl_ul,
                     merge_dwh_usm_kpi_cell_rrc_conn_est,
                     merge_dwh_usm_availability,
                     merge_dwh_usm_s1_Connection_Establishment,
                     merge_dwh_usm_scell_added_information,
                     merge_dwh_unique_users,
                     merge_dwh_DISD_BEC,
                     merge_dwh_BlinQ,
                     merge_dwh_cpe_online,
                    merge_dwh_IMSI

                     ]
