import logging
import pyodbc
from decimal import Decimal

import bspump
import asab

L = logging.getLogger(__name__)


class SybaseEventGenerator(bspump.Generator):

	def __init__(self, app, pipeline, id=None, config=None):
		super().__init__(app, pipeline, id, config)
		self.Username = asab.Config.get(
			"sybase",
			"username"
		)
		self.Password = asab.Config.get(
			"sybase",
			"password"
		)
		self.Host = asab.Config.get(
			"sybase",
			"host"
		)
		self.Port = asab.Config.get(
			"sybase",
			"port"
		)
		self.Driver = asab.Config.get(
			"sybase",
			"driver"
		)
		self.Database = asab.Config.get(
			"sybase",
			"database"
		)
		self.Server = asab.Config.get(
			"sybase",
			"server"
		)
		self.connection_string = "Driver={};UID={};PWD={};Server={};DBN={};CommLinks=TCPIP{};DriverUnicodeType=1".format(self.Driver, self.Username, self.Password, self.Server, self.Database, "{{host={};port={}}}".format(self.Host, self.Port))

		print(self.connection_string)

		self.Query = """select
						date_id,
						hour_id,
						min_id,
						eutrancell,
						[LTE Cell Throughput MAC DL Mbps] = pmRadioThpVolDl/pmSchedActivityCellDl,
						[LTE Cell Throughput MAC UL Mbps] = pmRadioThpVolUl/pmSchedActivityCellUl,
						[LTE Cell Throughput PDCP DL Mbps] =  pmPdcpVolDlDrb/pmSchedActivityCellDl,
						[LTE Cell Throughput PDCP UL Mbps] =  pmPdcpVolUlDrb / pmSchedActivityCellUl,
						[LTE Data Traffic DL MAC GB] =  pmRadioThpVolDl / ( 8 * 1000 * 1000 ),
						[LTE Data Traffic DL MAC PCell GB] =  ( pmRadioThpVolDl - pmRadioThpVolDlSCell ) / ( 8 * 1000 * 1000 ),
						[LTE Data Traffic DL MAC SCell GB] = pmRadioThpVolDlSCell / ( 8 * 1000 * 1000 ),
						[LTE Data Traffic DL PDCP GB] =  pmPdcpVolDlDrb / ( 8 * 1000 * 1000 ),
						[LTE Data Traffic UL MAC GB] =  pmRadioThpVolUl / ( 8 * 1000 * 1000 ),
						[LTE Data Traffic UL MAC PCell GB] =  ( pmRadioThpVolUl - pmRadioThpVolUlSCell ) / ( 8 * 1000 * 1000 ),
						[LTE Data Traffic UL MAC SCell GB] = pmRadioThpVolUlSCell / ( 8 * 1000 * 1000 ),
						[LTE Data Traffic UL PDCP GB] =  ( pmPdcpVolUlDrb ) / ( 8 * 1000 * 1000 ),
						[LTE PRB DL Average Usage] = pmPrbUsedDlSum / pmPrbUsedDlSamp,
						[LTE PRB DL Max Usage] = pmPrbUsedDlMax,
						[LTE Ue Active DL (avg)] =  pmActiveUeDlSum / pmSchedActivityCellDl,
						[LTE Ue Active UL (avg)] =  pmActiveUeUlSum / pmSchedActivityCellUl,
						[LTE Ue RRC Connected (avg)] =  pmRrcConnLevSum / sumpmRrcConnLevSamp,
						[LTE Ue Throughput PDCP DL Mbps] =  ( pmPdcpVolDlDrb - pmPdcpVolDlDrbLastTTI ) / pmUeThpTimeDl,
						[LTE Ue Throughput PDCP UL Mbps] = pmUeThpVolUl / pmUeThpTimeUl,
						[L_01 Cell Availiability] =  1 - pmCellDowntimeAuto / ( 60 * period_duration ),
						[L_02 PS Setup Success Rate] =  ( pmRrcConnEstabSucc /  ( pmRrcConnEstabAtt - pmRrcConnEstabAttReatt ) ) * ( ( LFV.pmErabEstabSuccInitQci + LFV.pmErabEstabSuccAddedQci ) / ( LFV.pmErabEstabAttInitQci + LFV.pmErabEstabAttAddedQci ) ),
						[L_03 PS Drop Rate] = LFV.pmErabRelAbnormalEnbActQci / ( LFV.pmErabRelAbnormalEnbQci + LFV.pmErabRelNormalEnbQci + LFV.pmErabRelMmeQci ),
						[L_03 PS Drops] =  LFV.pmErabRelAbnormalEnbActQci,
						[L_04 VoLTE Call Drop Rate] = ( VolLF.pmErabRelAbnormalEnbActQci + pmErabRelAbnormalMmeActQci ) / ( VolLF.pmErabRelAbnormalEnbQci + VolLF.pmErabRelNormalEnbQci + VolLF.pmErabRelMmeQci ),
						[L_04 VoLTE Call Drops #] = VolLF.pmErabRelAbnormalEnbActQci + pmErabRelAbnormalMmeActQci,
						[L_05 VoLTE Call Setup Success Rate] =  ( pmRrcConnEstabSucc / ( pmRrcConnEstabAtt - pmRrcConnEstabAttReatt ) ) * ( ( VolLF.pmErabEstabSuccInitQci  + VolLF.pmErabEstabSuccAddedQci ) / ( VolLF.pmErabEstabAttInitQci + VolLF.pmErabEstabAttAddedQci - pmErabEstabAttAddedHoOngoingQci ) ),
						[L_07 Data Traffic DL GB] =  ( pmPdcpVolDlDrb + pmPdcpVolDlSrb ) / ( 8 * 1000 * 1000 ),
						[L_07 Data Traffic UL GB] = ( pmPdcpVolUlDrb + pmPdcpVolUlSrb ) / ( 8 * 1000 * 1000 ),
						[L_08 SgNB Addition Attempts #] =  pmEndcSetupUeAtt,
						[L_08 SgNB Addition Success Rate] = (pmEndcSetupUeSucc / pmEndcSetupUeAtt)
						from
						(SELECT
							DATE_ID	DATE_IDV,
							HOUR_ID	  HOUR_IDV,
							MIN_ID	  MIN_IDV,
							substr(EUtranCellTDD,2,5)	 siteV,
							EUtranCellTDD	  EUtranCellV,
							sum(pmErabEstabSuccInitQci)	  pmErabEstabSuccInitQci,
							sum(pmErabEstabSuccAddedQci)	  pmErabEstabSuccAddedQci,
							sum(pmErabEstabAttInitQci)	  pmErabEstabAttInitQci,
							sum(pmErabEstabAttAddedQci)	  pmErabEstabAttAddedQci,
							sum(pmErabRelAbnormalEnbActQci)	  pmErabRelAbnormalEnbActQci,
							sum(pmErabRelAbnormalEnbQci)	  pmErabRelAbnormalEnbQci,
							sum(pmErabRelNormalEnbQci)	  pmErabRelNormalEnbQci,
							sum(pmErabRelMmeQci)	  pmErabRelMmeQci
						FROM
							dcpublic.DC_E_ERBS_EUTRANCELLTDD_V_RAW
						WHERE
							DATE_ID > '2022-02-21'
							and DCVECTOR_INDEX  IN  ( 5, 6, 7, 8, 9, 192  )
						GROUP BY
							DATE_ID,
							HOUR_ID,
							MIN_ID,
							EUtranCellTDD) LFV JOIN
						(SELECT
						DATE_ID	DATE_ID,
						HOUR_ID	HOUR_ID,
						MIN_ID	MIN_ID,
						substr(EUtranCellTDD,2,5) site,
						EUtranCellTDD	EUtranCell,
						sum(PERIOD_DURATION)	PERIOD_DURATION,
						sum(pmCellDowntimeAuto)	pmCellDowntimeAuto,
						sum(pmRrcConnEstabSucc)	pmRrcConnEstabSucc,
						sum(pmRrcConnEstabAtt)	pmRrcConnEstabAtt,
						sum(pmRrcConnEstabAttReatt)	pmRrcConnEstabAttReatt,
						sum(pmPdcpVolDlDrb)	pmPdcpVolDlDrb,
						sum(pmPdcpVolDlSrb)	pmPdcpVolDlSrb,
						sum(pmPdcpVolUlDrb)	pmPdcpVolUlDrb,
						sum(pmPdcpVolUlSrb)	pmPdcpVolUlSrb,
						sum(pmEndcSetupUeSucc)	pmEndcSetupUeSucc,
						sum(pmEndcSetupUeAtt)	pmEndcSetupUeAtt,
						sum(pmRadioThpVolDl)	pmRadioThpVolDl,
						sum(pmRadioThpVolDlSCell)	pmRadioThpVolDlSCell,
						sum(pmRadioThpVolUl)	pmRadioThpVolUl,
						sum(pmRadioThpVolUlSCell)	pmRadioThpVolUlSCell,
						sum(pmPdcpVolDlDrbLastTTI)	pmPdcpVolDlDrbLastTTI,
						sum(pmPdcpVolDlDrbLastTTICa)	pmPdcpVolDlDrbLastTTICa,
						sum(pmSchedActivityCellDl)	pmSchedActivityCellDl,
						sum(pmSchedActivityCellUl)	pmSchedActivityCellUl,
						sum(pmUeThpTimeDl)	pmUeThpTimeDl,
						sum(pmUeThpTimeUl)	pmUeThpTimeUl,
						sum(pmUeThpTimeDlCa)	pmUeThpTimeDlCa,
						sum(pmUeThpTimeUlCa)	pmUeThpTimeUlCa,
						sum(pmRrcConnLevSum)	pmRrcConnLevSum,
						sum(pmRrcConnLevSamp)	sumpmRrcConnLevSamp,
						avg(pmRrcConnLevSamp)	avgpmRrcConnLevSamp,
						max(pmRrcConnMax)	pmRrcConnMax,
						sum(pmRrcConnMaxEndc0)	pmRrcConnMaxEndc0,
						sum(pmRrcConnMaxEndc1)	pmRrcConnMaxEndc1,
						sum(pmRrcConnMaxEndc2)	pmRrcConnMaxEndc2,
						sum(pmRrcConnMaxEndcUnavail)	pmRrcConnMaxEndcUnavail,
						sum(pmActiveUeDlSum)	pmActiveUeDlSum,
						max(pmActiveUeDlMax)	pmActiveUeDlMax,
						sum(pmActiveUeUlSum)	pmActiveUeUlSum,
						max(pmActiveUeUlMax)	pmActiveUeUlMax,
						sum(pmUeThpVolUl)	pmUeThpVolUl,
						sum(pmUeThpVolUlCa)	pmUeThpVolUlCa,
						sum(pmPrbUsedDlSum)	pmPrbUsedDlSum,
						sum(pmPrbUsedDlSamp)	pmPrbUsedDlSamp,
						max(pmPrbUsedDlMax)	pmPrbUsedDlMax
						FROM
							dcpublic.DC_E_ERBS_EUTRANCELLTDD_RAW
						WHERE
							( DATE_ID > '2022-02-21')
						GROUP BY
							DATE_ID,
							MIN_ID,
							HOUR_ID,
							EUtranCellTDD) LF
							on LF.DATE_ID=LFV.DATE_IDV and LF.HOUR_ID=LFV.HOUR_IDV and LF.MIN_ID=LFV.MIN_IDV and LF.EUtranCell=LFV.EUtranCellV
							LEFT JOIN
							(SELECT
							DATE_ID DATE_IDVol,
							HOUR_ID HOUR_IDVol,
							MIN_ID MIN_IDVol,
							substr(EUtranCellTDD,2,5)	siteVol,
							EUtranCellTDD	  EUtranCellVol,
							sum(pmErabRelAbnormalEnbActQci)	  pmErabRelAbnormalEnbActQci,
							sum(pmErabRelAbnormalEnbQci)	  pmErabRelAbnormalEnbQci,
							sum(pmErabRelNormalEnbQci)	  pmErabRelNormalEnbQci,
							sum(pmErabRelMmeQci)	  pmErabRelMmeQci,
							sum(pmErabEstabSuccInitQci)	  pmErabEstabSuccInitQci,
							sum(pmErabEstabSuccAddedQci)	  pmErabEstabSuccAddedQci,
							sum(pmErabEstabAttInitQci)	  pmErabEstabAttInitQci,
							sum(pmErabEstabAttAddedQci)	  pmErabEstabAttAddedQci,
							sum(pmErabEstabAttAddedHoOngoingQci)	  pmErabEstabAttAddedHoOngoingQci,
							sum(pmSessionTimeDrbQci)	  pmSessionTimeDrbQci,
							sum(pmErabRelAbnormalMmeActQci)	  pmErabRelAbnormalMmeActQci
						FROM
							dcpublic.DC_E_ERBS_EUTRANCELLTDD_V_RAW
						WHERE
							DCVECTOR_INDEX  IN  ( 1  )
							AND
							DATE_ID > '2022-02-21'
						GROUP BY
							DATE_ID,
							HOUR_ID,
							MIN_ID,
							EUtranCellTDD
							) VolLF ON LF.DATE_ID=VolLF.DATE_IDVol and LF.HOUR_ID=VolLF.HOUR_IDVol and LF.MIN_ID=VolLF.MIN_IDVol and LF.EUtranCell=VolLF.EUtranCellVol
	"""

	async def generate(self, context, event, depth):
		cnxn = pyodbc.connect(self.connection_string)
		cursor = cnxn.cursor()
		cursor.execute(self.Query)
		columns = [column[0] for column in cursor.description]

		for row in cursor.fetchall():
			row_data = []
			for data in row:
				if type(data) is Decimal:
					row_data.append(float(data))
				else:
					row_data.append(str(data))
			event_new = dict(zip(columns, row_data))
			try:
				await self.Pipeline.inject(context, event_new, depth)
			except Exception as e:
				print(e)

		cursor.close()
		cnxn.close()
