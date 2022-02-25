import logging
import pyodbc
from decimal import Decimal
from datetime import datetime, timedelta

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

		self.QueryLocation = asab.Config.get(
			"sybase",
			"query"
		)

		self.connection_string = "Driver={};UID={};PWD={};Server={};DBN={};CommLinks=TCPIP{};DriverUnicodeType=1".format(self.Driver, self.Username, self.Password, self.Server, self.Database, "{{host={};port={}}}".format(self.Host, self.Port))

		L.info("Connection string".format(self.connection_string))

	async def generate(self, context, event, depth):
		current_time = self.round_minutes(datetime.now(), 15)

		with open(self.QueryLocation, 'r') as q:
			query = q.read().format(current_time)

		print(query)
		cnxn = pyodbc.connect(self.connection_string)
		cursor = cnxn.cursor()
		cursor.execute(query)
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

	def round_minutes(self, dt, resolutionInMinutes):
		dtTrunc = dt.replace(second=0, microsecond=0)
		excessMinutes = (dtTrunc.hour*60 + dtTrunc.minute) % resolutionInMinutes
		return dtTrunc + timedelta(minutes=-excessMinutes)
