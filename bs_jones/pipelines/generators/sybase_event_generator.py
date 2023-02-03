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

		self.resolution = asab.Config.get(
			"sybase",
			"resolution"
		)

		self.daily = asab.Config.get(
			"sybase",
			"daily"
		)

		self.connection_string = "Driver={};UID={};PWD={};Server={};DBN={};CommLinks=TCPIP{};DriverUnicodeType=1".format(self.Driver, self.Username, self.Password, self.Server, self.Database, "{{host={};port={}}}".format(self.Host, self.Port))

		L.log(asab.LOG_NOTICE, "Connection string {}".format(self.connection_string))

	async def generate(self, context, event, depth):

		try:
			self.resolution = eval(self.resolution)
		except Exception as e:
			L.debug("resolution in config must be either an expression or an number {}".format(e))

		current_time = self.round_minutes(datetime.now(), eval(self.resolution))

		try:
			self.daily = int(self.daily)
		except Exception as e:
			L.debug("Incorrect Daily format. Please use 0 for False 1 for True {}".format(e))
		if (self.daily):
			current_time = datetime.now() - timedelta(1)
			current_time = current_time.date()

		with open(self.QueryLocation, 'r') as q:
			query = q.read().format(current_time)

		L.log(asab.LOG_NOTICE, "Currently executing {}".format(query))
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
				# TODO: deal with this better
				L.debug("Nonetype {}".format(e))

		cursor.close()
		cnxn.close()

	def round_minutes(self, dt, resolutionInMinutes):
		dtTrunc = dt.replace(second=0, microsecond=0)
		excessMinutes = (dtTrunc.hour * 60 + dtTrunc.minute) % resolutionInMinutes
		return dtTrunc + timedelta(minutes=-excessMinutes)
