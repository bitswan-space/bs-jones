import logging
import pyodbc
from decimal import Decimal
from datetime import datetime, timedelta
import time

import bspump
import asab

import traceback

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

		try:
			self.previous_timestamp = open("/conf/previous_timestamp.txt", "r").read()
		except FileNotFoundError:
			self.previous_timestamp = "NULL"

		self.connection_string = "Driver={};UID={};PWD={};Server={};DBN={};CommLinks=TCPIP{};DriverUnicodeType=1".format(self.Driver, self.Username, self.Password, self.Server, self.Database, "{{host={};port={}}}".format(self.Host, self.Port))

		L.debug("Connection string {}".format(self.connection_string))

	async def generate(self, context, event, depth):
		"""
		Ignore errors caused by transient SQL connection issues.
		"""
		try:
			await self._generate(context, event, depth)
		except Exception as e:
			# log full error as a stacktrace using traceback module
			L.log(asab.LOG_NOTICE, "SybaseEventGenerator error: {} {}".format(traceback.format_exc(), e))


	async def _generate(self, context, event, depth):

		try:
			self.resolution = eval(str(self.resolution))
		except Exception as e:
			L.log(asab.LOG_NOTICE, "resolution in config must be either an expression or an number {}".format(e))

		current_time = self.round_minutes(datetime.now(), self.resolution)

		try:
			self.daily = int(self.daily)
		except Exception as e:
			L.log(asab.LOG_NOTICE, "Incorrect Daily format. Please use 0 for False 1 for True {}".format(e))
		if (self.daily):
			current_time = datetime.now() - timedelta(1)
			current_time = current_time.date()

		timestamp_field = asab.Config.get("sybase", "timestamp_field", fallback=None)
		with open(self.QueryLocation, 'r') as q:
			context = {
				"current_time": current_time,
			}
			if timestamp_field:
				if (self.previous_timestamp == "NULL"):
					self.previous_timestamp = current_time
				context["previous_time"] = self.previous_timestamp
			query = q.read().format(
				**context
			)

		try:
			start_connection = time.time()
			cnxn = pyodbc.connect(self.connection_string)
			elapsed_connection = time.time() - start_connection
		except Exception as e:
			L.log(asab.LOG_NOTICE, "Connection failed {}".format(e))
			return
		cursor = cnxn.cursor()

		start_time = time.time()

		cursor.execute(query)

		elapsed_time = time.time() - start_time

		L.log(asab.LOG_NOTICE, "Connection took {} seconds, Query took {} seconds".format(elapsed_connection, elapsed_time))

		columns = [column[0] for column in cursor.description]

		for row in cursor.fetchall():
			row_data = []
			for data in row:
				if type(data) is Decimal:
					row_data.append(float(data))
				else:
					row_data.append(str(data))
			event_new = dict(zip(columns, row_data))
			print(event_new)
			if timestamp_field:
				self.previous_timestamp = event_new[timestamp_field]
				open("/conf/previous_timestamp.txt", "w").write(self.previous_timestamp)
			try:
				self.Pipeline.inject(context, event_new, depth)
			except Exception as e:
				# TODO:  deal with this better
				L.log("Nonetype {}".format(e))

		cursor.close()
		cnxn.close()

	def round_minutes(self, dt, resolutionInMinutes):
		dtTrunc = dt.replace(second=0, microsecond=0)
		excessMinutes = (dtTrunc.hour * 60 + dtTrunc.minute) % resolutionInMinutes
		return dtTrunc + timedelta(minutes=-excessMinutes)
