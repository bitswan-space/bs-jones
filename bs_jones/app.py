import asab
import bspump
import bspump.elasticsearch
import fastkafka

from .pipelines import BSJonesPipeline


asab.Config.add_defaults(
	{
		"sybase": {
			"server": 'dwh_reader_2',
			"host": "localhost",
			"port": "3456",
			"database": 'dwhdb',
			"username": 'dcpublic',
			"password": 'En1q_dcpublic#147',
			"driver": '{ODBC Driver for Sybase IQ 16 SQL Server}'
		}
	}
)


class BSJonesApp(bspump.BSPumpApplication):

	def __init__(self):
		super().__init__()

		self.BSPumpService = self.get_service("bspump.PumpService")
		fast_kafka_connection = fastkafka.FastKafkaConnection(self, "KafkaConnection")
		self.BSPumpService.add_connection(fast_kafka_connection)
		self.BSPumpService.add_pipeline(BSJonesPipeline(self, "BSJonesPipeline"))
