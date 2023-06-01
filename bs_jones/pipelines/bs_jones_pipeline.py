import logging

import asab
import bspump
import bspump.file
import bspump.trigger
import bspump.common
import bspump.abc.source
import fastkafka

from .generators import SybaseEventGenerator


L = logging.getLogger(__name__)


class LoadSource(bspump.TriggerSource):

	def __init__(self, app, pipeline, choice=None, id=None, config=None):
		super().__init__(app, pipeline, id=id, config=config)

	async def cycle(self):
		await self.process("event")


class BSJonesPipeline(bspump.Pipeline):
	def __init__(self, app, pipeline_id):
		super().__init__(app, pipeline_id)

		try:
			self.QueryInterval = int(eval(asab.Config.get("sybase", "query_interval")))
		except Exception as e:
			L.debug("query_interval in config must be either an expression or an number {}".format(e))

		self.build(
			LoadSource(app, self).on(bspump.trigger.OpportunisticTrigger(app, self, chilldown_period=self.QueryInterval)),
			SybaseEventGenerator(app, self),
			bspump.common.StdDictToJsonParser(app, self),
			bspump.common.StringToBytesParser(app, self),
			fastkafka.FastKafkaSink(app, self, "KafkaConnection", id="KafkaSink")
		)

	def handle_error(*args, **kwargs):
		return True