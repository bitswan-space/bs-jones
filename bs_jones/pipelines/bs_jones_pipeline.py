import logging

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
		self.Number = 500000

	async def cycle(self):
		await self.process("event")


class BSJonesPipeline(bspump.Pipeline):
	def __init__(self, app, pipeline_id):
		super().__init__(app, pipeline_id)

		self.build(
			# 43200
			LoadSource(app, self).on(bspump.trigger.PeriodicTrigger(app, 10)),
			SybaseEventGenerator(app, self),
			bspump.common.StdDictToJsonParser(app, self),
			bspump.common.StringToBytesParser(app, self),
			fastkafka.FastKafkaSink(app, self, "KafkaConnection", id="KafkaSink")
		)
