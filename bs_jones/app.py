import asab
import bspump
import bspump.elasticsearch

from migration_ericsson.pipeline import BSJonesPipeline

asab.Config.add_defaults(
	{
		"section": {
			"name": "stuff"
		}
	}
)


class BSJonesApp(bspump.BSPumpApplication):

	def __init__(self):
		super().__init__()

		svc = self.get_service("bspump.PumpService")

		svc.add_pipeline(BSJonesPipeline(self, "BSJonesPipeline"))
