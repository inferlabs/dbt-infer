from dbt.adapters.base import AdapterPlugin
from dbt.adapters.infer.connections import InferConnectionManager  # noqa
from dbt.adapters.infer.connections import InferCredentials
from dbt.adapters.infer.impl import InferAdapter
from dbt.include import infer

Plugin = AdapterPlugin(
    adapter=InferAdapter,
    credentials=InferCredentials,
    include_path=infer.PACKAGE_PATH,
    dependencies=[],
)
