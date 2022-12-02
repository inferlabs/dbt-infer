import base64
import io
import os
import uuid
from time import sleep
from typing import Any, Dict, List, Optional, Tuple

import agate

from dbt.adapters.base import BaseAdapter
from dbt.adapters.base import Column as BaseColumn
from dbt.adapters.base.meta import available
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.infer import InferConnectionManager
from dbt.clients import agate_helper
from dbt.contracts.connection import AdapterResponse, Connection
from dbt.contracts.results import RunStatus
from dbt.events import AdapterLogger
from dbt.exceptions import RuntimeException
from dbt.task.seed import SeedTask

logger = AdapterLogger("Infer")


class InferAdapter(BaseAdapter):
    SourceAdapter = None
    ConnectionManager = InferConnectionManager
    Relation = None
    Column = None

    def __init__(self, config):
        super().__init__(config)
        self.config = config
        self.data_adapter = None
        self.create_view_mode = False

    @classmethod
    def is_cancelable(cls) -> bool:
        return False

    @available.parse(lambda *a, **k: (None, None))
    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
    ) -> Tuple[Connection, Any]:
        adapter = self.get_data_adapter()
        with adapter.connection_named("master"):
            return adapter.add_query(sql, auto_begin, bindings, abridge_sql_log)

    @classmethod
    def date_function(cls):
        return cls.SourceAdapter.date_function()

    @available.parse(lambda *a, **k: ("", agate_helper.empty_table()))
    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[AdapterResponse, agate.Table]:
        adapter = self.get_data_adapter()
        thread_connection = self.connections.get_if_exists()
        if not thread_connection:
            # we assume that we are in a nested execution
            with adapter.connection_named("master"):
                return adapter.execute(sql, auto_begin, fetch)
        data_source = thread_connection.handle
        session = data_source["session"]
        parsed_sql = session.parse(sql)
        if parsed_sql.get("errors", None):
            error = parsed_sql["errors"][0]
            logger.info(f"Failed to parse SQL as SQL-inf: ({error['type']}) {error['value']}")
            logger.info(f"Will try to process with {adapter.__class__.__name__}.")
        if not parsed_sql["infer_commands"]:
            with adapter.connection_named("master"):
                return adapter.execute(sql, auto_begin, fetch)

        if self.create_view_mode:
            raise RuntimeException("SQL-inf commands can only be used with TABLE materializations")

        datasets = []
        with adapter.connection_named("load_queries"):
            for query in parsed_sql["load_queries"]:
                result = adapter.execute(query, False, True)
                dataset_name = "tmp_" + str(uuid.uuid4()).replace("-", "")
                fp = io.StringIO()
                result[1].to_csv(fp)
                encoded_fp = base64.b64encode(fp.getvalue().encode())
                datasets.append(
                    {
                        "base64": encoded_fp.decode(),
                        "filename": dataset_name,
                        "table_query": query,
                    }
                )
                fp.close()

        result_id = session.dbt_run(name="dbt_run", query=sql, datasets=datasets)

        keep_running = True
        result = {}
        result_status = "STARTED"
        while keep_running:
            result_status, result = session.get_dbt_result(result_id)
            keep_running = result_status in ["STARTED", "RUNNING"]
            sleep(3)
        if result_status == "ERROR":
            if result:
                raise RuntimeException(
                    f"Failed to run SQL-inf command: "
                    f"({result.get('type', 'Unknown Internal Error')}) {result.get('error', '')}"
                )
            else:
                raise RuntimeException(f"Failed to run SQL-inf command: Internal Error")
        if not result:
            raise RuntimeException(f"Failed to get result for SQL-inf command sql={sql}")

        temp_table_name = "tmp_infer_" + str(uuid.uuid4()).replace("-", "")
        output_file_path = f"seeds/{temp_table_name}.csv"
        with open(output_file_path, "w+b") as fp:
            fp.write(result)

        class SeedArgs:
            state = None
            single_threaded = True
            selector_name = None
            select = [temp_table_name]
            exclude = None
            show = None

        task = SeedTask(SeedArgs(), adapter.config)
        r = task.run()
        if r.results[0].status == RunStatus.Error:
            raise RuntimeException(r.results[0].message)

        os.remove(output_file_path)

        with adapter.connection_named("upload_infer_results"):
            full_temp_table_name = f"{adapter.config.credentials.schema}.{temp_table_name}"
            outer_sql = parsed_sql["outer"].replace(
                "__INNER_SELECT__", f"SELECT * FROM {full_temp_table_name}"
            )
            outer_result = adapter.execute(outer_sql, False, True)

            database = adapter.config.credentials.database
            schema = adapter.config.credentials.schema
            relation = adapter.Relation.create(
                database=database,
                schema=schema,
                identifier=temp_table_name,
                type="table",
                quote_policy=adapter.config.quoting,
            )

        adapter.drop_relation(relation)

        return outer_result

    def get_data_adapter(self):
        if not self.data_adapter:
            data_source = self.connections.get_thread_connection().handle
            self.data_adapter = data_source["data_adapter"](self.config)
        return self.data_adapter

    @available.parse(lambda *a, **k: {})
    def set_create_view_mode(self, view_mode):
        self.create_view_mode = view_mode

    @available.parse(lambda *a, **k: {})
    def adapter_macro(self, macro, macro_dict):
        data_adapter = self.get_data_adapter()
        return data_adapter.execute_macro(
            f"{data_adapter.type()}__{macro}",
            kwargs=macro_dict,
            manifest=data_adapter._macro_manifest,
        )

    @available.parse(lambda *a, **k: {})
    def get_view_options(self, config, node):
        adapter = self.get_data_adapter()
        with adapter.connection_named("master"):
            return adapter.get_common_options(config, node)

    def list_relations_without_caching(self, schema_relation: BaseRelation) -> List[BaseRelation]:
        adapter = self.get_data_adapter()
        with adapter.connection_named("master"):
            return adapter.list_relations_without_caching(schema_relation)

    def __getattribute__(self, item):
        if item == "_available_":
            adapter = self.get_data_adapter()
            return frozenset().union(adapter._available_, object.__getattribute__(self, item))
        if item == "database":
            return self.data_config.database
        elif item == "schema":
            return self.data_config.schema
        elif item == "project":
            return self.data_config.project.strip("\"'")
        return object.__getattribute__(self, item)

    def __getattr__(self, name):
        return getattr(self.get_data_adapter(), name)

    @available.parse(lambda *a, **k: True)
    def is_replaceable(self, relation, conf_partition, conf_cluster) -> bool:
        adapter = self.get_data_adapter()
        return adapter.is_replaceable(relation, conf_partition, conf_cluster)

    @available.parse(lambda *a, **k: {})
    def get_table_options(
        self, config: Dict[str, Any], node: Dict[str, Any], temporary: bool
    ) -> Dict[str, Any]:
        adapter = self.get_data_adapter()
        return adapter.get_table_options(config, node, temporary)

    @available
    def parse_partition_by(self, raw_partition_by: Any) -> Any:
        adapter = self.get_data_adapter()
        return adapter.parse_partition_by(raw_partition_by)

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return cls.SourceAdapter.convert_text_type(agate_table, col_idx)

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return cls.SourceAdapter.convert_number_type(agate_table, col_idx)

    @classmethod
    def convert_boolean_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return cls.SourceAdapter.convert_boolean_type(agate_table, col_idx)

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return cls.SourceAdapter.convert_datetime_type(agate_table, col_idx)

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return cls.SourceAdapter.convert_date_type(agate_table, col_idx)

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return cls.SourceAdapter.convert_time_type(agate_table, col_idx)

    @available.parse_none
    def create_schema(self, relation: BaseRelation):
        adapter = self.get_data_adapter()
        with adapter.connection_named("master"):
            return adapter.create_schema(relation)

    @available.parse_none
    def drop_schema(self, relation: BaseRelation):
        adapter = self.get_data_adapter()
        with adapter.connection_named("master"):
            return adapter.drop_schema(relation)

    @available
    @classmethod
    def quote(cls, identifier: str) -> str:
        return cls.SourceAdapter.quote(identifier)

    def quote(self, identifier: str) -> str:
        return self.get_data_adapter().quote(identifier)

    def expand_column_types(self, goal: BaseRelation, current: BaseRelation) -> None:
        adapter = self.get_data_adapter()
        with adapter.connection_named("master"):
            return adapter.expand_column_types(goal, current)

    @available.parse_none
    def drop_relation(self, relation: BaseRelation) -> None:
        adapter = self.get_data_adapter()
        with adapter.connection_named("master"):
            return adapter.drop_relation(relation)

    @available.parse_none
    def truncate_relation(self, relation: BaseRelation) -> None:
        adapter = self.get_data_adapter()
        with adapter.connection_named("master"):
            return adapter.truncate_relation(relation)

    @available.parse_none
    def rename_relation(self, from_relation: BaseRelation, to_relation: BaseRelation) -> None:
        adapter = self.get_data_adapter()
        with adapter.connection_named("master"):
            return adapter.rename_relation(from_relation, to_relation)

    @available.parse_list
    def get_columns_in_relation(self, relation: BaseRelation) -> List[BaseColumn]:
        adapter = self.get_data_adapter()
        with adapter.connection_named("master"):
            return adapter.get_columns_in_relation(relation)

    def list_schemas(self, database: str) -> List[str]:
        adapter = self.get_data_adapter()
        with adapter.connection_named("master"):
            return adapter.list_schemas(database.strip("\"'"))
