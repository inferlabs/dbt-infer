import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import agate
import requests

import dbt.exceptions
from dbt.adapters.base import BaseConnectionManager, Credentials
from dbt.adapters.factory import get_adapter_class_by_name, load_plugin
from dbt.contracts.connection import AdapterResponse
from dbt.logger import GLOBAL_LOGGER as logger


@dataclass
class InferAdapterResponse(AdapterResponse):
    something: Optional[int] = None


@dataclass
class InferCredentials(Credentials):

    apikey: str
    data_config: Dict[str, Any]

    _ALIASES = {"url": "database", "username": "schema"}

    def __init__(self, database: str, schema: str, apikey: str, data_config: Dict[str, Any]):
        self.url = os.getenv("INFER_URL", database)
        self.username = os.getenv("INFER_USER", schema)
        self.apikey = os.getenv("INFER_KEY", apikey)
        self.data_config = data_config
        # setting up the adapter class before we use it
        from . import Plugin

        Plugin.dependencies = [data_config["type"]]
        from .impl import InferAdapter

        InferAdapter.SourceAdapter = InferConnectionManager.get_source_module(data_config)[0]
        InferAdapter.Relation = InferAdapter.SourceAdapter.Relation
        InferAdapter.Column = InferAdapter.SourceAdapter.Column

    def __getattr__(self, name):
        if name == "adapter_credentials":
            _, cls = InferConnectionManager.get_source_module(self.data_config)
            data = cls.translate_aliases(self.data_config)
            cls.validate(data)
            self.adapter_credentials = cls.from_dict(data)
            return self.adapter_credentials
        return getattr(self.adapter_credentials, name)

    def __getattribute__(self, item):
        if item == "database":
            return self.data_config.database
        elif item == "schema":
            return self.data_config.schema
        elif item == "project":
            return self.data_config.project.strip("\"'")
        return object.__getattribute__(self, item)

    @property
    def type(self):
        return "infer"

    @property
    def unique_field(self):
        return self.database

    def _connection_keys(self):
        return ("database", "schema")


class InferSession:
    def __init__(self, credentials):
        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f'Token token="{credentials.apikey}",'
                f"email={credentials.username}",
                "Content-Type": "application/json",
            }
        )
        url = f"{credentials.url}/api/v1"
        r = session.get(f"{url}/users/me")
        if r.status_code != 200:
            raise RuntimeError(f"Failed to connect to Infer server {url} end point 'users/me'")
        self.__baseurl = credentials.url
        self.__url = url
        self.__session = session

    def parse(self, sql):
        r = self.__session.post(f"{self.__url}/parse", json={"q": sql})
        if r.status_code != 200:
            raise RuntimeError(
                f"Failed to connect to Infer server {self.__url} end "
                f"point 'parse' got return code {r.status_code}"
            )
        return r.json()["result"]

    def single_dataset_run(self, dataset_id, name, query):
        r = self.__session.post(
            f"{self.__url}/datasets/{dataset_id}/results",
            json={"result": {"name": name, "query": query}},
        )
        if r.status_code != 200:
            raise RuntimeError(
                f"Failed to connect to Infer server {self.__url} "
                f"to run query={query} on dataset_id={dataset_id} "
                f"got return code {r.status_code}"
            )
        return r.json()["id"]

    def dbt_run(self, name, query, datasets):
        r = self.__session.post(
            f"{self.__url}/dbt_runs",
            json={
                "dbt_run": {
                    "name": name,
                    "description": name,
                    "query": query,
                    "datasets": datasets,
                }
            },
        )
        if r.status_code != 200:
            raise RuntimeError(
                f"Failed to run `dbt_run` on {self.__url} "
                f"with query={query} got return code {r.status_code}"
            )
        return r.json()["id"]

    def get_dbt_result(self, result_id):
        r = self.__session.get(f"{self.__url}/dbt_runs/{result_id}")
        if r.status_code != 200:
            raise RuntimeError(
                f"Failed to connect to Infer server {self.__url} "
                f"to retrieve result_id={result_id} "
                f"got return code {r.status_code}"
            )
        r_json = r.json()
        r_status = r_json["status"]
        rtn_obj = None
        if r_status == "COMPLETED":
            url = f"{self.__baseurl}{r_json['output_url']}"
            r = self.__session.get(url)
            if r.status_code != 200:
                raise RuntimeError(
                    f"Failed to connect to retrieve result {url} "
                    f"got return code {r.status_code}"
                )
            rtn_obj = r.content
        elif r_status == "ERROR":
            rtn_obj = r_json["raw_output"]
        return r_status, rtn_obj

    def get_result(self, result_id):
        r = self.__session.get(f"{self.__url}/results/{result_id}")
        if r.status_code != 200:
            raise RuntimeError(
                f"Failed to connect to Infer server {self.__url} "
                f"to retrieve result_id={result_id} "
                f"got return code {r.status_code}"
            )
        r_json = r.json()
        r_status = r_json["status"]
        rtn_obj = None
        if r_status == "COMPLETED":
            url = f"{self.__baseurl}{r_json['result_url']}"
            r = self.__session.get(url)
            if r.status_code != 200:
                raise RuntimeError(
                    f"Failed to connect to retrieve result {url} "
                    f"got return code {r.status_code}"
                )
            rtn_obj = r.content
        return r_status, rtn_obj

    def delete_dataset(self, dataset_id):
        r = self.__session.delete(f"{self.__url}/datasets/{dataset_id}")
        if r.status_code not in [200, 204]:
            raise RuntimeError(
                f"Failed to connect to Infer server {self.__url} "
                f"end point 'datasets/{dataset_id}' got "
                f"return code {r.status_code}"
            )

    def create_dataset(self, name, query, encode_data):
        data = {
            "dataset": {
                "name": name,
                "description": query,
                "source_file": {"data": encode_data},
            }
        }
        r = self.__session.post(f"{self.__url}/datasets", json=data)
        if r.status_code != 200:
            raise RuntimeError(
                f"Failed to connect to Infer server {self.__url} "
                f"end point 'datasets' got return code {r.status_code}"
            )
        return r.json()["id"]

    def close(self):
        self.__session.close()


class InferConnectionManager(BaseConnectionManager):
    TYPE = "infer"

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except Exception as exc:
            logger.debug("myadapter error: {}".format(str(exc)))
            raise dbt.exceptions.DatabaseException(str(exc))
        except Exception as exc:
            logger.debug("Error running SQL: {}".format(sql))
            logger.debug("Rolling back transaction.")
            raise dbt.exceptions.RuntimeException(str(exc))

    @classmethod
    def get_source_module(cls, source):
        source_type = source["type"]
        credentials = load_plugin(source_type)
        return get_adapter_class_by_name(source_type), credentials

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials
        try:
            connection.state = "open"
            data_adapter, _ = cls.get_source_module(connection.credentials.data_config)
            connection.handle = {
                "session": InferSession(credentials),
                "data_adapter": data_adapter,
            }
        except Exception as e:
            raise e
        return connection

    @classmethod
    def close(cls, connection):
        if connection.state == "open":
            connection.handle["session"].close()
            connection.state = "closed"
        return connection

    def begin(self):
        pass

    def commit(self):
        pass

    def cancel_open(self) -> None:
        pass

    def execute(
        self, sql, auto_begin=False, fetch=None
    ) -> Tuple[InferAdapterResponse, agate.Table]:
        raise dbt.exceptions.NotImplementedException(
            "`execute` not implemented on InferConnectionManager"
        )

    @classmethod
    def get_response(cls, cursor):
        # we do not support rich metadata
        pass

    def cancel(self, connection):
        # we do not support cancelling a request
        pass
