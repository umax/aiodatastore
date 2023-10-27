import os
from typing import Any, Dict, List, IO, Optional, Union

from gcloud.aio.auth import AioSession, Token
from aiodatastore.commit import CommitResult
from aiodatastore.constants import Mode, ReadConsistency
from aiodatastore.entity import Entity
from aiodatastore.key import Key
from aiodatastore.lookup import LookupResult
from aiodatastore.mutation import (
    Mutation,
    InsertMutation,
    UpsertMutation,
    UpdateMutation,
    DeleteMutation,
)
from aiodatastore.query import GQLQuery, Query, QueryResultBatch
from aiodatastore.transaction import ReadOnlyOptions, ReadWriteOptions

__all__ = ("Datastore",)

try:
    API_URL = f'http://{os.environ["DATASTORE_EMULATOR_HOST"]}/v1'
    EMULATOR_MODE = True
except KeyError:
    API_URL = "https://datastore.googleapis.com/v1"
    EMULATOR_MODE = False

SCOPES = (
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/datastore",
)


class Datastore:
    def __init__(
        self,
        project_id: str,
        service_file: Union[str, IO, None] = None,
        namespace: str = "",
    ):
        self._project_id = project_id
        self._namespace = namespace
        self._session = AioSession(None)
        self._token = None
        if not EMULATOR_MODE and service_file:
            self._token = Token(
                service_file=service_file,
                session=self._session.session,
                scopes=list(SCOPES),
            )

    def _get_read_options(
        self,
        consistency: ReadConsistency,
        transaction_id: Optional[str],
    ):
        if transaction_id is not None:
            return {"transaction": transaction_id}

        return {"readConsistency": consistency.value}

    def _get_partition_id(self):
        return {
            "projectId": self._project_id,
            "namespaceId": self._namespace,
        }

    async def _get_headers(self):
        if self._token is None:
            return {}

        token = await self._token.get()
        return {"Authorization": f"Bearer {token}"}

    # https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/allocateIds
    async def allocate_ids(self, keys: List[Key]) -> List[Key]:
        headers = await self._get_headers()
        req_data = {"keys": [key.to_ds() for key in keys]}

        resp = await self._session.request(
            "POST",
            f"{API_URL}/projects/{self._project_id}:allocateIds",
            headers=headers,
            json=req_data,
        )
        resp_data = await resp.json()
        return [Key.from_ds(key) for key in resp_data["keys"]]

    # https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/reserveIds
    async def reserve_ids(self, keys: List[Key], database_id: str = ""):
        headers = await self._get_headers()
        req_data = {
            "databaseId": database_id,
            "keys": [key.to_ds() for key in keys],
        }

        await self._session.request(
            "POST",
            f"{API_URL}/projects/{self._project_id}:reserveIds",
            headers=headers,
            json=req_data,
        )

    # https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/lookup
    async def lookup(
        self,
        keys: List[Key],
        consistency: ReadConsistency = ReadConsistency.EVENTUAL,
        transaction_id: Optional[str] = None,
    ) -> LookupResult:
        headers = await self._get_headers()
        req_data = {
            "keys": [key.to_ds() for key in keys],
            "readOptions": self._get_read_options(consistency, transaction_id),
        }

        resp = await self._session.request(
            "POST",
            f"{API_URL}/projects/{self._project_id}:lookup",
            headers=headers,
            json=req_data,
        )
        resp_data = await resp.json()
        return LookupResult.from_ds(resp_data)

    # https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/beginTransaction
    async def begin_transaction(
        self,
        opts: Optional[Union[ReadOnlyOptions, ReadWriteOptions]] = None,
    ) -> str:
        headers = await self._get_headers()

        req_data: Dict[str, Any] = {}
        if opts is not None:
            req_data = opts.to_ds()

        resp = await self._session.request(
            "POST",
            f"{API_URL}/projects/{self._project_id}:beginTransaction",
            headers=headers,
            json=req_data,
        )
        resp_data = await resp.json()
        return resp_data["transaction"]

    # https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/rollback
    async def rollback(self, transaction_id: str) -> None:
        headers = await self._get_headers()
        req_data = {"transaction": transaction_id}

        await self._session.request(
            "POST",
            f"{API_URL}/projects/{self._project_id}:rollback",
            headers=headers,
            json=req_data,
        )

    # https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/commit
    async def commit(
        self,
        mutations: List[Union[Mutation, DeleteMutation]],
        transaction_id: Optional[str] = None,
        mode: Optional[Mode] = None,
    ) -> CommitResult:
        headers = await self._get_headers()
        mode = mode or Mode.TRANSACTIONAL

        req_data = {
            "mode": mode.value,
            "mutations": [mut.to_ds() for mut in mutations],
        }
        if mode == Mode.TRANSACTIONAL and transaction_id is None:
            transaction_id = await self.begin_transaction()
            req_data["transaction"] = transaction_id

        resp = await self._session.request(
            "POST",
            f"{API_URL}/projects/{self._project_id}:commit",
            headers=headers,
            json=req_data,
        )
        resp_data = await resp.json()
        return CommitResult.from_ds(resp_data)

    async def insert(self, entity: Entity) -> CommitResult:
        mutation = InsertMutation(entity)
        return await self.commit([mutation])

    async def upsert(self, entity: Entity) -> CommitResult:
        mutation = UpsertMutation(entity)
        return await self.commit([mutation])

    # TODO: handle entity not found
    async def update(self, entity: Entity) -> CommitResult:
        mutation = UpdateMutation(entity)
        return await self.commit([mutation])

    # TODO: handle entity not found
    async def delete(self, obj: Union[Entity, Key]) -> CommitResult:
        key = obj.key if isinstance(obj, Entity) else obj
        mutation = DeleteMutation(key)  # type: ignore
        return await self.commit([mutation])

    # https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery
    async def run_query(
        self,
        query: Union[Query, GQLQuery],
        consistency: ReadConsistency = ReadConsistency.EVENTUAL,
        transaction_id: Optional[str] = None,
    ) -> QueryResultBatch:
        headers = await self._get_headers()

        req_data = {
            "partitionId": self._get_partition_id(),
            "readOptions": self._get_read_options(consistency, transaction_id),
        }
        if isinstance(query, Query):
            req_data["query"] = query.to_ds()
        elif isinstance(query, GQLQuery):
            req_data["gqlQuery"] = query.to_ds()
        else:
            raise RuntimeError(f"unsupported query type: {query}")

        resp = await self._session.request(
            "POST",
            f"{API_URL}/projects/{self._project_id}:runQuery",
            headers,
            json=req_data,
        )
        resp_data = await resp.json()
        return QueryResultBatch.from_ds(resp_data["batch"])

    async def close(self):
        await self._session.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()
