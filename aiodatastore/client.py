import io
import os
from typing import Optional, Union

from gcloud.aio.auth import AioSession, Token
from aiodatastore.constants import ReadConsistency
from aiodatastore.query import GQLQuery, Query
from aiodatastore.result import QueryResultBatch

__all__ = ("Datastore",)

try:
    API_URL = f'http://{os.environ["DATASTORE_EMULATOR_HOST"]}/v1'
    DEV_MODE = True
except KeyError:
    API_URL = "https://datastore.googleapis.com/v1"
    DEV_MODE = False

SCOPES = (
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/datastore",
)


class Datastore:
    def __init__(
        self,
        project_id: Optional[str] = None,
        service_file: Optional[Union[str, io.IOBase]] = None,
        namespace: str = "",
    ):
        self._namespace = namespace or os.environ.get("DATASTORE_NAMESPACE", "")
        self._session = AioSession(None)

        if DEV_MODE:
            self._project_id = self._get_project_id()
            self._token = None
        else:
            self._project_id = project_id
            self._token = Token(
                service_file=service_file,
                session=self._session.session,
                scopes=SCOPES,
            )

        if not self._project_id:
            raise RuntimeError("project id not set")

        self.run_query_url = API_URL + f"/projects/{self._project_id}:runQuery"

    def _get_project_id(self):
        return os.environ.get(
            "DATASTORE_PROJECT_ID", os.environ.get("GOOGLE_CLOUD_PROJECT")
        )

    def _get_read_options(self, consistency, transaction):
        if transaction is not None:
            return {"transaction": transaction}
        else:
            return {"readConsistency": consistency.value}

    def _get_partition_id(self):
        return {
            "projectId": self._project_id,
            "namespaceId": self._namespace,
        }

    async def _get_headers(self):
        if DEV_MODE:
            return {}

        token = await self._token.get()
        return {"Authorization": f"Bearer {token}"}

    async def run_query(
        self,
        query: Union[Query, GQLQuery],
        read_opts=ReadConsistency.EVENTUAL,
        transaction=None,
    ) -> QueryResultBatch:
        headers = await self._get_headers()
        req_data = {
            "partitionId": self._get_partition_id(),
            "readOptions": self._get_read_options(read_opts, transaction),
        }
        if isinstance(query, Query):
            req_data["query"] = query.to_ds()
        elif isinstance(query, GQLQuery):
            req_data["gqlQuery"] = query.to_ds()
        else:
            raise RuntimeError(f'unsupported query type: {query}')

        resp = await self._session.request(
            "POST",
            self.run_query_url,
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
