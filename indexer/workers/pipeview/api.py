"""
API server for pipeview database
"""

import argparse
import datetime as dt
import logging
import os
from typing import Annotated

# PyPI:
import uvicorn
from fastapi import FastAPI, Query
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio.session import async_sessionmaker

from indexer.app import App

# local directory:
from indexer.workers.pipeview.models import Crumb, CrumbKey

logger = logging.getLogger("pipeview-api")

DATABASE_URL = os.environ.get("DATABASE_URL", "")
PIPEVIEW_DAYS = os.environ.get("PIPEVIEW_DAYS")

# pool_size, echo??
async_engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)

# https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html#preventing-implicit-io-when-using-asyncsession
AsyncSession = async_sessionmaker(async_engine, expire_on_commit=False)

app = FastAPI(
    title="PipeView",
    description="Look inside story-indexer pipeline",
)


class PipeViewAPI(App):
    """
    Indexer app: initializes logging, stats with command line options
    """

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument(
            "--api-port", type=int, default=os.environ.get("PIPEVIEW_API_PORT", 8000)
        )

    def main_loop(self) -> None:
        assert self.args
        uvicorn.run(app, host="0.0.0.0", port=self.args.api_port, log_config=None)


# instantiate early for access to stats calls (pvapp.{incr,gauge,timing})
pvapp = PipeViewAPI("pipeview-api", "PipeView API Server")


# XXX avoid having all rows in memory by creating new endpoint w/:
#        async def generator(stream):
#            async for row in stream:
#                yield json.dumps(row._asdict()) + "\n"
#        results = await session.stream(query)
#        return StreamingReponse(generator(results), media_type="application/x-ndjson")


class SumParams(BaseModel):
    """
    Pydantic model is necessary to forbid extra params:
    """

    model_config = {"extra": "forbid"}

    col: list[CrumbKey] = []
    # filters
    source_id: int | None = None
    feed_id: int | None = None
    domain: str | None = None
    app: str | None = None
    status: str | None = None
    start_date: dt.date | None = None
    end_date: dt.date | None = None


@app.get("/sum/")
async def sum(
    params: Annotated[SumParams, Query()],
) -> list[dict[str, int | str | None]]:

    columns = [getattr(Crumb, c) for c in params.col]
    query = select(*columns, func.sum(Crumb.count), func.count(Crumb.id))

    # default start date to day range kept by pruner
    # (ignore dead feeds from the deep past)
    # if you want ALL dates, pass start_date=2000-01-01
    start_date = params.start_date
    if start_date is None and PIPEVIEW_DAYS and PIPEVIEW_DAYS.isdigit():
        start_date = dt.datetime.utcnow().date() - dt.timedelta(days=int(PIPEVIEW_DAYS))

    # apply filters
    if params.source_id is not None:
        query = query.where(Crumb.source_id == params.source_id)
    if params.feed_id is not None:
        query = query.where(Crumb.feed_id == params.feed_id)
    if params.domain is not None:
        # treat empty string as NULL
        query = query.where(Crumb.domain == (params.domain or None))
    if params.app is not None:
        query = query.where(Crumb.app == params.app)
    if params.status is not None:
        query = query.where(Crumb.status == params.status)
    if params.start_date is not None:
        query = query.where(Crumb.date >= params.start_date.isoformat())
    if params.end_date is not None:
        query = query.where(Crumb.date <= params.end_date.isoformat())

    query = query.group_by(*columns)
    # need order_by if paginating

    async with AsyncSession() as session:
        # row is sqlalchemy.engine.row.Row
        return [row._asdict() for row in await session.execute(query)]


if __name__ == "__main__":
    pvapp.main()
