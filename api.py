from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter

from shared.consts import SEGMENT_ALLOCATION_WORKFLOW_QUEUE
from shared.models.segment_allocation import SegmentAllocationInput, SegmentAllocationResult
from shared.settings import TemporalSettings
from workflows.segment_allocation import SegmentAllocationWorkflow

_settings = TemporalSettings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.temporal_client = await Client.connect(
        _settings.temporal_host,
        namespace=_settings.temporal_namespace,
        data_converter=pydantic_data_converter,
    )
    yield


app = FastAPI(title="Redbull Workflows Orchestrator API", lifespan=lifespan)


@app.post("/workflows/segment-allocation", response_model=SegmentAllocationResult)
async def allocate_segment(allocation_input: SegmentAllocationInput) -> SegmentAllocationResult:
    client: Client = app.state.temporal_client
    return await client.execute_workflow(
        SegmentAllocationWorkflow.run,
        allocation_input,
        id=f"segment-allocation-{allocation_input.site}-{allocation_input.cluster_name}",
        task_queue=SEGMENT_ALLOCATION_WORKFLOW_QUEUE,
    )
