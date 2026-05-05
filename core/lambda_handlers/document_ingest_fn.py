"""AWS Lambda entry point: document-ingestion-queue.fifo -> document_ingest_fn."""
import asyncio
import json
import logging

logger = logging.getLogger(__name__)


def _build_pipeline():
    from core.aggregation.service import AggregationService
    from core.credit.assembler import CreditAssembler
    from core.identity.xref_store import XRefStore
    from core.income.assembler import IncomeAssembler
    from core.pipelines.ingest_pipeline import IngestPipeline
    from core.storage.postgres_store import PostgresStore
    from core.storage.redis_store import RedisStore
    from core.storage.s3_client import S3Client

    xref_store = XRefStore()
    service = AggregationService(
        xref_store=xref_store,
        golden_record_store=xref_store,
        income_assembler=IncomeAssembler(),
        credit_assembler=CreditAssembler(),
        redis_store=RedisStore(),
        postgres_store=PostgresStore(),
    )
    return IngestPipeline(
        s3_client=S3Client(),
        postgres_store=PostgresStore(),
        aggregation_service=service,
    )


def lambda_handler(event, context):
    pipeline = _build_pipeline()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    results: list[dict] = []
    try:
        for record in event.get("Records", [event]):
            body = (
                json.loads(record["body"])
                if isinstance(record.get("body"), str)
                else record
            )
            result = loop.run_until_complete(pipeline.process(body))
            results.append(result)
    finally:
        loop.close()
    return {"statusCode": 200, "body": json.dumps({"processed": results})}
