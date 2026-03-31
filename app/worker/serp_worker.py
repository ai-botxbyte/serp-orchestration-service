"""Run SERP Consumer as standalone service.

This worker consumes from serp_req_queue, calls the SERP lambda service,
and routes responses to serp_response_queue (success) or serp_req_dlx_queue (failure).
"""

import asyncio
import sys
import os
import uuid

# Add project root to Python path BEFORE importing app modules
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from loguru import logger
from app.consumer.serp_consumer import SerpConsumer
from app.config.baseapp_config import get_base_config


async def main():
    """Main entry point for SERP Consumer as standalone service."""
    # Generate unique worker ID
    worker_id = os.environ.get("WORKER_ID", str(uuid.uuid4())[:8])

    # Configure loguru for production with worker ID
    logger.remove()
    logger.add(
        sys.stdout,
        format=f"{{time:YYYY-MM-DD HH:mm:ss}} | {{level}} | [Worker-{worker_id}] {{message}}",
        level="INFO"
    )

    logger.info("=" * 60)
    logger.info(f"SERP Worker Starting - ID: {worker_id}")
    logger.info("=" * 60)
    logger.info("Queue: serp_req_queue -> serp_response_queue / serp_req_dlx_queue")
    logger.info("=" * 60)

    config = get_base_config()

    # Get SERP lambda URL from environment or config
    serp_lambda_url = os.environ.get(
        "SERP_LAMBDA_SERVICE_URL",
        getattr(config, 'SERP_LAMBDA_SERVICE_URL', 'http://localhost:8000')
    )

    # Create consumer with worker ID
    consumer = SerpConsumer(
        config=config,
        serp_lambda_url=serp_lambda_url,
        worker_id=worker_id
    )

    try:
        # Connect and start consuming
        await consumer.connect()
        logger.info(f"SERP Lambda Service URL: {serp_lambda_url}")
        await consumer.start_consuming()

    except (ConnectionError, RuntimeError, asyncio.TimeoutError) as e:
        logger.error(f"SERP consumer service error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("SERP Consumer interrupted by user")
    finally:
        await consumer.disconnect()
        logger.info("SERP Consumer service shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("SERP Consumer interrupted by user")
        sys.exit(0)
    except (SystemExit, RuntimeError) as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
