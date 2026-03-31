"""Run SERP DLX Consumer as standalone service.

This worker consumes from serp_req_dlx_queue and republishes failed messages
back to serp_req_queue for retry. Implements infinite retry with exponential backoff.
"""

import asyncio
import sys
import os

# Add project root to Python path BEFORE importing app modules
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from loguru import logger
from app.consumer.serp_dlx_consumer import SerpDLXConsumer
from app.config.baseapp_config import get_base_config


async def main():
    """Main entry point for SERP DLX Consumer as standalone service."""
    # Configure loguru for production
    logger.remove()
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}",
        level="INFO"
    )

    logger.info("=" * 60)
    logger.info("SERP DLX Worker Starting")
    logger.info("=" * 60)
    logger.info("Queue: serp_req_dlx_queue -> serp_req_queue")
    logger.info("Mode: INFINITE RETRY")
    logger.info("=" * 60)

    config = get_base_config()

    # Get retry configuration from environment
    retry_delay = int(os.environ.get("SERP_DLX_RETRY_DELAY_SECONDS", "30"))
    max_retry_delay = int(os.environ.get("SERP_DLX_MAX_RETRY_DELAY_SECONDS", "300"))

    # Create consumer
    consumer = SerpDLXConsumer(
        config=config,
        retry_delay_seconds=retry_delay,
        max_retry_delay_seconds=max_retry_delay
    )

    try:
        # Connect and start consuming
        await consumer.connect()
        logger.info(f"Retry delay: {retry_delay}s, Max delay: {max_retry_delay}s")
        await consumer.start_consuming()

    except (ConnectionError, RuntimeError, asyncio.TimeoutError) as e:
        logger.error(f"SERP DLX consumer service error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("SERP DLX Consumer interrupted by user")
    finally:
        await consumer.disconnect()
        logger.info("SERP DLX Consumer service shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("SERP DLX Consumer interrupted by user")
        sys.exit(0)
    except (SystemExit, RuntimeError) as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
