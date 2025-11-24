"""Run Demo B Worker as standalone service"""

import asyncio
import signal
import sys
import os

# Add project root to Python path BEFORE importing app modules
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from loguru import logger
from app.worker.demo_B_worker import DemoBWorker


async def main():
    """Main entry point for DemoBWorker as standalone service"""
    # Configure loguru for production
    logger.remove()
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}",
        level="INFO"
    )
    
    # Create worker
    worker = DemoBWorker()
    
    # Setup signal handlers for graceful shutdown
    shutdown_event = asyncio.Event()
    
    def signal_handler(signum, _frame):
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        shutdown_event.set()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Connect and start consuming
        await worker.connect()
        consume_task = asyncio.create_task(worker.start_consuming())
        
        # Wait for shutdown signal
        shutdown_task = asyncio.create_task(shutdown_event.wait())
        
        # Wait for either completion or shutdown
        done, pending = await asyncio.wait(
            [consume_task, shutdown_task],
            return_when=asyncio.FIRST_COMPLETED
        )
        
        # Cancel pending tasks
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        # Check for errors
        if consume_task in done:
            try:
                consume_task.result()
            except Exception as e:
                logger.error(f"Demo B worker failed: {e}")
                raise
                
    except (ConnectionError, RuntimeError, asyncio.TimeoutError) as e:
        logger.error(f"Demo B worker service error: {e}")
        sys.exit(1)
    finally:
        await worker.disconnect()
        logger.info("Demo B Worker service shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Demo B Worker interrupted by user")
        sys.exit(0)
    except (SystemExit, RuntimeError) as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

