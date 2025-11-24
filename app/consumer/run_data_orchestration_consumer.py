import asyncio
import signal
import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from loguru import logger
from app.consumer.data_orchestration_consumer import DataOrchestrationConsumer


async def main():
    """Main entry point for DataOrchestrationConsumer as standalone service"""
    logger.remove()
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}",
        level="INFO"
    )
    
    consumer = DataOrchestrationConsumer()
    
    shutdown_event = asyncio.Event()
    
    def signal_handler(signum, _frame):
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        shutdown_event.set()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        await consumer.connect()
        consume_task = asyncio.create_task(consumer.start_consuming())
        shutdown_task = asyncio.create_task(shutdown_event.wait())
        
        done, pending = await asyncio.wait(
            [consume_task, shutdown_task],
            return_when=asyncio.FIRST_COMPLETED
        )
        
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        if consume_task in done:
            try:
                consume_task.result()
            except Exception as e:
                logger.error(f"Data orchestration consumer failed: {e}")
                raise
                
    except (ConnectionError, RuntimeError, asyncio.TimeoutError) as e:
        logger.error(f"Data orchestration consumer service error: {e}")
        sys.exit(1)
    finally:
        await consumer.disconnect()
        logger.info("Data Orchestration Consumer service shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Data Orchestration Consumer interrupted by user")
        sys.exit(0)
    except (SystemExit, RuntimeError) as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

