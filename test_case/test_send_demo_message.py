import asyncio
import json
import sys
import os
from datetime import datetime
from uuid import uuid4
from loguru import logger

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

import aio_pika
from app.config.baseapp_config import get_base_config


async def send_simple_demo_message():
    """Send simple test message to the demo_queue"""

    # Configure loguru
    logger.remove()
    logger.add(sys.stdout, format="{time:HH:mm:ss} | {level} | {message}", level="INFO")

    config = get_base_config()

    try:
        # Connect to RabbitMQ
        logger.info("Connecting to RabbitMQ...")
        connection = await aio_pika.connect_robust(config.RABBITMQ_URL)
        channel = await connection.channel()

        # Declare the demo queue with priority support (ensure it exists)
        queue = await channel.declare_queue(
            "demo_queue", 
            durable=True,
            arguments={
                "x-max-priority": 10  # Support priority levels 0-10
            }
        )

        logger.info("Connected to RabbitMQ successfully!")

        # Simple test message - just data field with any message
        simple_message = {
            "data": {
                "message": "Hello from demo queue test!",
                "test_id": str(uuid4()),
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
        }

        logger.info("Sending simple demo message...")
        
        await channel.default_exchange.publish(
            aio_pika.Message(
                json.dumps(simple_message).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                priority=5
            ),
            routing_key="demo_queue"
        )

        logger.info("✓ Simple demo message sent successfully!")
        logger.info(f"   Message: {simple_message['data']['message']}")
        logger.info(f"   Test ID: {simple_message['data']['test_id']}")

        await connection.close()
        logger.info("Connection closed.")

    except (ConnectionError, RuntimeError, asyncio.TimeoutError) as e:
        logger.error(f"Error sending message: {e}")
        raise


async def send_demo_message():
    """Send test messages to the demo_queue"""

    # Configure loguru
    logger.remove()
    logger.add(sys.stdout, format="{time:HH:mm:ss} | {level} | {message}", level="INFO")

    config = get_base_config()

    try:
        # Connect to RabbitMQ
        logger.info("Connecting to RabbitMQ...")
        connection = await aio_pika.connect_robust(config.RABBITMQ_URL)
        channel = await connection.channel()

        # Declare the demo queue with priority support (ensure it exists)
        queue = await channel.declare_queue(
            "demo_queue", 
            durable=True,
            arguments={
                "x-max-priority": 10  # Support priority levels 0-10
            }
        )

        logger.info("Connected to RabbitMQ successfully!")

        # Test Message 1: Simple demo message
        demo_message_1 = {
            "data": {
                "service_name": "demo-service-1",
                "event_type": "user_action",
                "user_id": str(uuid4()),
                "action": "button_click",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "metadata": {
                    "page": "/dashboard",
                    "element": "submit_button",
                    "session_id": str(uuid4())
                }
            }
        }

        # Test Message 2: System event message
        demo_message_2 = {
            "data": {
                "service_name": "demo-service-2",
                "event_type": "system_event",
                "event_name": "database_backup",
                "status": "completed",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "metadata": {
                    "backup_size": "2.5GB",
                    "duration": "15 minutes",
                    "location": "/backups/db_backup_20241201.sql"
                }
            }
        }

        # Test Message 3: Error event message
        demo_message_3 = {
            "data": {
                "service_name": "demo-service-3",
                "event_type": "error_event",
                "error_code": "E001",
                "error_message": "Connection timeout to external API",
                "severity": "warning",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "metadata": {
                    "api_endpoint": "https://api.example.com/data",
                    "retry_count": 3,
                    "user_affected": str(uuid4())
                }
            }
        }

        # Send messages
        messages = [
            ("Demo Message 1 - User Action", demo_message_1, 5),
            ("Demo Message 2 - System Event", demo_message_2, 3),
            ("Demo Message 3 - Error Event", demo_message_3, 8)
        ]

        for message_type, message_data, priority in messages:
            logger.info(f"Sending {message_type}...")
            
            await channel.default_exchange.publish(
                aio_pika.Message(
                    json.dumps(message_data).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    priority=priority
                ),
                routing_key="demo_queue"
            )

            logger.info(f"✓ {message_type} sent successfully!")
            logger.info(f"   Priority: {priority}")
            logger.info(f"   Service: {message_data['data']['service_name']}")
            logger.info(f"   Event Type: {message_data['data']['event_type']}")
            logger.info("")

        await connection.close()
        logger.info("Connection closed.")

    except (ConnectionError, RuntimeError, asyncio.TimeoutError) as e:
        logger.error(f"Error sending message: {e}")
        raise


async def send_multiple_demo_messages(count: int = 10):
    """Send multiple demo messages with different event types"""

    config = get_base_config()

    try:
        connection = await aio_pika.connect_robust(config.RABBITMQ_URL)
        channel = await connection.channel()
        queue = await channel.declare_queue(
            "demo_queue", 
            durable=True,
            arguments={
                "x-max-priority": 10  # Support priority levels 0-10
            }
        )

        logger.info(f"Sending {count} demo messages...")

        event_types = ["user_action", "system_event", "error_event", "analytics"]
        
        for i in range(count):
            event_type = event_types[i % len(event_types)]
            priority = (i % 10) + 1  # Priority 1-10
            
            message = {
                "data": {
                    "service_name": f"demo-service-{i % 3 + 1}",
                    "event_type": event_type,
                    "event_id": str(uuid4()),
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "message": f"Test {event_type} message {i + 1}",
                    "metadata": {
                        "sequence": i + 1,
                        "batch_id": str(uuid4()),
                        "test_run": True
                    }
                }
            }

            await channel.default_exchange.publish(
                aio_pika.Message(
                    json.dumps(message).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    priority=priority
                ),
                routing_key="demo_queue"
            )

            logger.info(f"✓ Sent {event_type} message {i + 1} (priority: {priority})")

        await connection.close()
        logger.info(f"All {count} demo messages sent successfully!")

    except (ConnectionError, RuntimeError, asyncio.TimeoutError) as e:
        logger.error(f"Error sending messages: {e}")
        raise


async def send_custom_demo_message():
    """Send a custom demo message with user input"""

    config = get_base_config()

    try:
        connection = await aio_pika.connect_robust(config.RABBITMQ_URL)
        channel = await connection.channel()
        queue = await channel.declare_queue(
            "demo_queue", 
            durable=True,
            arguments={
                "x-max-priority": 10
            }
        )

        # Get user input
        service_name = input("Enter service name (default: custom-service): ").strip() or "custom-service"
        event_type = input("Enter event type (default: custom_event): ").strip() or "custom_event"
        message_text = input("Enter message text: ").strip()
        priority = int(input("Enter priority (1-10, default: 5): ").strip() or "5")

        if not message_text:
            logger.error("Message text cannot be empty!")
            return

        custom_message = {
            "data": {
                "service_name": service_name,
                "event_type": event_type,
                "message": message_text,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "metadata": {
                    "custom": True,
                    "user_input": True,
                    "created_by": "test_script"
                }
            }
        }

        await channel.default_exchange.publish(
            aio_pika.Message(
                json.dumps(custom_message).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                priority=priority
            ),
            routing_key="demo_queue"
        )

        logger.info("✓ Custom demo message sent successfully!")
        logger.info(f"   Service: {service_name}")
        logger.info(f"   Event Type: {event_type}")
        logger.info(f"   Message: {message_text}")
        logger.info(f"   Priority: {priority}")

        await connection.close()

    except (ConnectionError, RuntimeError, asyncio.TimeoutError, ValueError) as e:
        logger.error(f"Error sending custom message: {e}")
        raise


async def main():
    """Main function"""
    logger.info("=" * 50)
    logger.info("Demo Queue Test Script")
    logger.info("=" * 50)
    
    while True:
        print("\nChoose an option:")
        print("1. Send simple demo message (just data field)")
        print("2. Exit")
        
        try:
            choice = input("\nEnter your choice (1-2): ").strip()
            
            if choice == "1":
                await send_simple_demo_message()
            elif choice == "2":
                logger.info("Exiting...")
                break
            else:
                logger.warning("Invalid choice. Please enter 1-2.")
                
        except KeyboardInterrupt:
            logger.info("\nTest cancelled by user")
            break
        except (ConnectionError, RuntimeError, asyncio.TimeoutError, ValueError) as e:
            logger.error(f"Error: {e}")
    
    logger.info("Test completed!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
    except (SystemExit, RuntimeError) as e:
        logger.error(f"Test failed: {e}")
        sys.exit(1)
