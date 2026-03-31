"""Test script to insert random queries into serp_req_queue.

Usage:
    python test_serp_queue.py <number_of_queries>

Example:
    python test_serp_queue.py 100
    python test_serp_queue.py 250
"""

import asyncio
import sys
import os
import uuid
import random

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from loguru import logger
from app.helper.rabbitmq_helper import RabbitMQHelper
from app.config.baseapp_config import get_base_config

# Sample domains for generating random queries
SAMPLE_DOMAINS = [
    "google.com", "facebook.com", "twitter.com", "instagram.com", "linkedin.com",
    "apple.com", "microsoft.com", "amazon.com", "netflix.com", "youtube.com",
    "reddit.com", "wikipedia.org", "github.com", "stackoverflow.com", "medium.com",
    "quora.com", "nytimes.com", "forbes.com", "cnn.com", "bbc.com",
    "reuters.com", "techcrunch.com", "wired.com", "theguardian.com", "bloomberg.com",
    "wsj.com", "adobe.com", "salesforce.com", "shopify.com", "wordpress.org",
    "cloudflare.com", "zoom.us", "slack.com", "spotify.com", "twitch.tv",
    "pinterest.com", "tumblr.com", "flickr.com", "behance.net", "dribbble.com",
    "canva.com", "dropbox.com", "wetransfer.com", "hubspot.com", "mailchimp.com",
    "godaddy.com", "bluehost.com", "hostgator.com", "siteground.com", "digitalocean.com",
    "vultr.com", "linode.com", "heroku.com", "vercel.com", "netlify.com",
    "mongodb.com", "mysql.com", "postgresql.org", "oracle.com", "ibm.com",
    "intel.com", "nvidia.com", "amd.com", "tesla.com", "spacex.com",
    "nasa.gov", "un.org", "who.int", "cdc.gov", "nih.gov",
    "webmd.com", "mayoclinic.org", "healthline.com", "booking.com", "expedia.com",
    "tripadvisor.com", "airbnb.com", "uber.com", "lyft.com", "zomato.com",
    "yelp.com", "ebay.com", "walmart.com", "target.com", "bestbuy.com",
    "homedepot.com", "ikea.com", "nike.com", "adidas.com", "hm.com",
    "zara.com", "shutterstock.com", "gettyimages.com", "pixabay.com", "unsplash.com",
    "pexels.com", "stripe.com", "paypal.com", "squarespace.com", "wix.com",
]


def generate_query() -> dict:
    """Generate a random query with UUID."""
    domain = random.choice(SAMPLE_DOMAINS)
    return {
        "query": f"site:{domain}",
        "query_id": str(uuid.uuid4())
    }


async def publish_queries(num_queries: int) -> None:
    """
    Publish random queries to serp_req_queue.

    Args:
        num_queries: Number of queries to publish
    """
    logger.info(f"Publishing {num_queries} queries to serp_req_queue")

    config = get_base_config()
    rabbitmq_helper = RabbitMQHelper()

    try:
        # Ensure queue exists
        await rabbitmq_helper.ensure_queue_exists(
            queue_name="serp_req_queue",
            durable=True,
            auto_delete=False,
            arguments={"x-max-priority": 10}
        )

        success_count = 0
        failed_count = 0

        for i in range(num_queries):
            query = generate_query()

            published = await rabbitmq_helper.publish_message(
                queue_name="serp_req_queue",
                message=query,
                priority=5,
                ensure_queue=False
            )

            if published:
                success_count += 1
                if (i + 1) % 50 == 0 or (i + 1) == num_queries:
                    logger.info(f"Progress: {i + 1}/{num_queries} queries published")
            else:
                failed_count += 1
                logger.warning(f"Failed to publish query {i + 1}: {query['query_id']}")

        logger.info("=" * 60)
        logger.info(f"Publishing complete!")
        logger.info(f"Total: {num_queries}")
        logger.info(f"Success: {success_count}")
        logger.info(f"Failed: {failed_count}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error publishing queries: {e}")
        raise
    finally:
        await rabbitmq_helper.close()


def main():
    """Main entry point."""
    # Configure logger
    logger.remove()
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        level="INFO"
    )

    # Parse command line arguments
    if len(sys.argv) != 2:
        print("Usage: python test_serp_queue.py <number_of_queries>")
        print("Example: python test_serp_queue.py 100")
        sys.exit(1)

    try:
        num_queries = int(sys.argv[1])
        if num_queries <= 0:
            raise ValueError("Number of queries must be positive")
    except ValueError as e:
        print(f"Error: Invalid number of queries - {e}")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("SERP Queue Test Script")
    logger.info("=" * 60)

    # Run the async function
    asyncio.run(publish_queries(num_queries))


if __name__ == "__main__":
    main()
