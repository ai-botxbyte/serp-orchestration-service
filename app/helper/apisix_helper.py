"""APISIX Gateway Helper for route registration."""

import httpx
from app.config.apisix_config import get_apisix_config
from app.config.baseapp_config import get_base_config
from app.config.logger_config import logger


class APISIXHelper:
    """Helper class to manage APISIX Gateway route registration."""

    def __init__(self):
        """Initialize APISIX helper with configuration."""
        self.config = get_apisix_config()
        self.base_config = get_base_config()
        self.admin_url = (
            f"{self.config.APISIX_ADMIN_URL}/apisix/admin/routes/"
            f"{self.config.APISIX_ROUTE_NAME}"
        )
        self.headers = {
            "X-API-KEY": self.config.APISIX_ADMIN_API_KEY,
            "Content-Type": "application/json",
        }

    async def register_route(self) -> bool:
        """
        Register the service route with APISIX Gateway.

        Returns:
            bool: True if registration successful, False otherwise
        """
        if not self.config.IS_APISIX_ENABLED:
            logger.info("⚠️  APISIX integration is disabled (IS_APISIX_ENABLED=False)")
            return False

        # Build the route configuration
        route_config = {
            "name": self.config.APISIX_ROUTE_NAME,
            "uri": f"/{self.base_config.SERVICE_NAME}/*",
            "methods": ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
            "upstream": {
                "type": "roundrobin",
                "scheme": "http",
                "nodes": {
                    f"{self.base_config.SERVICE_NAME}:{self.base_config.APP_PORT}": 1
                },
            },
            "plugins": {
                "cors": {
                    "allow_origins": (
                        "http://localhost,http://localhost:80,http://localhost:3000,"
                        "http://localhost:8080,http://127.0.0.1,http://127.0.0.1:80,"
                        "http://127.0.0.1:3000,http://127.0.0.1:8080,"
                        "http://articleinnovator.com,http://www.articleinnovator.com,"
                        "https://articleinnovator.com,https://www.articleinnovator.com,"
                        "http://botxbyte.com,http://www.botxbyte.com,"
                        "https://botxbyte.com,https://www.botxbyte.com,"
                        "https://claude.ai,https://www.claudeusercontent.com"
                    ),
                    "allow_methods": "GET,POST,PUT,PATCH,DELETE,OPTIONS,HEAD",
                    "allow_headers": (
                        "Content-Type,Authorization,X-Correlation-ID,Accept,Origin,"
                        "X-Requested-With,X-API-KEY,Cache-Control,Pragma,Expires,DNT,"
                        "User-Agent,Keep-Alive,If-Modified-Since,X-CustomHeader,"
                        "workspace-id,user-id"
                    ),
                    "expose_headers": (
                        "Content-Length,Content-Type,X-Correlation-ID,Authorization"
                    ),
                    "allow_credential": True,
                    "max_age": 86400,
                },
                "rbac_abac_workspace": {"jwt_secret": self.config.APISIX_JWT_SECRET},
            },
            "status": 1,
        }

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.put(
                    self.admin_url, headers=self.headers, json=route_config
                )

                if response.status_code in [200, 201]:
                    logger.info(
                        f"✅ Successfully registered route '{self.config.APISIX_ROUTE_NAME}' "
                        f"with APISIX Gateway"
                    )
                    logger.info(f"   Route URI: /{self.base_config.SERVICE_NAME}/*")
                    logger.info(
                        f"   Upstream: {self.base_config.SERVICE_NAME}:{self.base_config.APP_PORT}"
                    )
                    logger.info("   Methods: GET, POST, PUT, PATCH, DELETE, OPTIONS")
                    return True

                logger.error(
                    f"❌ Failed to register APISIX route. "
                    f"Status: {response.status_code}, Response: {response.text}"
                )
                return False

        except httpx.ConnectError as e:
            logger.error(
                f"❌ Failed to connect to APISIX Admin API at {self.config.APISIX_ADMIN_URL}: {e}"
            )
            return False
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error(f"❌ Error registering APISIX route: {e}")
            return False

    async def check_route_exists(self) -> bool:
        """
        Check if the route already exists in APISIX.

        Returns:
            bool: True if route exists, False otherwise
        """
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(self.admin_url, headers=self.headers)
                return response.status_code == 200
        except Exception:  # pylint: disable=broad-exception-caught
            return False

    async def delete_route(self) -> bool:
        """
        Delete the service route from APISIX Gateway.

        Returns:
            bool: True if deletion successful, False otherwise
        """
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.delete(self.admin_url, headers=self.headers)

                if response.status_code in [200, 204]:
                    logger.info(
                        f"✅ Successfully deleted route '{self.config.APISIX_ROUTE_NAME}' "
                        f"from APISIX"
                    )
                    return True

                logger.error(
                    f"❌ Failed to delete APISIX route. "
                    f"Status: {response.status_code}, Response: {response.text}"
                )
                return False

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error(f"❌ Error deleting APISIX route: {e}")
            return False


# Global instance
_apisix_helper: APISIXHelper | None = None


def get_apisix_helper() -> APISIXHelper:
    """Get the APISIX helper instance."""
    global _apisix_helper  # pylint: disable=global-statement
    if _apisix_helper is None:
        _apisix_helper = APISIXHelper()
    return _apisix_helper
