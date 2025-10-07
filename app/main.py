from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from app.config.config import config
# from app.config.logger_config import configure_logging
from app.api.v1.router import api_router
from app.middleware.correlation import CorrelationIdMiddleware
from app.exception.fastapi.error_handlers import setup_error_handlers


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    # configure_logging()

    app = FastAPI(
        title=config.APP_NAME,
        version=config.APP_VERSION,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        default_response_class=JSONResponse,
    )

    # Add middleware
    app.add_middleware(CorrelationIdMiddleware)
    
    # Setup custom error handlers
    setup_error_handlers(app)

    # Include API routes
    app.include_router(router=api_router, prefix="/api/v1")
    
    # Mount static files for media

    # app.mount("/media", StaticFiles(directory="app/media"), name="media")

    return app