from fastapi import APIRouter

router = APIRouter()


@router.get("/health/", summary="Health check")
async def health_check():
    """
    Health check for the Demo Orchestration Service
    """
    return {"success": True, "data": {"status": "ok"}, "message": "Demo Orchestration Service healthy"} 