"""Data Orchestration Repository"""

from app.repository.baseapp_repository import BaseAppRepository
from app.model.data_orchestration_model import DataOrchestrationModel
from app.exception.baseapp_exception import InternalServerErrorException


class DataOrchestrationRepository(BaseAppRepository[DataOrchestrationModel]):
    """Repository for data orchestration operations."""

    def __init__(self, db):
        super().__init__(db, DataOrchestrationModel)

    async def insert(self, data: DataOrchestrationModel) -> DataOrchestrationModel:
        """
        Insert a new data orchestration record.
        
        Args:
            data: Dictionary containing model data
            
        Returns:
            Created model instance
            
        Raises:
            InternalServerErrorException: If insertion fails
        """
        try:
            record = self.model(**data)
            self.db.add(record)
            await self.db.flush()
            await self.db.refresh(record)
            return record
        except Exception as e:  # pylint: disable=broad-except
            raise InternalServerErrorException(
                message=f"Failed to insert data orchestration record: {str(e)}"
            ) from e
