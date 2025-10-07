from uuid import UUID
from datetime import datetime, timedelta, date
from typing import Optional, Dict, List, Any, Type, Generic, TypeVar
from math import ceil
from app.config.constants import DatabaseErrorMessages
from app.config.logger_config import configure_logging
from app.exception.baseapp_exception import InternalServerErrorException
from sqlalchemy import select, asc, desc, func, or_, and_, not_
from sqlalchemy.ext.asyncio import AsyncSession

# Configure logging
configure_logging()

# Generic type for the model
ModelType = TypeVar('ModelType')

# Frontend to Backend operator mapping
OPERATOR_MAPPING = {
    # Number operators
    "equal_to": "eq",
    "not_equal_to": "ne", 
    "greater_than": "gt",
    "less_than": "lt",
    "between": "between",
    "greater_than_or_equal": "gte",
    "less_than_or_equal": "lte",
    
    # Text operators
    "is": "is",
    "is_not": "is_not",
    "contains": "contains",
    "does_not_contain": "not_contains",
    "starts_with": "startswith",
    "ends_with": "endswith",
    "is_empty": "is_empty",
    "is_not_empty": "not_empty",
    
    # Boolean operators (same as text)
    # Enum operators (same as text)
    
    # Date operators (keep as is)
    "today": "today",
    "yesterday": "yesterday",
    "previous_day": "previous_day",
    "previous_7_days": "previous_7_days",
    "previous_30_days": "previous_30_days",
    "previous_month": "previous_month",
    "previous_3_months": "previous_3_months",
    "previous_12_months": "previous_12_months",
    "before": "before",
    "after": "after",
    "on": "on",
    "previous": "previous",
    "current": "current",
    "next": "next",
    
    # Time operators
    "this_hour": "this_hour",
    "last_hour": "last_hour",
    "last_3_hours": "last_3_hours",
    "morning": "morning",
    "afternoon": "afternoon",
    "evening": "evening",
    "night": "night"
}

def map_frontend_operator(operator: str) -> str:
    """Map frontend operator to backend operator"""
    return OPERATOR_MAPPING.get(operator, operator)


class BaseAppRepository(Generic[ModelType]):
    """Base repository class that can be reused by all repositories."""

    def __init__(self, db: AsyncSession, model: Type[ModelType]):
        self.db = db
        self.model = model
        # logger.info(f" {self.__class__.__name__} initialized with DB session: {id(db)}")  # Disabled to reduce log noise

    async def get_all(
        self,
        filters: Optional[List[Dict[str, Any]]] = None,
        search: Optional[str] = None,
        order_by: Optional[str] = None,
        skip: int = 0,
        limit: int = 20,
        user_id: Optional[UUID] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get all records with dynamic filters + search + ordering + pagination.
        """
        filters = filters or []
        query = select(self.model)
        count_query = select(func.count()).select_from(self.model)

        where_clauses = []
        count_clauses = []

        # --- Handle dynamic filters ---
        for f in filters:
            col_name = f.get("column_name")
            operator = map_frontend_operator(f.get("operator"))  # Map frontend operator to backend
            value = f.get("value")
            logical = f.get("logical", "and")
            case_sensitive = f.get("case_sensitive", False)
            columns = f.get("columns", [col_name]) if col_name == "search" else [col_name]

            clause = None
            for col in columns:
                if not hasattr(self.model, col):
                    continue
                column_attr = getattr(self.model, col)
                col_type = str(column_attr.type).lower()
                clause_part = None

                # --- Number filters ---
                if any(t in col_type for t in ["int", "numeric", "float", "decimal"]):
                    if operator == "eq":
                        clause_part = column_attr == value
                    elif operator == "ne":
                        clause_part = column_attr != value
                    elif operator == "gt":
                        clause_part = column_attr > value
                    elif operator == "gte":
                        clause_part = column_attr >= value
                    elif operator == "lt":
                        clause_part = column_attr < value
                    elif operator == "lte":
                        clause_part = column_attr <= value
                    elif operator == "between":
                        clause_part = column_attr.between(value[0], value[1])
                    elif operator == "is_empty":
                        clause_part = column_attr.is_(None)
                    elif operator == "not_empty":
                        clause_part = column_attr.is_not(None)

                # --- Text filters ---
                elif any(t in col_type for t in ["char", "text", "string"]):
                    cmp_func = column_attr if case_sensitive else func.lower(column_attr)
                    cmp_value = value if case_sensitive else str(value).lower()

                    if operator == "is":
                        clause_part = cmp_func == cmp_value
                    elif operator == "is_not":
                        clause_part = cmp_func != cmp_value
                    elif operator == "as_it":  # Case-insensitive equality
                        clause_part = func.lower(column_attr) == str(value).lower()
                    elif operator == "contains":
                        clause_part = cmp_func.ilike(f"%{cmp_value}%") if not case_sensitive else cmp_func.like(f"%{cmp_value}%")
                    elif operator == "not_contains":
                        clause_part = ~cmp_func.ilike(f"%{cmp_value}%") if not case_sensitive else ~cmp_func.like(f"%{cmp_value}%")
                    elif operator == "startswith":
                        clause_part = cmp_func.ilike(f"{cmp_value}%") if not case_sensitive else cmp_func.like(f"{cmp_value}%")
                    elif operator == "endswith":
                        clause_part = cmp_func.ilike(f"%{cmp_value}") if not case_sensitive else cmp_func.like(f"%{cmp_value}")
                    elif operator == "is_empty":
                        clause_part = column_attr.is_(None)
                    elif operator == "not_empty":
                        clause_part = column_attr.is_not(None)

                # --- Boolean filters ---
                elif "bool" in col_type:
                    if operator == "is":
                        clause_part = column_attr.is_(value)
                    elif operator == "is_not":
                        clause_part = column_attr.isnot(value)
                    elif operator == "is_empty":
                        clause_part = column_attr.is_(None)
                    elif operator == "not_empty":
                        clause_part = column_attr.is_not(None)

                # --- Enum filters ---
                elif "enum" in col_type:
                    if operator == "is":
                        clause_part = column_attr == value
                    elif operator == "is_not":
                        clause_part = column_attr != value
                    elif operator == "is_empty":
                        clause_part = column_attr.is_(None)
                    elif operator == "not_empty":
                        clause_part = column_attr.is_not(None)

                # --- Date / Time filters ---
                elif any(t in col_type for t in ["date", "time", "timestamp"]):
                    now = datetime.now()
                    today = date.today()

                    # Date operators
                    if operator == "today":
                        clause_part = func.date(column_attr) == today
                    elif operator == "yesterday":
                        clause_part = func.date(column_attr) == today - timedelta(days=1)
                    elif operator == "previous_day":
                        clause_part = func.date(column_attr) == today - timedelta(days=2)
                    elif operator == "prev_7_days" or operator == "previous_7_days":
                        clause_part = column_attr >= today - timedelta(days=7)
                    elif operator == "prev_30_days" or operator == "previous_30_days":
                        clause_part = column_attr >= today - timedelta(days=30)
                    elif operator == "previous_month":
                        # First day of previous month
                        first_day_prev_month = today.replace(day=1) - timedelta(days=1)
                        first_day_prev_month = first_day_prev_month.replace(day=1)
                        # Last day of previous month
                        last_day_prev_month = today.replace(day=1) - timedelta(days=1)
                        clause_part = func.date(column_attr).between(first_day_prev_month, last_day_prev_month)
                    elif operator == "previous_3_months":
                        clause_part = column_attr >= today - timedelta(days=90)
                    elif operator == "previous_12_months":
                        clause_part = column_attr >= today - timedelta(days=365)
                    elif operator == "between":
                        clause_part = column_attr.between(value[0], value[1])
                    elif operator == "before":
                        clause_part = column_attr < value
                    elif operator == "after":
                        clause_part = column_attr > value
                    elif operator == "on":
                        clause_part = func.date(column_attr) == value
                    
                    # Time operators
                    elif operator == "this_hour":
                        clause_part = func.date_trunc('hour', column_attr) == now.replace(minute=0, second=0, microsecond=0)
                    elif operator == "last_hour":
                        last_hour = now - timedelta(hours=1)
                        clause_part = column_attr.between(last_hour, now)
                    elif operator == "last_3_hours":
                        last_3_hours = now - timedelta(hours=3)
                        clause_part = column_attr.between(last_3_hours, now)
                    elif operator == "morning":
                        # 6 AM to 12 PM
                        morning_start = now.replace(hour=6, minute=0, second=0, microsecond=0)
                        morning_end = now.replace(hour=12, minute=0, second=0, microsecond=0)
                        clause_part = func.time(column_attr).between(morning_start.time(), morning_end.time())
                    elif operator == "afternoon":
                        # 12 PM to 5 PM
                        afternoon_start = now.replace(hour=12, minute=0, second=0, microsecond=0)
                        afternoon_end = now.replace(hour=17, minute=0, second=0, microsecond=0)
                        clause_part = func.time(column_attr).between(afternoon_start.time(), afternoon_end.time())
                    elif operator == "evening":
                        # 5 PM to 10 PM
                        evening_start = now.replace(hour=17, minute=0, second=0, microsecond=0)
                        evening_end = now.replace(hour=22, minute=0, second=0, microsecond=0)
                        clause_part = func.time(column_attr).between(evening_start.time(), evening_end.time())
                    elif operator == "night":
                        # 10 PM to 6 AM (next day)
                        night_start = now.replace(hour=22, minute=0, second=0, microsecond=0)
                        night_end = now.replace(hour=6, minute=0, second=0, microsecond=0)
                        clause_part = or_(
                            func.time(column_attr) >= night_start.time(),
                            func.time(column_attr) <= night_end.time()
                        )
                    
                    # Relative range operators (enhanced)
                    elif operator == "previous":
                        # Handle relative date range from value
                        if isinstance(value, dict) and "relativeDateRange" in value:
                            rel_range = value["relativeDateRange"]
                            period_type = rel_range.get("periodType", "day")
                            count = rel_range.get("count", 1)
                            
                            if period_type == "day":
                                clause_part = column_attr >= today - timedelta(days=count)
                            elif period_type == "week":
                                clause_part = column_attr >= today - timedelta(weeks=count)
                            elif period_type == "month":
                                clause_part = column_attr >= today - timedelta(days=count * 30)
                            elif period_type == "year":
                                clause_part = column_attr >= today - timedelta(days=count * 365)
                    elif operator == "current":
                        # Handle current period
                        if isinstance(value, dict) and "relativeDateRange" in value:
                            rel_range = value["relativeDateRange"]
                            period_type = rel_range.get("periodType", "day")
                            
                            if period_type == "day":
                                clause_part = func.date(column_attr) == today
                            elif period_type == "week":
                                # Current week (Monday to Sunday)
                                start_of_week = today - timedelta(days=today.weekday())
                                end_of_week = start_of_week + timedelta(days=6)
                                clause_part = func.date(column_attr).between(start_of_week, end_of_week)
                            elif period_type == "month":
                                # Current month
                                start_of_month = today.replace(day=1)
                                if today.month == 12:
                                    end_of_month = today.replace(year=today.year + 1, month=1, day=1) - timedelta(days=1)
                                else:
                                    end_of_month = today.replace(month=today.month + 1, day=1) - timedelta(days=1)
                                clause_part = func.date(column_attr).between(start_of_month, end_of_month)
                    elif operator == "next":
                        # Handle next period
                        if isinstance(value, dict) and "relativeDateRange" in value:
                            rel_range = value["relativeDateRange"]
                            period_type = rel_range.get("periodType", "day")
                            count = rel_range.get("count", 1)
                            
                            if period_type == "day":
                                clause_part = column_attr.between(today + timedelta(days=1), today + timedelta(days=count))
                            elif period_type == "week":
                                clause_part = column_attr.between(today + timedelta(days=1), today + timedelta(weeks=count))
                            elif period_type == "month":
                                clause_part = column_attr.between(today + timedelta(days=1), today + timedelta(days=count * 30))

                if clause_part is not None:
                    clause = clause_part if clause is None else or_(clause, clause_part)

            if clause is not None:
                if logical == "not":
                    where_clauses.append(not_(clause))
                    count_clauses.append(not_(clause))
                else:  # "and" and "or" cases are identical
                    where_clauses.append(clause)
                    count_clauses.append(clause)

        # --- Handle search ---
        if search:
            search_clauses = []
            # Find all string columns that can be searched
            for column_name in dir(self.model):
                if not column_name.startswith('_'):
                    column_attr = getattr(self.model, column_name)
                    if hasattr(column_attr, 'type'):
                        col_type = str(column_attr.type).lower()
                        if any(t in col_type for t in ["char", "text", "string"]):
                            search_clauses.append(column_attr.ilike(f"%{search}%"))
            
            if search_clauses:
                search_condition = or_(*search_clauses)
                where_clauses.append(search_condition)
                count_clauses.append(search_condition)

        # --- Exclude deleted by default if status exists ---
        if hasattr(self.model, 'status'):
            where_clauses.append(getattr(self.model, 'status') != "deleted")
            count_clauses.append(getattr(self.model, 'status') != "deleted")

        # --- Apply WHERE ---
        if where_clauses:
            query = query.where(and_(*where_clauses))
            count_query = count_query.where(and_(*count_clauses))

        # --- Handle ordering ---
        if order_by:
            desc_order = order_by.startswith("-")
            field_name = order_by[1:] if desc_order else order_by
            if hasattr(self.model, field_name):
                column_attr = getattr(self.model, field_name)
                query = query.order_by(desc(column_attr) if desc_order else asc(column_attr))
        else:
            if hasattr(self.model, 'created_at'):
                query = query.order_by(desc(getattr(self.model, 'created_at')))

        # --- Pagination ---
        query = query.offset(skip).limit(limit)

        # --- Execute queries ---
        try:
            total_count = (await self.db.execute(count_query)).scalar() or 0
            result = await self.db.execute(query)
            data = result.scalars().all()

            pagination = {
                "total_count": total_count,
                "offset": skip,
                "limit": limit,
                "total_pages": ceil(total_count / limit) if limit else 1,
            }

            return {"data": data, "pagination": pagination}
        except Exception as e:
            raise InternalServerErrorException(message=f"{DatabaseErrorMessages.DATA_RETRIEVAL_ERROR}: {str(e)}") from e
