from uuid import UUID
from datetime import datetime, timedelta, date
from typing import Optional, Dict, List, Any, Type, Generic, TypeVar
from math import ceil
from sqlalchemy import select, asc, desc, func, or_, and_, not_, literal_column
from sqlalchemy.ext.asyncio import AsyncSession
from app.config.logger_config import configure_logging
from app.config.constants import DatabaseErrorMessages
from app.exception.baseapp_exception import InternalServerErrorException

# Configure logging
configure_logging()

# Generic type for the model
T = TypeVar("T")

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
    "previous_1_month": "previous_1_month",
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
    "night": "night",
}


def map_frontend_operator(operator: str) -> str:
    """Map frontend operator to backend operator"""
    return OPERATOR_MAPPING.get(operator, operator)


def parse_date_value(value: Any) -> Optional[datetime]:
    """
    Parse various date formats to datetime object.
    Handles: datetime objects, date objects, ISO strings, timestamps
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())

    dt = None
    if isinstance(value, str):
        try:
            # Try ISO format first
            # Be flexible with separator between date and time
            value = value.replace(":", "T", 1) if value.count(":") > 1 else value
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            for fmt in ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%d-%m-%Y"]:
                try:
                    dt = datetime.strptime(value, fmt)
                    break  # Found a format
                except (ValueError, TypeError):
                    continue

    elif isinstance(value, (int, float)):
        try:
            # Assume timestamp
            dt = datetime.fromtimestamp(value)
        except (ValueError, TypeError, OSError):
            pass  # dt remains None

    return dt


class BaseAppRepository(Generic[T]):
    """Base repository class that can be reused by all repositories."""

    def __init__(self, db: AsyncSession, model: Type[T]):
        self.db = db
        self.model = model

    def _get_primary_key_col(self):
        for col_name in dir(self.model):
            if not col_name.startswith("_"):
                col_attr = getattr(self.model, col_name)
                if hasattr(col_attr, "primary_key") and col_attr.primary_key:
                    return col_attr
        return None

    def _get_number_filter(self, column_attr, operator, value, value2):
        operator_map = {
            "eq": lambda: column_attr == value,
            "ne": lambda: column_attr != value,
            "gt": lambda: column_attr > value,
            "gte": lambda: column_attr >= value,
            "lt": lambda: column_attr < value,
            "lte": lambda: column_attr <= value,
            "between": lambda: column_attr.between(value, value2),
            "is_empty": lambda: column_attr.is_(None),
            "not_empty": lambda: column_attr.is_not(None),
        }
        return operator_map.get(operator, lambda: None)()

    def _get_text_filter(self, column_attr, operator, value, case_sensitive):
        cmp_func = column_attr if case_sensitive else func.lower(column_attr)
        cmp_value = value if case_sensitive else str(value).lower()

        operator_map = {
            "is": lambda: cmp_func == cmp_value,
            "is_not": lambda: cmp_func != cmp_value,
            "as_it": lambda: func.lower(column_attr) == cmp_value,
            "contains": lambda: (
                column_attr.ilike(f"%{value}%")
                if not case_sensitive
                else column_attr.like(f"%{value}%")
            ),
            "not_contains": lambda: (
                ~column_attr.ilike(f"%{value}%")
                if not case_sensitive
                else ~column_attr.like(f"%{value}%")
            ),
            "startswith": lambda: (
                column_attr.ilike(f"{value}%")
                if not case_sensitive
                else column_attr.like(f"{value}%")
            ),
            "endswith": lambda: (
                column_attr.ilike(f"%{value}")
                if not case_sensitive
                else column_attr.like(f"%{value}")
            ),
            "is_empty": lambda: column_attr.is_(None),
            "not_empty": lambda: column_attr.is_not(None),
        }
        return operator_map.get(operator, lambda: None)()

    def _get_boolean_filter(self, column_attr, operator, value):
        bool_value = None
        if isinstance(value, str):
            if value.lower() == "true":
                bool_value = True
            elif value.lower() == "false":
                bool_value = False
        elif isinstance(value, bool):
            bool_value = value

        operator_map = {
            "is": lambda: column_attr.is_(bool_value),
            "is_not": lambda: column_attr.isnot(bool_value),
            "is_empty": lambda: column_attr.is_(None),
            "not_empty": lambda: column_attr.is_not(None),
        }
        return operator_map.get(operator, lambda: None)()

    def _get_enum_filter(self, column_attr, operator, value):
        operator_map = {
            "is": lambda: column_attr == value,
            "is_not": lambda: column_attr != value,
            "is_empty": lambda: column_attr.is_(None),
            "not_empty": lambda: column_attr.is_not(None),
        }
        return operator_map.get(operator, lambda: None)()

    def _get_date_operator_filter(self, column_attr, operator):
        today = date.today()
        operator_map = {
            "today": lambda: func.date(column_attr) == today,
            "yesterday": lambda: func.date(column_attr) == today - timedelta(days=1),
            "previous_day": lambda: func.date(column_attr) == today - timedelta(days=2),
            "previous_7_days": lambda: column_attr >= today - timedelta(days=7),
            "prev_7_days": lambda: column_attr >= today - timedelta(days=7),
            "previous_30_days": lambda: column_attr >= today - timedelta(days=30),
            "prev_30_days": lambda: column_attr >= today - timedelta(days=30),
            "previous_1_month": lambda: func.date(column_attr).between(
                (today.replace(day=1) - timedelta(days=1)).replace(day=1),
                today.replace(day=1) - timedelta(days=1),
            ),
            "previous_3_months": lambda: column_attr >= today - timedelta(days=90),
            "previous_12_months": lambda: column_attr >= today - timedelta(days=365),
        }
        return operator_map.get(operator, lambda: None)()

    def _get_time_operator_filter(self, column_attr, operator):
        now = datetime.now()
        operator_map = {
            "this_hour": lambda: (
                func.date_trunc("hour", column_attr)
                == now.replace(minute=0, second=0, microsecond=0)
            ),
            "last_hour": lambda: (column_attr.between(now - timedelta(hours=1), now)),
            "last_3_hours": lambda: (
                column_attr.between(now - timedelta(hours=3), now)
            ),
            "morning": lambda: (
                func.time(column_attr).between(
                    datetime.min.replace(hour=6).time(),
                    datetime.min.replace(hour=12).time(),
                )
            ),
            "afternoon": lambda: (
                func.time(column_attr).between(
                    datetime.min.replace(hour=12).time(),
                    datetime.min.replace(hour=17).time(),
                )
            ),
            "evening": lambda: (
                func.time(column_attr).between(
                    datetime.min.replace(hour=17).time(),
                    datetime.min.replace(hour=22).time(),
                )
            ),
            "night": lambda: or_(
                func.time(column_attr) >= datetime.min.replace(hour=22).time(),
                func.time(column_attr) <= datetime.min.replace(hour=6).time(),
            ),
        }
        return operator_map.get(operator, lambda: None)()

    def _get_date_value_filter(self, column_attr, operator, value, value2, col_type):
        parsed_value = parse_date_value(value)
        if not parsed_value:
            return None

        operator_map = {
            "between": lambda: (
                column_attr.between(parse_date_value(value), parse_date_value(value2))
                if "timestamp" in col_type
                else column_attr.between(
                    parse_date_value(value).date(), parse_date_value(value2).date()
                )
            ),
            "before": lambda: column_attr < parsed_value,
            "after": lambda: column_attr > parsed_value,
            "on": lambda: func.date(column_attr) == parsed_value.date(),
        }
        return operator_map.get(operator, lambda: None)()

    def _get_previous_date_filter(self, column_attr, days_to_subtract, include_today):
        today = date.today()
        if include_today:
            start_date = today - timedelta(days=days_to_subtract - 1)
            end_date = today + timedelta(days=1)
        else:
            start_date = today - timedelta(days=days_to_subtract)
            end_date = today
        return and_(
            column_attr >= datetime.combine(start_date, datetime.min.time()),
            column_attr < datetime.combine(end_date, datetime.min.time()),
        )

    def _get_current_date_filter(self, column_attr, period_type):
        today = date.today()
        if period_type == "day":
            return func.date(column_attr) == today
        if period_type == "week":
            start_of_week = today - timedelta(days=today.weekday())
            end_of_week = start_of_week + timedelta(days=6)
            return func.date(column_attr).between(start_of_week, end_of_week)
        if period_type == "month":
            start_of_month = today.replace(day=1)
            if start_of_month.month == 12:
                end_of_month = start_of_month.replace(
                    year=start_of_month.year + 1, month=1, day=1
                ) - timedelta(days=1)
            else:
                end_of_month = start_of_month.replace(
                    month=start_of_month.month % 12 + 1
                ) - timedelta(days=1)
            return func.date(column_attr).between(start_of_month, end_of_month)
        return None

    def _get_next_date_filter(self, column_attr, days_to_add, include_today):
        today = date.today()
        if include_today:
            start_date = today
            end_date = today + timedelta(days=days_to_add)
        else:
            start_date = today + timedelta(days=1)
            end_date = today + timedelta(days=days_to_add + 1)
        return and_(
            column_attr >= datetime.combine(start_date, datetime.min.time()),
            column_attr < datetime.combine(end_date, datetime.min.time()),
        )

    def _get_relative_date_filter(self, column_attr, operator, f):
        rel_range = f.get("relative_date_range") or f.get("relativeDateRange")
        if not rel_range:
            return None

        period_type = rel_range.get("period_type") or rel_range.get("periodType", "day")
        period_count = rel_range.get("count", 1)
        include_today = rel_range.get("include_today") or rel_range.get(
            "includeToday", False
        )

        days = 0
        if period_type == "day":
            days = period_count
        elif period_type == "week":
            days = period_count * 7
        elif period_type == "month":
            days = period_count * 30
        elif period_type == "year":
            days = period_count * 365

        if operator == "previous" and days > 0:
            return self._get_previous_date_filter(column_attr, days, include_today)

        if operator == "current":
            return self._get_current_date_filter(column_attr, period_type)

        if operator == "next":
            return self._get_next_date_filter(column_attr, days, include_today)

        return None

    def _get_date_filter(self, column_attr, operator, value, value2, f, col_type):
        date_operator_clause = self._get_date_operator_filter(column_attr, operator)
        if date_operator_clause is not None:
            return date_operator_clause

        time_operator_clause = self._get_time_operator_filter(column_attr, operator)
        if time_operator_clause is not None:
            return time_operator_clause

        date_value_clause = self._get_date_value_filter(
            column_attr, operator, value, value2, col_type
        )
        if date_value_clause is not None:
            return date_value_clause

        relative_date_clause = self._get_relative_date_filter(column_attr, operator, f)
        if relative_date_clause is not None:
            return relative_date_clause

        return None

    def _get_clause_for_column(self, col, operator, value, value2, f, case_sensitive):
        if col is None or not hasattr(self.model, col):
            return None
        column_attr = getattr(self.model, col)
        col_type = str(column_attr.type).lower()
        return self._get_filter_clause(
            column_attr, operator, value, value2, f, col_type, case_sensitive
        )

    def _get_filter_clause(
        self, column_attr, operator, value, value2, f, col_type, case_sensitive
    ):
        col_type_str = str(col_type).lower()

        if any(t in col_type_str for t in ["date", "time", "timestamp"]):
            return self._get_date_filter(
                column_attr, operator, value, value2, f, col_type
            )

        if any(t in col_type_str for t in ["char", "text", "string"]):
            return self._get_text_filter(column_attr, operator, value, case_sensitive)

        if any(t in col_type_str for t in ["int", "numeric", "float", "decimal"]):
            return self._get_number_filter(column_attr, operator, value, value2)

        if "bool" in col_type_str:
            return self._get_boolean_filter(column_attr, operator, value)

        if "enum" in col_type_str:
            return self._get_enum_filter(column_attr, operator, value)

        return None

    def _build_filter_clause(self, f):
        col_name = f.get("column")
        operator = map_frontend_operator(f.get("operator"))
        value = f.get("value")
        case_sensitive = f.get("caseSensitive", False)
        columns = f.get("columns", [col_name]) if col_name == "search" else [col_name]
        value2 = f.get("value2")

        clauses = [
            self._get_clause_for_column(c, operator, value, value2, f, case_sensitive)
            for c in columns
        ]
        clauses = [c for c in clauses if c is not None]

        if not clauses:
            return None

        logical = f.get("logical", "and")
        return or_(*clauses) if logical == "or" else and_(*clauses)

    def _get_filter_rules_and_logic(self, filters):
        if not filters:
            return [], "AND"
        if isinstance(filters, list):
            return filters, "AND"
        if isinstance(filters, dict):
            return filters.get("Filters", []), filters.get("logic", "AND")
        return [], "AND"

    def _build_filter_group(self, filters):
        filter_rules, overall_logic = self._get_filter_rules_and_logic(filters)
        if not filter_rules:
            return None

        where_clauses = []
        for f in filter_rules:
            clause = self._build_filter_clause(f)
            if clause is not None:
                if f.get("logical") == "not":
                    where_clauses.append(not_(clause))
                else:
                    where_clauses.append(clause)

        if not where_clauses:
            return None

        logic_func = or_ if overall_logic.upper() in ("ANY", "OR") else and_
        return logic_func(*where_clauses)

    def _build_search_group(self, search):
        if not search:
            return None

        search_clauses = []
        for col_name in dir(self.model):
            if col_name.startswith("_"):
                continue

            column_attr = getattr(self.model, col_name)
            if not hasattr(column_attr, "type"):
                continue

            col_type = str(column_attr.type).lower()
            if any(t in col_type for t in ["char", "text", "string"]):
                search_clauses.append(column_attr.ilike(f"%{search}%"))

        return or_(*search_clauses) if search_clauses else None

    def _apply_filters_and_search(
        self, query, count_query, filters, search, user_id, workspace_id
    ):
        """Apply filters and search to the query."""
        clauses = []
        if workspace_id and hasattr(self.model, "workspace_id"):
            clauses.append(getattr(self.model, "workspace_id") == workspace_id)
        if user_id and hasattr(self.model, "created_by"):
            clauses.append(getattr(self.model, "created_by") == user_id)

        filter_group = self._build_filter_group(filters)
        if filter_group is not None:
            clauses.append(filter_group)

        search_group = self._build_search_group(search)
        if search_group is not None:
            clauses.append(search_group)

        if hasattr(self.model, "status"):
            clauses.append(getattr(self.model, "status") != "deleted")

        if clauses:
            final_clause = and_(*clauses)
            query = query.where(final_clause)
            count_query = count_query.where(final_clause)

        return query, count_query

    def _apply_ordering(self, query, order_by):
        if order_by:
            desc_order = order_by.startswith("-")
            field_name = order_by[1:] if desc_order else order_by
            if hasattr(self.model, field_name):
                column_attr = getattr(self.model, field_name)
                query = query.order_by(
                    desc(column_attr) if desc_order else asc(column_attr)
                )
        elif hasattr(self.model, "created_at"):
            query = query.order_by(desc(getattr(self.model, "created_at")))
        return query

    async def _execute_query(self, query, count_query, skip, limit):
        try:
            total_count_result = await self.db.execute(count_query)
            total_count = total_count_result.scalar() or 0

            query = query.offset(skip).limit(limit)
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
            raise InternalServerErrorException(
                message=f"{DatabaseErrorMessages.DATA_RETRIEVAL_ERROR}: {str(e)}"
            ) from e

    async def get_all(
        self,
        filters: Optional[List[Dict[str, Any]]] = None,
        search: Optional[str] = None,
        order_by: Optional[str] = None,
        skip: int = 0,
        limit: int = 20,
        user_id: Optional[UUID] = None,
        workspace_id: Optional[UUID] = None,
    ) -> Optional[Dict[str, Any]]:
        """This is a helper function to get all the records"""
        query = select(self.model)
        primary_key_col = self._get_primary_key_col()

        if primary_key_col is not None:
            count_query = select(func.count())  # pylint: disable=not-callable
        else:
            count_query = select(literal_column("1"))

        query, count_query = self._apply_filters_and_search(
            query, count_query, filters, search, user_id, workspace_id
        )
        query = self._apply_ordering(query, order_by)

        return await self._execute_query(query, count_query, skip, limit)
