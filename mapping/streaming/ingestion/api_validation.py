"""
API Data Validation Models

Validates the structure of incoming API data to ensure it follows
the canonical format required by the ingestion service.
"""

from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, field_validator


class TableData(BaseModel):
    """
    Model for a single table in the API response.
    
    Each table must have:
    - table_name: Name of the table
    - data: List of records (each record is a dict)
    """
    table_name: str = Field(..., description="Name of the table", min_length=1)
    data: List[Dict[str, Any]] = Field(..., description="List of data records")
    
    @field_validator('table_name')
    @classmethod
    def validate_table_name(cls, v: str) -> str:
        """
        Validate table name is not empty and follows naming conventions.
        
        Note: Table names are automatically converted to lowercase for consistency
        with the canonical schema table names used throughout the system.
        """
        if not v or not v.strip():
            raise ValueError("table_name cannot be empty")
        # Allow alphanumeric, underscores, and hyphens
        if not all(c.isalnum() or c in ('_', '-') for c in v):
            raise ValueError("table_name must contain only alphanumeric characters, underscores, or hyphens")
        return v.strip().lower()
    
    @field_validator('data')
    @classmethod
    def validate_data_not_none(cls, v: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate that data is not None.
        
        Note: Empty lists are allowed - use [] if there is no data.
        """
        if v is None:
            raise ValueError("data cannot be None, use empty list [] if no data")
        return v


class APIDataFormat(BaseModel):
    """
    Model for the expected API data format.
    
    The API must return data in this format:
    {
        "tables": [
            {
                "table_name": "users",
                "data": [{"id": 1, "name": "Alice"}]
            }
        ]
    }
    """
    tables: List[TableData] = Field(..., description="List of tables with their data")
    
    @field_validator('tables')
    @classmethod
    def validate_tables_present(cls, v: List[TableData]) -> List[TableData]:
        """Validate that at least one table is present."""
        if not v:
            raise ValueError("At least one table must be present in 'tables' array")
        return v


def validate_api_data(data: Dict[str, Any]) -> APIDataFormat:
    """
    Validate API data against the expected format.
    
    Args:
        data: Raw API response data
        
    Returns:
        Validated APIDataFormat object
        
    Raises:
        ValueError: If data doesn't match expected format
    """
    try:
        return APIDataFormat(**data)
    except Exception as e:
        raise ValueError(f"Invalid API data format: {str(e)}")


def get_expected_format_example() -> Dict[str, Any]:
    """
    Get an example of the expected API data format.
    
    Returns:
        Dictionary with example format
    """
    return {
        "tables": [
            {
                "table_name": "users",
                "data": [
                    {
                        "id": 1,
                        "name": "Alice",
                        "email": "alice@example.com"
                    },
                    {
                        "id": 2,
                        "name": "Bob",
                        "email": "bob@example.com"
                    }
                ]
            },
            {
                "table_name": "orders",
                "data": [
                    {
                        "order_id": 101,
                        "user_id": 1,
                        "amount": 250
                    },
                    {
                        "order_id": 102,
                        "user_id": 2,
                        "amount": 180
                    }
                ]
            }
        ]
    }
