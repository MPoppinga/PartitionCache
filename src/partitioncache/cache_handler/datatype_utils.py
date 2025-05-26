"""
Utility functions for datatype conversions.
These are helper functions for converting between Python types and datatype strings.
"""

from typing import Type, Dict
from datetime import datetime

# Python type to datatype string mapping
PYTHON_TYPE_TO_DATATYPE: Dict[Type, str] = {
    int: "integer",
    float: "float",
    str: "text",
    datetime: "timestamp",
}

# Datatype string to Python type mapping
DATATYPE_TO_PYTHON_TYPE: Dict[str, Type] = {
    "integer": int,
    "float": float,
    "text": str,
    "timestamp": datetime,
}


def get_datatype_from_settype(settype: Type) -> str:
    """
    Convert a Python type to a datatype string.
    
    Args:
        settype (Type): The Python type
        
    Returns:
        str: The corresponding datatype string
        
    Raises:
        ValueError: If the type is not supported
    """
    if settype not in PYTHON_TYPE_TO_DATATYPE:
        raise ValueError(f"Unsupported Python type: {settype}")
    
    return PYTHON_TYPE_TO_DATATYPE[settype]


def get_python_type_from_datatype(datatype: str) -> Type:
    """
    Convert a datatype string to a Python type.
    
    Args:
        datatype (str): The datatype string
        
    Returns:
        Type: The corresponding Python type
        
    Raises:
        ValueError: If the datatype is not supported
    """
    if datatype not in DATATYPE_TO_PYTHON_TYPE:
        raise ValueError(f"Unsupported datatype: {datatype}")
    
    return DATATYPE_TO_PYTHON_TYPE[datatype] 