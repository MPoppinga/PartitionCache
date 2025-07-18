"""
Common CLI argument definitions and utilities for PartitionCache CLI tools.

This module provides reusable argument groups and utilities to ensure
consistency across all CLI tools and reduce code duplication.
"""

import argparse
import json
import os
import sys
from logging import getLogger
from pathlib import Path
from typing import Any

import dotenv

logger = getLogger("PartitionCache")


def add_database_args(parser: argparse.ArgumentParser, include_sqlite: bool = True) -> None:
    """
    Add common database connection arguments to an ArgumentParser.

    Args:
        parser: The ArgumentParser to add arguments to
        include_sqlite: Whether to include SQLite in backend choices
    """
    db_group = parser.add_argument_group("database connection")

    # Database backend selection
    backend_choices = ["postgresql", "mysql"]
    if include_sqlite:
        backend_choices.append("sqlite")

    db_group.add_argument("--db-backend", type=str, default="postgresql", choices=backend_choices, help="Database backend to use (default: postgresql)")

    # Database name/path
    db_group.add_argument("--db-name", type=str, help="Database name (if not specified, uses DB_NAME environment variable)")

    if include_sqlite:
        db_group.add_argument("--db-dir", type=str, default="data/test_db.sqlite", help="Database directory/path for SQLite (default: data/test_db.sqlite)")


def add_cache_args(parser: argparse.ArgumentParser, require_partition_key: bool = False) -> None:
    """
    Add common cache-related arguments to an ArgumentParser.

    Args:
        parser: The ArgumentParser to add arguments to
        require_partition_key: Whether partition key is required
    """
    cache_group = parser.add_argument_group("cache configuration")

    cache_group.add_argument("--cache-backend", type=str, help="Cache backend to use (if not specified, uses CACHE_BACKEND environment variable)")

    cache_group.add_argument("--partition-key", type=str, required=require_partition_key, help="Name of the partition key column")

    cache_group.add_argument(
        "--partition-datatype",
        type=str,
        choices=["integer", "float", "text", "timestamp"],
        help="Datatype of the partition key (if not specified, will be inferred)",
    )

    cache_group.add_argument(
        "--bitsize",
        type=int,
        help="Bitsize for bit cache handlers (default: uses handler default or environment variable)",
    )


def add_environment_args(parser: argparse.ArgumentParser) -> None:
    """
    Add environment file loading arguments to an ArgumentParser.

    Args:
        parser: The ArgumentParser to add arguments to
    """
    env_group = parser.add_argument_group("environment")

    env_group.add_argument(
        "--env-file",
        "--env",  # Alias for backward compatibility
        dest="env_file",
        type=str,
        default=".env",
        help="Path to environment file with configuration (default: .env)",
    )


def add_queue_args(parser: argparse.ArgumentParser) -> None:
    """
    Add queue-related arguments to an ArgumentParser.

    Args:
        parser: The ArgumentParser to add arguments to
    """
    queue_group = parser.add_argument_group("queue configuration")

    queue_group.add_argument(
        "--queue-provider",
        type=str,
        choices=["postgresql", "redis"],
        help="Queue provider to use (if not specified, uses QUERY_QUEUE_PROVIDER environment variable)",
    )


def add_verbosity_args(parser: argparse.ArgumentParser) -> None:
    """
    Add verbosity control arguments to an ArgumentParser.

    Args:
        parser: The ArgumentParser to add arguments to
    """
    verbosity_group = parser.add_argument_group("verbosity options")

    verbosity_group.add_argument("--quiet", "-q", action="store_true", help="Suppress status messages (only output data/results)")

    verbosity_group.add_argument("--verbose", "-v", action="store_true", help="Show detailed status and progress messages")


def add_output_args(parser: argparse.ArgumentParser) -> None:
    """
    Add output formatting arguments to an ArgumentParser.

    Args:
        parser: The ArgumentParser to add arguments to
    """
    output_group = parser.add_argument_group("output options")

    output_group.add_argument(
        "--output-format",
        choices=["list", "json", "lines"],
        default="list",
        help="Output format: list (comma-separated), json (JSON array), lines (one per line)",
    )

    output_group.add_argument("--output-file", type=str, help="Write output to file instead of console")


def configure_logging(args: argparse.Namespace) -> None:
    """
    Configure logging based on verbosity arguments.

    Args:
        args: Parsed command line arguments with quiet/verbose flags
    """
    import logging

    logger = getLogger("PartitionCache")

    # Remove existing handlers to avoid duplication
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Configure based on verbosity
    if getattr(args, "quiet", False):
        logger.setLevel(logging.WARNING)  # Only warnings and errors
    elif getattr(args, "verbose", False):
        logger.setLevel(logging.DEBUG)  # All messages including debug
    else:
        logger.setLevel(logging.INFO)  # Default: info, warnings, errors

    # Always send logging to stderr
    handler = logging.StreamHandler(sys.stderr)

    # Custom formatter that only shows level prefix for WARNING and ERROR
    class SelectiveLevelFormatter(logging.Formatter):
        def format(self, record):
            if record.levelno >= logging.WARNING:
                return f"{record.levelname}: {record.getMessage()}"
            else:
                return record.getMessage()

    handler.setFormatter(SelectiveLevelFormatter())
    logger.addHandler(handler)


def load_environment_with_validation(env_file: str, required_vars: list[str] | None = None) -> dict[str, str]:
    """
    Load environment variables from file with validation.

    Args:
        env_file: Path to environment file
        required_vars: List of required environment variable names

    Returns:
        Dictionary of loaded environment variables

    Raises:
        SystemExit: If required variables are missing
    """
    # Load environment file if it exists
    env_path = Path(env_file)
    logger = getLogger("PartitionCache")

    if env_path.exists():
        dotenv.load_dotenv(env_file)
        logger.debug(f"Loaded environment from {env_file}")
    elif env_file != ".env":  # Only warn if user explicitly specified a file
        logger.warning(f"Environment file {env_file} not found")

    # Validate required variables
    if required_vars:
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
            logger.error("Please set these variables in your environment or .env file")
            sys.exit(1)

    # Return current environment
    return dict(os.environ)


def parse_variant_generation_json_args(args: argparse.Namespace) -> tuple[dict[str, str] | None, list[str] | None, list[str] | None]:
    """
    Parse JSON string arguments for constraint modifications.

    Args:
        args: Parsed command line arguments

    Returns:
        Tuple containing (add_constraints, remove_constraints_all, remove_constraints_add)
    """
    add_constraints = None
    remove_constraints_all = None
    remove_constraints_add = None

    # Parse add_constraints
    if args.add_constraints:
        try:
            add_constraints = json.loads(args.add_constraints)
            if not isinstance(add_constraints, dict):
                raise ValueError("add_constraints must be a JSON object/dict")
            # Validate dict values are strings
            for table, constraint in add_constraints.items():
                if not isinstance(table, str) or not isinstance(constraint, str):
                    raise ValueError("add_constraints entries must be string table names mapping to string constraints")
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"Failed to parse --add-constraints: {e}")
            logger.error("Expected format: --add-constraints '{{\"table_name\": \"constraint_condition\"}}'")
            sys.exit(1)

    # Parse remove_constraints_all
    if args.remove_constraints_all:
        try:
            remove_constraints_all = json.loads(args.remove_constraints_all)
            if not isinstance(remove_constraints_all, list):
                raise ValueError("remove_constraints_all must be a JSON array/list")
            # Validate all elements are strings
            for item in remove_constraints_all:
                if not isinstance(item, str):
                    raise ValueError("All elements in remove_constraints_all must be strings")
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"Failed to parse --remove-constraints-all: {e}")
            logger.error("Expected format: --remove-constraints-all '[\"attr1\", \"attr2\"]'")
            sys.exit(1)

    # Parse remove_constraints_add
    if args.remove_constraints_add:
        try:
            remove_constraints_add = json.loads(args.remove_constraints_add)
            if not isinstance(remove_constraints_add, list):
                raise ValueError("remove_constraints_add must be a JSON array/list")
            # Validate all elements are strings
            for item in remove_constraints_add:
                if not isinstance(item, str):
                    raise ValueError("All elements in remove_constraints_add must be strings")
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"Failed to parse --remove-constraints-add: {e}")
            logger.error("Expected format: --remove-constraints-add '[\"attr1\", \"attr2\"]'")
            sys.exit(1)

    return add_constraints, remove_constraints_all, remove_constraints_add


def add_variant_generation_args(parser: argparse.ArgumentParser) -> None:
    """
    Add common variant generation arguments to an ArgumentParser.

    Args:
        parser: The ArgumentParser to add arguments to
    """
    variant_group = parser.add_argument_group("variant generation configuration")
    variant_group.add_argument(
        "--min-component-size",
        type=int,
        default=int(os.getenv("PARTITION_CACHE_MIN_COMPONENT_SIZE", "1")),
        help="Minimum number of tables in query variants (connected components) (default: 1 or PARTITION_CACHE_MIN_COMPONENT_SIZE)",
    )
    variant_group.add_argument(
        "--follow-graph",
        type=lambda x: x.lower() in ("true", "1", "yes"),
        default=os.getenv("PARTITION_CACHE_FOLLOW_GRAPH", "true").lower() in ("true", "1", "yes"),
        help="Only generate variants from tables forming connected subgraphs via multi-table predicates like distance conditions or non-equijoin conditions (default: True or PARTITION_CACHE_FOLLOW_GRAPH)",
    )
    variant_group.add_argument(
        "--no-auto-detect-star-join",
        action="store_true",
        default=os.getenv("PARTITION_CACHE_NO_AUTO_DETECT_STAR_JOIN", "false").lower() in ("true", "1", "yes"),
        help="Disable automatic star-join table detection based on query pattern (default: False or PARTITION_CACHE_NO_AUTO_DETECT_STAR_JOIN)",
    )
    variant_group.add_argument(
        "--star-join-table",
        type=str,
        default=os.getenv("PARTITION_CACHE_STAR_JOIN_TABLE", None),
        help="Explicitly specify star-join table alias or name (only one star-join table per query, or set PARTITION_CACHE_STAR_JOIN_TABLE)",
    )
    variant_group.add_argument(
        "--max-component-size",
        type=int,
        default=int(os.getenv("PARTITION_CACHE_MAX_COMPONENT_SIZE", "0")) or None,
        help="Maximum number of tables in query variants (connected components) (default: no limit or PARTITION_CACHE_MAX_COMPONENT_SIZE)",
    )
    variant_group.add_argument(
        "--no-warn-partition-key",
        action="store_true",
        default=os.getenv("PARTITION_CACHE_NO_WARN_PARTITION_KEY", "false").lower() in ("true", "1", "yes"),
        help="Disable warnings for tables not using partition key (default: False or PARTITION_CACHE_NO_WARN_PARTITION_KEY)",
    )
    variant_group.add_argument(
        "--bucket-steps",
        type=float,
        default=float(os.getenv("PARTITION_CACHE_BUCKET_STEPS", "1.0")),
        help="Step size for normalizing distance conditions (e.g., 1.0, 0.5, etc.) (default: 1.0 or PARTITION_CACHE_BUCKET_STEPS)",
    )
    variant_group.add_argument(
        "--add-constraints",
        type=str,
        default=os.getenv("PARTITION_CACHE_ADD_CONSTRAINTS", None),
        help='JSON dict mapping table names to constraints to add, e.g. \'{"table": "col = val"}\' (default: None or PARTITION_CACHE_ADD_CONSTRAINTS)',
    )
    variant_group.add_argument(
        "--remove-constraints-all",
        type=str,
        default=os.getenv("PARTITION_CACHE_REMOVE_CONSTRAINTS_ALL", None),
        help='JSON list of attribute names to remove from all query variants, e.g. \'["col1", "col2"]\' (default: None or PARTITION_CACHE_REMOVE_CONSTRAINTS_ALL)',
    )
    variant_group.add_argument(
        "--remove-constraints-add",
        type=str,
        default=os.getenv("PARTITION_CACHE_REMOVE_CONSTRAINTS_ADD", None),
        help='JSON list of attribute names to remove, creating additional variants, e.g. \'["col1", "col2"]\' (default: None or PARTITION_CACHE_REMOVE_CONSTRAINTS_ADD)',
    )


def resolve_cache_backend(args: argparse.Namespace) -> str:
    """
    Resolve cache backend from arguments or environment.

    Args:
        args: Parsed command line arguments

    Returns:
        Cache backend string

    Raises:
        SystemExit: If no cache backend is specified
    """
    cache_backend = getattr(args, "cache_backend", None) or os.getenv("CACHE_BACKEND")

    if not cache_backend:
        print("Error: No cache backend specified. Use --cache-backend or set CACHE_BACKEND environment variable")
        print("Available backends: postgresql_array, postgresql_bit, postgresql_roaringbit, redis_set, redis_bit, rocksdb_set, rocksdb_bit, rocksdict")
        sys.exit(1)

    return cache_backend


def resolve_database_name(args: argparse.Namespace) -> str:
    """
    Resolve database name from arguments or environment.

    Args:
        args: Parsed command line arguments

    Returns:
        Database name string

    Raises:
        SystemExit: If no database name is specified
    """
    db_name = getattr(args, "db_name", None) or os.getenv("DB_NAME")

    if not db_name:
        print("Error: No database name specified. Use --db-name or set DB_NAME environment variable")
        sys.exit(1)

    return db_name


def validate_mutual_exclusivity(args: argparse.Namespace, groups: list[tuple[str, list[str]]]) -> None:
    """
    Validate mutual exclusivity of argument groups.

    Args:
        args: Parsed command line arguments
        groups: List of (group_name, arg_names) tuples for mutual exclusivity validation

    Raises:
        SystemExit: If mutual exclusivity is violated
    """
    for _group_name, arg_names in groups:
        specified_args = [arg for arg in arg_names if getattr(args, arg.replace("-", "_"), None)]

        if len(specified_args) == 0:
            print(f"Error: One of {', '.join(f'--{arg}' for arg in arg_names)} must be specified")
            sys.exit(1)
        elif len(specified_args) > 1:
            print(f"Error: Only one of {', '.join(f'--{arg}' for arg in specified_args)} can be specified")
            sys.exit(1)


def get_database_connection_params(args: argparse.Namespace, backend_type: str | None = None) -> dict[str, Any]:
    """
    Get database connection parameters based on backend type and environment.

    Args:
        args: Parsed command line arguments
        backend_type: Database backend type (uses args.db_backend if not specified)

    Returns:
        Dictionary of connection parameters
    """
    backend = backend_type or getattr(args, "db_backend", "postgresql")

    if backend == "postgresql":
        return {
            "host": os.getenv("PG_DB_HOST", os.getenv("DB_HOST", "localhost")),
            "port": int(os.getenv("PG_DB_PORT", os.getenv("DB_PORT", "5432"))),
            "user": os.getenv("PG_DB_USER", os.getenv("DB_USER", "postgres")),
            "password": os.getenv("PG_DB_PASSWORD", os.getenv("DB_PASSWORD", "postgres")),
            "dbname": resolve_database_name(args),
        }
    elif backend == "mysql":
        return {
            "host": os.getenv("MY_DB_HOST", os.getenv("DB_HOST", "localhost")),
            "port": int(os.getenv("MY_DB_PORT", os.getenv("DB_PORT", "3306"))),
            "user": os.getenv("MY_DB_USER", os.getenv("DB_USER", "root")),
            "password": os.getenv("MY_DB_PASSWORD", os.getenv("DB_PASSWORD", "root")),
            "dbname": resolve_database_name(args),
        }
    elif backend == "sqlite":
        return {"db_path": getattr(args, "db_dir", "data/test_db.sqlite")}
    else:
        raise ValueError(f"Unsupported database backend: {backend}")


def setup_logging(verbose: bool = False) -> None:
    """
    Setup consistent logging configuration for CLI tools.

    Args:
        verbose: Whether to enable verbose logging
    """
    import logging

    level = logging.DEBUG if verbose else logging.INFO
    format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s" if verbose else "%(levelname)s: %(message)s"

    logging.basicConfig(level=level, format=format_string, datefmt="%Y-%m-%d %H:%M:%S")
