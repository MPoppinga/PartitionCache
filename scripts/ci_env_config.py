#!/usr/bin/env python3
"""
CI Environment Configuration Helper

This script helps manage environment variables for different cache handlers
in CI/CD pipelines. It can generate environment files, validate configurations,
and provide handler-specific settings.
"""

import os
import json
from typing import Dict, List, Optional, Any
from dataclasses import dataclass


@dataclass
class CacheHandlerConfig:
    """Configuration for a specific cache handler."""
    name: str
    backend: str
    table_prefix: str
    requires_postgres: bool
    requires_redis: bool
    requires_rocksdb: bool
    env_vars: Dict[str, str]
    test_timeout: int = 300


class CIEnvironmentConfig:
    """Manage CI environment configurations for different cache handlers."""
    
    def __init__(self):
        self.handlers = self._initialize_handlers()
        self.global_env = self._get_global_env()
    
    def _initialize_handlers(self) -> Dict[str, CacheHandlerConfig]:
        """Initialize all supported cache handler configurations."""
        return {
            'postgresql-array': CacheHandlerConfig(
                name='postgresql-array',
                backend='postgresql_array',
                table_prefix='pcache_array_test',
                requires_postgres=True,
                requires_redis=False,
                requires_rocksdb=False,
                env_vars={
                    'CACHE_BACKEND': 'postgresql_array',
                    'PG_ARRAY_CACHE_TABLE_PREFIX': 'pcache_array_test',
                },
                test_timeout=300
            ),
            'postgresql-bit': CacheHandlerConfig(
                name='postgresql-bit',
                backend='postgresql_bit',
                table_prefix='pcache_bit_test',
                requires_postgres=True,
                requires_redis=False,
                requires_rocksdb=False,
                env_vars={
                    'CACHE_BACKEND': 'postgresql_bit',
                    'PG_BIT_CACHE_TABLE_PREFIX': 'pcache_bit_test',
                    'PG_BIT_CACHE_BITSIZE': '10000',
                },
                test_timeout=300
            ),
            'postgresql-roaringbit': CacheHandlerConfig(
                name='postgresql-roaringbit',
                backend='postgresql_roaringbit',
                table_prefix='pcache_roaring_test',
                requires_postgres=True,
                requires_redis=False,
                requires_rocksdb=False,
                env_vars={
                    'CACHE_BACKEND': 'postgresql_roaringbit',
                    'PG_ROARINGBIT_CACHE_TABLE_PREFIX': 'pcache_roaring_test',
                },
                test_timeout=300
            ),
            'redis-set': CacheHandlerConfig(
                name='redis-set',
                backend='redis',  # Factory uses "redis" not "redis_set"
                table_prefix='pcache_redis_test',
                requires_postgres=True,  # Still need PostgreSQL for queue
                requires_redis=True,
                requires_rocksdb=False,
                env_vars={
                    'CACHE_BACKEND': 'redis',
                    'REDIS_HOST': 'localhost',
                    'REDIS_PORT': '6379',
                    'REDIS_CACHE_DB': '0',
                    'REDIS_CACHE_KEY_PREFIX': 'pcache_redis_test:',
                },
                test_timeout=300
            ),
            'redis-bit': CacheHandlerConfig(
                name='redis-bit',
                backend='redis_bit',
                table_prefix='pcache_redis_bit_test',
                requires_postgres=True,  # Still need PostgreSQL for queue
                requires_redis=True,
                requires_rocksdb=False,
                env_vars={
                    'CACHE_BACKEND': 'redis_bit',
                    'REDIS_HOST': 'localhost',
                    'REDIS_PORT': '6379',
                    'REDIS_BIT_DB': '1',
                    'REDIS_BIT_BITSIZE': '10000',
                    'REDIS_CACHE_KEY_PREFIX': 'pcache_redis_bit_test:',
                },
                test_timeout=300
            ),
            # Note: rocks_db_set and rocks_db_bit require conda-forge RocksDB installation
            # They are not included in CI due to pip compatibility issues
            # For local testing with full RocksDB: conda install -c conda-forge rocksdb
            'rocksdict': CacheHandlerConfig(
                name='rocksdict',
                backend='rocksdict',  # Factory uses "rocksdict" not "rocks_dict"
                table_prefix='pcache_rocksdict_test',
                requires_postgres=True,  # Still need PostgreSQL for queue
                requires_redis=False,
                requires_rocksdb=False,  # RocksDict doesn't require full RocksDB
                env_vars={
                    'CACHE_BACKEND': 'rocksdict',
                    'ROCKS_DICT_PATH': '/tmp/rocksdict_test',
                    'ROCKSDB_DICT_PATH': '/tmp/rocksdict_test',
                },
                test_timeout=300
            ),
        }
    
    def _get_global_env(self) -> Dict[str, str]:
        """Get global environment variables used across all handlers."""
        return {
            'CI': 'true',
            'PYTHON_VERSION': '3.12',
            # Database settings (for cache)
            'DB_HOST': 'localhost',
            'DB_PORT': '5432',
            'DB_USER': 'test_user',
            'DB_PASSWORD': 'test_password',
            'DB_NAME': 'test_db',
            # Legacy PG variables (for compatibility)
            'PG_HOST': 'localhost',
            'PG_PORT': '5432',
            'PG_USER': 'test_user',
            'PG_PASSWORD': 'test_password',
            'PG_DBNAME': 'test_db',
            # Redis settings
            'REDIS_HOST': 'localhost',
            'REDIS_PORT': '6379',
            # Queue settings
            'QUERY_QUEUE_PROVIDER': 'postgresql',
            'PG_QUEUE_HOST': 'localhost',
            'PG_QUEUE_PORT': '5432',
            'PG_QUEUE_USER': 'test_user',
            'PG_QUEUE_PASSWORD': 'test_password',
            'PG_QUEUE_DB': 'test_db',
            # Test settings
            'PYTEST_TIMEOUT': '300',
        }
    
    def get_handler_config(self, handler_name: str) -> Optional[CacheHandlerConfig]:
        """Get configuration for a specific handler."""
        return self.handlers.get(handler_name)
    
    def get_all_handlers(self) -> List[str]:
        """Get list of all supported handler names."""
        return list(self.handlers.keys())
    
    def get_handlers_requiring_service(self, service: str) -> List[str]:
        """Get handlers that require a specific service (postgres, redis, rocksdb)."""
        handlers = []
        for name, config in self.handlers.items():
            if service == 'postgres' and config.requires_postgres:
                handlers.append(name)
            elif service == 'redis' and config.requires_redis:
                handlers.append(name)
            elif service == 'rocksdb' and config.requires_rocksdb:
                handlers.append(name)
        return handlers
    
    def generate_github_actions_matrix(self) -> Dict[str, Any]:
        """Generate GitHub Actions matrix configuration."""
        matrix_config = {
            'cache-handler': []
        }
        
        for name, config in self.handlers.items():
            matrix_config['cache-handler'].append({
                'name': config.name,
                'backend': config.backend,
                'table_prefix': config.table_prefix,
                'requires_postgres': config.requires_postgres,
                'requires_redis': config.requires_redis,
                'requires_rocksdb': config.requires_rocksdb,
                'timeout': config.test_timeout
            })
        
        return matrix_config
    
    def generate_env_file(self, handler_name: str, output_file: Optional[str] = None) -> str:
        """Generate a .env file for a specific handler."""
        config = self.get_handler_config(handler_name)
        if not config:
            raise ValueError(f"Unknown handler: {handler_name}")
        
        env_lines = []
        env_lines.append(f"# Environment configuration for {handler_name}")
        env_lines.append(f"# Generated by CI Environment Config Helper")
        env_lines.append("")
        
        # Global environment variables
        env_lines.append("# Global settings")
        for key, value in self.global_env.items():
            env_lines.append(f"{key}={value}")
        
        env_lines.append("")
        env_lines.append(f"# {handler_name} specific settings")
        for key, value in config.env_vars.items():
            env_lines.append(f"{key}={value}")
        
        env_content = "\n".join(env_lines)
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(env_content)
            print(f"Environment file written to {output_file}")
        
        return env_content
    
    def validate_environment(self, handler_name: str) -> List[str]:
        """Validate that required environment variables are set for a handler."""
        config = self.get_handler_config(handler_name)
        if not config:
            return [f"Unknown handler: {handler_name}"]
        
        missing_vars = []
        
        # Check global variables
        for key in self.global_env.keys():
            if not os.getenv(key):
                missing_vars.append(f"Missing global variable: {key}")
        
        # Check handler-specific variables
        for key in config.env_vars.keys():
            if not os.getenv(key):
                missing_vars.append(f"Missing handler variable: {key}")
        
        # Check service-specific requirements
        if config.requires_postgres:
            postgres_vars = ['PG_HOST', 'PG_PORT', 'PG_USER', 'PG_PASSWORD', 'PG_DBNAME']
            for var in postgres_vars:
                if not os.getenv(var):
                    missing_vars.append(f"Missing PostgreSQL variable: {var}")
        
        if config.requires_redis:
            redis_vars = ['REDIS_HOST', 'REDIS_PORT']
            for var in redis_vars:
                if not os.getenv(var):
                    missing_vars.append(f"Missing Redis variable: {var}")
        
        return missing_vars
    
    def generate_docker_compose_services(self) -> Dict[str, Any]:
        """Generate Docker Compose services configuration."""
        services = {}
        
        # PostgreSQL service (required by most handlers)
        services['postgres'] = {
            'image': 'postgres:16',
            'environment': {
                'POSTGRES_DB': 'test_db',
                'POSTGRES_USER': 'test_user',
                'POSTGRES_PASSWORD': 'test_password'
            },
            'ports': ['5432:5432'],
            'healthcheck': {
                'test': ['CMD-SHELL', 'pg_isready -U test_user -d test_db'],
                'interval': '10s',
                'timeout': '5s',
                'retries': 10
            }
        }
        
        # Redis service (required by Redis handlers)
        services['redis'] = {
            'image': 'redis:7-alpine',
            'ports': ['6379:6379'],
            'healthcheck': {
                'test': ['CMD', 'redis-cli', 'ping'],
                'interval': '10s',
                'timeout': '5s',
                'retries': 10
            }
        }
        
        return services
    
    def print_handler_summary(self):
        """Print a summary of all handlers and their requirements."""
        print("Cache Handler Summary")
        print("=" * 50)
        
        for name, config in self.handlers.items():
            print(f"\n{config.name}:")
            print(f"  Backend: {config.backend}")
            print(f"  Table Prefix: {config.table_prefix}")
            print(f"  Requires PostgreSQL: {config.requires_postgres}")
            print(f"  Requires Redis: {config.requires_redis}")
            print(f"  Requires RocksDB: {config.requires_rocksdb}")
            print(f"  Test Timeout: {config.test_timeout}s")
            print(f"  Environment Variables:")
            for key, value in config.env_vars.items():
                print(f"    {key}={value}")


def main():
    """Main function for CLI usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description='CI Environment Configuration Helper')
    parser.add_argument('--list-handlers', action='store_true', help='List all supported handlers')
    parser.add_argument('--generate-env', metavar='HANDLER', help='Generate .env file for handler')
    parser.add_argument('--output', metavar='FILE', help='Output file for generated content')
    parser.add_argument('--validate', metavar='HANDLER', help='Validate environment for handler')
    parser.add_argument('--matrix', action='store_true', help='Generate GitHub Actions matrix JSON')
    parser.add_argument('--docker-compose', action='store_true', help='Generate Docker Compose services')
    parser.add_argument('--summary', action='store_true', help='Print handler summary')
    
    args = parser.parse_args()
    
    config = CIEnvironmentConfig()
    
    if args.list_handlers:
        print("Supported cache handlers:")
        for handler in config.get_all_handlers():
            print(f"  - {handler}")
    
    elif args.generate_env:
        try:
            env_content = config.generate_env_file(args.generate_env, args.output)
            if not args.output:
                print(env_content)
        except ValueError as e:
            print(f"Error: {e}")
    
    elif args.validate:
        missing = config.validate_environment(args.validate)
        if missing:
            print(f"Validation failed for {args.validate}:")
            for msg in missing:
                print(f"  - {msg}")
        else:
            print(f"Environment validation passed for {args.validate}")
    
    elif args.matrix:
        matrix = config.generate_github_actions_matrix()
        print(json.dumps(matrix, indent=2))
    
    elif args.docker_compose:
        services = config.generate_docker_compose_services()
        print(json.dumps({'services': services}, indent=2))
    
    elif args.summary:
        config.print_handler_summary()
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()