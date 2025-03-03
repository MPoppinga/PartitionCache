from partitioncache.db_handler.abstract import AbstractDBHandler


def get_db_handler(db_type: str, **kwargs) -> AbstractDBHandler:
    """
    Factory function to create database handlers.
    
    Args:
        db_type: Type of database ('sqlite', 'mysql', 'postgres')
        **kwargs: Arguments to pass to the handler constructor
        
    Returns:
        An instance of the requested database handler
    """
    if db_type.lower() == 'sqlite':
        from partitioncache.db_handler.sqlite import SQLiteDBHandler
        return SQLiteDBHandler(**kwargs)
    if db_type.lower() == 'mysql':
        from partitioncache.db_handler.mysql import MySQLDBHandler
        return MySQLDBHandler(**kwargs)
    if db_type.lower() == 'postgres':
        from partitioncache.db_handler.postgres import PostgresDBHandler
        return PostgresDBHandler(**kwargs)
        
    supported = ["sqlite", "mysql", "postgres"]
    raise ValueError(f"Unsupported database type: {db_type}. Options: {', '.join(supported)}")
