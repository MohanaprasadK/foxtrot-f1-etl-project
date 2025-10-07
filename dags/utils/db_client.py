import logging
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import List, Optional
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

class DBClient:
    """
    Handles all database operations for the F1 ETL pipeline
    """
    
    def __init__(self, connection_id: str = "formulaone_db"):
        self.connection_id = connection_id
        self.postgres_hook = PostgresHook(postgres_conn_id=connection_id)
    
    def truncate_table(self, table_name: str) -> bool:
        """
        Truncate a table (for full load strategy)
        
        Args:
            table_name: Name of the table to truncate
            
        Returns:
            bool: True if successful
        """
        try:
            conn = self.postgres_hook.get_conn()
            cursor = conn.cursor()
            
            truncate_query = f"TRUNCATE TABLE {table_name};"
            cursor.execute(truncate_query)
            conn.commit()
            
            logger.info(f"✅ Successfully truncated table: {table_name}")
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to truncate table {table_name}: {e}")
            raise
    
    def create_table_if_not_exists(self, table_name: str, columns_config: List[dict]) -> bool:
        """
        Create table if it doesn't exist based on column configuration
        
        Args:
            table_name: Name of the table to create
            columns_config: List of column definitions from metadata
            
        Returns:
            bool: True if table exists or was created successfully
        """
        try:
            conn = self.postgres_hook.get_conn()
            cursor = conn.cursor()
            
            # Check if table exists
            check_query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            );
            """
            cursor.execute(check_query, (table_name,))
            table_exists = cursor.fetchone()[0]
            
            if table_exists:
                logger.info(f"✅ Table already exists: {table_name}")
                cursor.close()
                conn.close()
                return True
            
            # Build CREATE TABLE query
            column_definitions = []
            for col in columns_config:
                col_name = col['name']
                col_type = self._map_data_type(col['type'])
                nullable = "NULL" if col.get('required', True) else "NOT NULL"
                column_definitions.append(f"{col_name} {col_type} {nullable}")
            
            create_query = f"CREATE TABLE {table_name} ({', '.join(column_definitions)});"
            
            cursor.execute(create_query)
            conn.commit()
            
            logger.info(f"✅ Successfully created table: {table_name}")
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create table {table_name}: {e}")
            raise
    
    def _map_data_type(self, data_type: str) -> str:
        """
        Map generic data types to PostgreSQL specific types
        
        Args:
            data_type: Generic data type from metadata
            
        Returns:
            str: PostgreSQL data type
        """
        type_mapping = {
            'integer': 'INTEGER',
            'int': 'INTEGER',
            'varchar': 'VARCHAR',
            'text': 'TEXT',
            'timestamp': 'TIMESTAMP',
            'datetime': 'TIMESTAMP',
            'boolean': 'BOOLEAN',
            'float': 'REAL',
            'double': 'DOUBLE PRECISION'
        }
        return type_mapping.get(data_type.lower(), 'TEXT')
    
    def insert_dataframe(self, table_name: str, df: pd.DataFrame, 
                        if_exists: str = 'append') -> bool:
        """
        Insert DataFrame into PostgreSQL table
        
        Args:
            table_name: Target table name
            df: DataFrame to insert
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            
        Returns:
            bool: True if successful
        """
        try:
            # Get SQLAlchemy engine from hook
            conn = self.postgres_hook.get_sqlalchemy_engine()
            
            # Insert DataFrame
            rows_inserted = df.to_sql(
                name=table_name,
                con=conn,
                if_exists=if_exists,
                index=False,
                method='multi'
            )
            
            logger.info(f"✅ Successfully inserted {rows_inserted} rows into {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to insert DataFrame into {table_name}: {e}")
            raise
    
    def execute_query(self, query: str, params: Optional[dict] = None) -> list:
        """
        Execute a custom SQL query
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            list: Query results
        """
        try:
            if params:
                records = self.postgres_hook.get_records(query, parameters=params)
            else:
                records = self.postgres_hook.get_records(query)
            
            return records
            
        except Exception as e:
            logger.error(f"❌ Failed to execute query: {e}")
            raise
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database
        
        Args:
            table_name: Table to check
            
        Returns:
            bool: True if table exists
        """
        try:
            query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            );
            """
            result = self.postgres_hook.get_first(query, parameters=(table_name,))
            return result[0] if result else False
            
        except Exception as e:
            logger.error(f"❌ Failed to check if table {table_name} exists: {e}")
            raise


# Utility functions for easy usage
def truncate_table(table_name: str, connection_id: str = "formulaone_db") -> bool:
    """Convenience function to truncate a table"""
    client = DBClient(connection_id)
    return client.truncate_table(table_name)

def insert_dataframe(table_name: str, df: pd.DataFrame, 
                    connection_id: str = "formulaone_db") -> bool:
    """Convenience function to insert DataFrame"""
    client = DBClient(connection_id)
    return client.insert_dataframe(table_name, df)

def create_table(table_name: str, columns_config: List[dict],
                connection_id: str = "formulaone_db") -> bool:
    """Convenience function to create table"""
    client = DBClient(connection_id)
    return client.create_table_if_not_exists(table_name, columns_config)