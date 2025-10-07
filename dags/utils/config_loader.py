import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

class ConfigLoader:
    """
    Loads configuration from Airflow Variables and PostgreSQL metadata table
    """
    
    def __init__(self):
        self.load_type = None
        self.db_connection_name = None
        self.metadata_table_name = None
        self._load_airflow_variables()
    
    def _load_airflow_variables(self) -> None:
        """
        Load Airflow Variables for the F1 ETL pipeline
        """
        try:
            self.load_type = Variable.get("load_type", default_var="full")
            self.db_connection_name = Variable.get("db_connection_name", default_var="formulaone_db")
            self.metadata_table_name = Variable.get("metadata_table_name", default_var="formulaone_metadata")
            
            logger.info(f"✅ Loaded Airflow Variables: "
                       f"load_type={self.load_type}, "
                       f"db_connection_name={self.db_connection_name}, "
                       f"metadata_table_name={self.metadata_table_name}")
                       
        except Exception as e:
            logger.error(f"❌ Failed to load Airflow Variables: {e}")
            raise
    
    def get_table_metadata(self) -> Dict[str, Any]:
        """
        Fetch and group metadata from PostgreSQL by table_name
        
        Returns:
            Dictionary with table_name as key and table config as value
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id=self.db_connection_name)
            
            # Query to get all active metadata for our DAG, grouped by table
            query = """
            SELECT 
                table_name,
                api_endpoint,
                ARRAY_AGG(column_name ORDER BY column_order) as columns,
                ARRAY_AGG(data_type ORDER BY column_order) as data_types,
                ARRAY_AGG(is_required ORDER BY column_order) as is_required_flags
            FROM {metadata_table}
            WHERE dag_name = 'formulaone' 
                AND is_active = true
            GROUP BY table_name, api_endpoint
            ORDER BY table_name;
            """.format(metadata_table=self.metadata_table_name)
            
            records = postgres_hook.get_records(query)
            logger.info(f"📊 Found {len(records)} tables in metadata")
            
            # Transform into structured dictionary
            table_metadata = {}
            for record in records:
                table_name, api_endpoint, columns, data_types, is_required_flags = record
                
                table_metadata[table_name] = {
                    'api_endpoint': api_endpoint,
                    'columns': [
                        {
                            'name': columns[i],
                            'type': data_types[i],
                            'required': is_required_flags[i],
                            'order': i + 1
                        }
                        for i in range(len(columns))
                    ],
                    'load_type': self.load_type
                }
                
                logger.info(f"📋 Table: {table_name}, Endpoint: {api_endpoint}, "
                           f"Columns: {len(columns)}")
            
            return table_metadata
            
        except Exception as e:
            logger.error(f"❌ Failed to load table metadata: {e}")
            raise
    
    def get_table_names(self) -> List[str]:
        """
        Get list of all table names from metadata
        """
        table_metadata = self.get_table_metadata()
        return list(table_metadata.keys())


# Utility function for easy usage
def load_table_metadata() -> Dict[str, Any]:
    """
    Convenience function to load all table metadata
    """
    loader = ConfigLoader()
    return loader.get_table_metadata()


def get_table_list() -> List[str]:
    """
    Convenience function to get list of table names
    """
    loader = ConfigLoader()
    return loader.get_table_names()