import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def save_dataframe_to_postgres(df, table_name="apache_logs_proc", if_exists="replace"):
    """
    Save a Pandas DataFrame to a PostgreSQL table
    
    Parameters:
    -----------
    df : pandas.DataFrame
        The DataFrame to save
    table_name : str
        The name of the table in PostgreSQL
    if_exists : str
        How to behave if the table already exists:
        - 'fail': Raise a ValueError
        - 'replace': Drop the table before inserting new values
        - 'append': Insert new values to the existing table
    """
    try:
        # PostgreSQL connection parameters
        db_params = {
            'dbname': 'postgres',
            'user': 'postgres',
            'password': 'postgres',  # Default password, change if different
            'host': 'postgresql',    # Container name from docker-compose
            'port': '5432'
        }
        
        logger.info(f"Connecting to PostgreSQL database at {db_params['host']}:{db_params['port']}")
        
        # Create SQLAlchemy engine
        connection_string = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
        engine = create_engine(connection_string)
        
        # Save DataFrame to PostgreSQL
        logger.info(f"Saving DataFrame to table '{table_name}' with '{if_exists}' option")

        print(f"\nMethod : insert_save Output :{df.head(5)}")
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            chunksize=1000  # Process data in chunks to handle large datasets
        )
        
        # Test connection to verify the data was saved
        with engine.connect() as conn:
            result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            logger.info(f"Successfully saved {result[0]} rows to table '{table_name}'")
        
        return True
    
    except Exception as e:
        logger.error(f"Error saving DataFrame to PostgreSQL: {str(e)}")
        raise

if __name__ == "__main__":
    # Example usage:
    # This section will only run when the script is executed directly
    
    try:
        # Example: Load a DataFrame from a CSV file
        sample_file = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "data", 
            "enriched_apache_logs.csv"
        )
        
        if os.path.exists(sample_file):
            df = pd.read_csv(sample_file)
            save_dataframe_to_postgres(df, "ml_dataset")
            logger.info("Example complete: DataFrame saved to PostgreSQL")
        else:
            logger.warning(f"Example file not found: {sample_file}")
    
    except Exception as e:
        logger.exception("Error in example usage")