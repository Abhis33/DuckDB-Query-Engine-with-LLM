import os
import json
import logging
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
import duckdb
from openai import OpenAI

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DuckDBLLMQueryEngine:
    """A class that integrates DuckDB with OpenAI's LLM for natural language to SQL translation."""

    def __init__(self, openai_api_key: str, database_path: str = ":memory:"):
        """
        Initialize the DuckDB LLM Query Engine.

        Args:
            openai_api_key: API key for OpenAI
            database_path: Path to DuckDB database file (defaults to in-memory database)
        """
        # Set API key as environment variable instead of passing to client
        os.environ["OPENAI_API_KEY"] = openai_api_key

        # Initialize client without any arguments
        self.client = OpenAI()

        self.conn = duckdb.connect(database_path)
        self.schema_info = None
        logger.info("DuckDB LLM Query Engine initialized")

    def load_csv_files(self, file_paths: Dict[str, str]) -> None:
        """
        Load CSV files into DuckDB tables.

        Args:
            file_paths: Dictionary mapping table names to CSV file paths
        """
        for table_name, file_path in file_paths.items():
            try:
                self.conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_csv_auto('{file_path}')")
                logger.info(f"Loaded {file_path} into table {table_name}")
            except Exception as e:
                logger.error(f"Error loading {file_path}: {str(e)}")
                raise

    def extract_schema_info(self) -> Dict[str, List[Dict[str, str]]]:
        """Extract schema information from DuckDB database."""
        tables = self.conn.execute("SHOW TABLES").fetchall()
        schema_info = {}

        for table in tables:
            table_name = table[0]
            columns = self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            schema_info[table_name] = [
                {"name": col[1], "type": col[2]} for col in columns
            ]

            # Add a sample of data for better context
            sample_data = self.conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
            sample_str = "\n".join(str(row) for row in sample_data)
            schema_info[f"{table_name}_sample"] = sample_str

        self.schema_info = schema_info
        return schema_info

    def generate_sql_from_question(self, question: str) -> str:
        """
        Generate SQL from a natural language question using OpenAI's LLM.

        Args:
            question: Natural language question to convert to SQL

        Returns:
            SQL query string
        """
        if not self.schema_info:
            self.extract_schema_info()

        prompt = self._create_sql_generation_prompt(question)

        try:
            response = self.client.chat.completions.create(
                model="gpt-4-turbo",
                messages=[
                    {"role": "system", "content": "You are a SQL expert that converts natural language questions into DuckDB SQL queries. Only respond with the SQL query and nothing else."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=500
            )

            sql_query = response.choices[0].message.content.strip()

            # Sometimes the model returns the SQL with markdown formatting - remove it
            if sql_query.startswith("```sql"):
                sql_query = sql_query.replace("```sql", "").replace("```", "").strip()

            logger.info(f"Generated SQL query: {sql_query}")
            return sql_query

        except Exception as e:
            logger.error(f"Error generating SQL: {str(e)}")
            raise

    def _create_sql_generation_prompt(self, question: str) -> str:
        """Create a prompt for the LLM with schema information and the question."""
        schema_str = json.dumps(self.schema_info, indent=2)

        prompt = f"""
Given the following DuckDB database schema:

{schema_str}

Convert this question into a DuckDB SQL query:
"{question}"

Only return the SQL query, without any explanation or markdown formatting.
Ensure the query is valid DuckDB SQL syntax.
"""
        return prompt

    def execute_query(self, query: str) -> Tuple[pd.DataFrame, str]:
        """
        Execute a SQL query against DuckDB.

        Args:
            query: SQL query string

        Returns:
            Tuple of (result DataFrame, query execution message)
        """
        try:
            result = self.conn.execute(query).fetchdf()
            message = f"Query executed successfully. Returned {len(result)} rows."
            logger.info(message)
            return result, message
        except Exception as e:
            error_msg = f"Error executing query: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    def query_from_natural_language(self, question: str) -> Tuple[pd.DataFrame, str, str]:
        """
        Process a natural language question, generate SQL, and return results.

        Args:
            question: Natural language question

        Returns:
            Tuple of (result DataFrame, generated SQL query, execution message)
        """
        sql_query = self.generate_sql_from_question(question)
        result, message = self.execute_query(sql_query)
        return result, sql_query, message

    def close(self):
        """Close the DuckDB connection."""
        self.conn.close()
        logger.info("DuckDB connection closed")