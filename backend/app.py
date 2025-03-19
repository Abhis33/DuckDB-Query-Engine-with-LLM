import os
import json
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from typing import Dict, List, Any

# Import the existing DuckDB LLM Query Engine
from duckdb_llm import DuckDBLLMQueryEngine

app = Flask(__name__, static_folder='../frontend/build')
CORS(app)  # Enable CORS for all routes

# Initialize the query engine
api_key = os.environ.get("OPENAI_API_KEY")
if not api_key:
    raise ValueError("OPENAI_API_KEY environment variable is not set")

engine = DuckDBLLMQueryEngine(api_key)

# Load sample data by default
DATA_DIR = os.environ.get("DATA_DIR", "./data")

@app.route('/api/load-csv', methods=['POST'])
def load_csv():
    """API endpoint to load CSV files from data directory"""
    try:
        data = request.json
        file_paths = data.get('file_paths', {})

        # Convert relative paths to absolute paths
        absolute_paths = {table: os.path.join(DATA_DIR, path)
                          for table, path in file_paths.items()}

        engine.load_csv_files(absolute_paths)
        engine.extract_schema_info()

        return jsonify({
            'success': True,
            'message': f"Loaded {len(file_paths)} tables",
            'schema': engine.schema_info
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400

@app.route('/api/schema', methods=['GET'])
def get_schema():
    """API endpoint to get database schema information"""
    try:
        if not engine.schema_info:
            engine.extract_schema_info()

        return jsonify({
            'success': True,
            'schema': engine.schema_info
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/query', methods=['POST'])
def execute_query():
    """API endpoint to execute a natural language query"""
    try:
        data = request.json
        question = data.get('question')

        if not question:
            return jsonify({
                'success': False,
                'error': 'No question provided'
            }), 400

        # Generate SQL and execute the query
        result, sql, message = engine.query_from_natural_language(question)

        # Convert DataFrame to JSON
        result_json = result.to_dict(orient='records')

        return jsonify({
            'success': True,
            'sql': sql,
            'result': result_json,
            'message': message,
            'columns': list(result.columns)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/files', methods=['GET'])
def list_files():
    """API endpoint to list available CSV files in the data directory"""
    try:
        files = []
        for file in os.listdir(DATA_DIR):
            if file.endswith('.csv'):
                files.append(file)

        return jsonify({
            'success': True,
            'files': files
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/load-sample', methods=['POST'])
def load_sample_data():
    """API endpoint to load sample data"""
    try:
        # Create sample data directory if it doesn't exist
        os.makedirs(DATA_DIR, exist_ok=True)

        # Create sample data files
        employees_path = os.path.join(DATA_DIR, "employees.csv")
        departments_path = os.path.join(DATA_DIR, "departments.csv")

        with open(employees_path, "w") as f:
            f.write("id,name,department,salary,join_date\n")
            f.write("1,John Smith,Engineering,85000,2020-01-15\n")
            f.write("2,Jane Doe,Marketing,75000,2019-05-20\n")
            f.write("3,Bob Johnson,Engineering,90000,2018-03-10\n")
            f.write("4,Alice Williams,Sales,65000,2021-07-05\n")
            f.write("5,Charlie Brown,Engineering,80000,2020-02-28\n")

        with open(departments_path, "w") as f:
            f.write("id,name,budget,location\n")
            f.write("1,Engineering,1000000,New York\n")
            f.write("2,Marketing,500000,San Francisco\n")
            f.write("3,Sales,750000,Chicago\n")
            f.write("4,HR,300000,New York\n")

        # Load the files into DuckDB
        engine.load_csv_files({
            "employees": employees_path,
            "departments": departments_path
        })

        # Extract schema information
        engine.extract_schema_info()

        return jsonify({
            'success': True,
            'message': 'Sample data loaded successfully',
            'schema': engine.schema_info
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Serve React frontend static files
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve(path):
    if path and os.path.exists(app.static_folder + '/' + path):
        return send_from_directory(app.static_folder, path)
    else:
        return send_from_directory(app.static_folder, 'index.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
