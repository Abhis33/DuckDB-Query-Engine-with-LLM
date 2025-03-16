# DuckDB SQL Query Engine with LLM

A powerful tool that lets you query DuckDB using natural language. This application translates your questions into SQL with OpenAI's GPT models.

## Features

- Translate natural language to SQL queries
- Query DuckDB databases with plain English
- Easy Docker deployment
- Sample data included for testing
- Interactive command-line interface

## Getting Started

### Prerequisites

- Docker and Docker Compose
- OpenAI API key

### Setup

1. Clone this repository
   ```bash
   git clone https://github.com/yourusername/duckdb-llm-query-engine.git
   cd duckdb-llm-query-engine
   ```

2. Create a .env file with your OpenAI API key
   ```bash
   echo "OPENAI_API_KEY=your-api-key-here" > .env
   ```

3. Build and run with Docker Compose
   ```bash
   docker-compose up -d
   ```

4. Run the application
   ```bash
   docker-compose exec duckdb-llm python app.py
   ```

## Usage

When the application starts, you'll see an interactive prompt. Type your questions in plain English:

```
=== DuckDB LLM Query Engine ===
Type your questions in natural language, or 'exit' to quit.

Question: How many employees work in Engineering?
```

The system will:
1. Convert your question to SQL
2. Run the query
3. Show you the results

## Adding Your Own Data

Place CSV files in the `data` directory and modify the `load_sample_data` method in `app.py`:

```python
def load_sample_data(self):
    self.engine.load_csv_files({
        "your_table": "path/to/your/file.csv",
    })
    self.engine.extract_schema_info()
```

## How It Works

1. Your question goes to the OpenAI LLM
2. The system provides table schemas as context
3. The LLM generates the appropriate SQL query
4. DuckDB executes the query and returns results

## Troubleshooting

If you see errors about OpenAI client initialization:

```bash
# Check OpenAI version
docker-compose exec duckdb-llm pip show openai

# If needed, update the requirements.txt file and rebuild
docker-compose build --no-cache
```

## License

MIT

## Acknowledgments

- [DuckDB](https://duckdb.org/) - The in-process SQL OLAP database
- [OpenAI](https://openai.com/) - For the language models
