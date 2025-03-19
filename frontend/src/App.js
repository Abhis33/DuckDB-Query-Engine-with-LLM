import React, { useState, useEffect } from 'react';
import './App.css';

function App() {
  const [question, setQuestion] = useState('');
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [schema, setSchema] = useState(null);
  const [error, setError] = useState(null);
  const [availableFiles, setAvailableFiles] = useState([]);
  const [selectedFiles, setSelectedFiles] = useState({});
  
  const apiUrl = process.env.REACT_APP_API_URL || 'http://localhost:5000';

  useEffect(() => {
    // Fetch available files
    fetchFiles();
  }, []);

  const fetchFiles = async () => {
    try {
      const response = await fetch(`${apiUrl}/api/files`);
      const data = await response.json();
      
      if (data.success) {
        setAvailableFiles(data.files);
      } else {
        console.error("Failed to fetch files:", data.error);
      }
    } catch (err) {
      console.error("Error fetching files:", err);
    }
  };

  const loadSampleData = async () => {
    setLoading(true);
    setError(null);
    
    try {
      const response = await fetch(`${apiUrl}/api/load-sample`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        }
      });
      
      const data = await response.json();
      
      if (data.success) {
        setSchema(data.schema);
        fetchFiles(); // Refresh file list
      } else {
        setError(data.error || "Failed to load sample data");
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleFileSelection = (file, tableName) => {
    setSelectedFiles(prev => ({
      ...prev,
      [tableName]: file
    }));
  };

  const loadSelectedFiles = async () => {
    if (Object.keys(selectedFiles).length === 0) {
      setError("Please select files to load");
      return;
    }
    
    setLoading(true);
    setError(null);
    
    try {
      const response = await fetch(`${apiUrl}/api/load-csv`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          file_paths: selectedFiles
        })
      });
      
      const data = await response.json();
      
      if (data.success) {
        setSchema(data.schema);
      } else {
        setError(data.error || "Failed to load files");
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };
  
  const fetchSchema = async () => {
    try {
      const response = await fetch(`${apiUrl}/api/schema`);
      const data = await response.json();
      
      if (data.success) {
        setSchema(data.schema);
      } else {
        console.error("Failed to fetch schema:", data.error);
      }
    } catch (err) {
      console.error("Error fetching schema:", err);
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!question.trim()) return;
    
    setLoading(true);
    setError(null);
    setResult(null);
    
    try {
      const response = await fetch(`${apiUrl}/api/query`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ question })
      });
      
      const data = await response.json();
      
      if (data.success) {
        setResult(data);
      } else {
        setError(data.error || "Query failed");
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="App">
      <header className="App-header">
        <h1>DuckDB LLM Query Engine</h1>
        <p>Ask questions about your data in natural language</p>
      </header>
      
      <main className="App-main">
        <section className="data-section">
          <div className="card">
            <h2>Load Data</h2>
            <div className="data-options">
              <button 
                onClick={loadSampleData} 
                disabled={loading} 
                className="button"
              >
                Load Sample Data
              </button>
              
              <div className="file-selector">
                <h3>Or load your own CSV files:</h3>
                {availableFiles.length > 0 ? (
                  <>
                    <div className="file-mapping">
                      {availableFiles.map(file => (
                        <div key={file} className="file-map-row">
                          <span>{file}</span>
                          <input 
                            type="text" 
                            placeholder="Table name" 
                            onChange={(e) => handleFileSelection(file, e.target.value)} 
                            className="table-input"
                          />
                        </div>
                      ))}
                    </div>
                    <button 
                      onClick={loadSelectedFiles} 
                      disabled={loading || Object.keys(selectedFiles).length === 0} 
                      className="button"
                    >
                      Load Selected Files
                    </button>
                  </>
                ) : (
                  <p>No CSV files found. Please place CSV files in the data directory.</p>
                )}
              </div>
            </div>
          </div>
        </section>
        
        {schema && (
          <section className="schema-section">
            <div className="card">
              <h2>Database Schema</h2>
              <div className="schema-info">
                {Object.keys(schema).filter(table => !table.includes('_sample')).map(table => (
                  <div key={table} className="table-info">
                    <h3>{table}</h3>
                    <ul>
                      {schema[table].map((column, idx) => (
                        <li key={idx}>{column.name}: {column.type}</li>
                      ))}
                    </ul>
                  </div>
                ))}
              </div>
            </div>
          </section>
        )}
        
        <section className="query-section">
          <div className="card">
            <h2>Ask a Question</h2>
            <form onSubmit={handleSubmit}>
              <input
                type="text"
                value={question}
                onChange={(e) => setQuestion(e.target.value)}
                placeholder="e.g., What's the average salary by department?"
                disabled={loading || !schema}
                className="question-input"
              />
              <button 
                type="submit" 
                disabled={loading || !schema || !question.trim()} 
                className="button"
              >
                {loading ? 'Processing...' : 'Submit'}
              </button>
            </form>
          </div>
        </section>
        
        {error && (
          <section className="error-section">
            <div className="card error">
              <h2>Error</h2>
              <p>{error}</p>
            </div>
          </section>
        )}
        
        {result && (
          <section className="result-section">
            <div className="card">
              <h2>Query Results</h2>
              <div className="query-details">
                <h3>Generated SQL:</h3>
                <pre className="sql-code">{result.sql}</pre>
                <p className="result-message">{result.message}</p>
              </div>
              
              <div className="result-table">
                <table>
                  <thead>
                    <tr>
                      {result.columns.map((col, idx) => (
                        <th key={idx}>{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {result.result.map((row, rowIdx) => (
                      <tr key={rowIdx}>
                        {result.columns.map((col, colIdx) => (
                          <td key={colIdx}>{String(row[col] !== null ? row[col] : '')}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </section>
        )}
      </main>
      
      <footer className="App-footer">
        <p>DuckDB LLM Query Engine &copy; 2025</p>
      </footer>
    </div>
  );
}

export default App;