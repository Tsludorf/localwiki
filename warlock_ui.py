#!/usr/bin/env python3
"""
Simple Web UI for warlock_ingester
"""

import os
import sys
import json
from pathlib import Path
import subprocess
import http.server
import socketserver
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler

# Add current directory to Python path to access modules
sys.path.insert(0, str(Path(__file__).parent))

class WarlockUIHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            self.serve_home()
        elif self.path == '/files':
            self.serve_files()
        elif self.path == '/api/ingest':
            self.start_ingestion()
        else:
            # Serve static files
            try:
                if '.' in self.path:
                    file_path = Path(__file__).parent / self.path.lstrip('/')
                    if file_path.exists():
                        with open(file_path, 'rb') as f:
                            self.send_response(200)
                            self.end_headers()
                            self.wfile.write(f.read())
                    else:
                        self.send_response(404)
                        self.end_headers()
                else:
                    self.send_response(404)
                    self.end_headers()
            except Exception as e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(str(e).encode())

    def serve_home(self):
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>warlock_ingester UI</title>
            <meta charset="UTF-8">
            <style>
                body { 
                    font-family: Arial, sans-serif; 
                    margin: 20px; 
                    background-color: #f5f5f5; 
                    color: #333;
                }
                .container { 
                    max-width: 1200px; 
                    margin: 0 auto; 
                    background-color: white; 
                    padding: 20px; 
                    border-radius: 8px; 
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1); 
                }
                h1 { 
                    color: #2c3e50; 
                    border-bottom: 2px solid #3498db; 
                    padding-bottom: 10px; 
                }
                .files-container { 
                    margin: 20px 0; 
                    background-color: #f8f9fa; 
                    padding: 15px; 
                    border-radius: 5px; 
                    border: 1px solid #dee2e6;
                }
                .file-item { 
                    padding: 10px; 
                    margin: 5px 0; 
                    background-color: white; 
                    border: 1px solid #dee2e6; 
                    border-radius: 4px; 
                    display: flex; 
                    justify-content: space-between;
                    align-items: center;
                }
                .file-name { 
                    font-weight: bold; 
                    color: #2c3e50;
                }
                .file-size { 
                    color: #6c757d; 
                    font-size: 0.9em; 
                }
                .btn { 
                    background-color: #3498db; 
                    color: white; 
                    padding: 12px 20px; 
                    border: none; 
                    border-radius: 4px; 
                    cursor: pointer; 
                    text-decoration: none;
                    display: inline-block;
                    font-size: 16px;
                    margin: 10px 5px;
                }
                .btn:hover { 
                    background-color: #2980b9; 
                }
                .btn-ingest { 
                    background-color: #27ae60; 
                }
                .btn-ingest:hover { 
                    background-color: #219653; 
                }
                .status { 
                    margin: 20px 0; 
                    padding: 15px; 
                    border-radius: 4px; 
                    background-color: #e9ecef;
                    display: none;
                }
                .success { 
                    background-color: #d4edda; 
                    color: #155724; 
                    border: 1px solid #c3e6cb;
                }
                .error { 
                    background-color: #f8d7da; 
                    color: #721c24; 
                    border: 1px solid #f5c6cb;
                }
                .loading {
                    display: none;
                    color: #3498db;
                }
                .refresh-btn {
                    background-color: #9b59b6;
                }
                .refresh-btn:hover {
                    background-color: #8e44ad;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>📚 warlock_ingester</h1>
                <p>Source files management and automatic ingestion</p>
                
                <div class="files-container">
                    <h2>📂 Source Files</h2>
                    <div id="files-list">
                        <p>Loading files...</p>
                    </div>
                    <div class="loading" id="loading">Loading files...</div>
                </div>
                
                <div>
                    <button class="btn btn-ingest" onclick="startIngestion()">🔄 Start Ingestion Process</button>
                    <button class="btn refresh-btn" onclick="loadFiles()">🔄 Refresh Files</button>
                </div>
                
                <div class="status" id="status"></div>
                <div class="status" id="status-success"></div>
            </div>

            <script>
                function loadFiles() {
                    const loading = document.getElementById('loading');
                    const filesList = document.getElementById('files-list');
                    loading.style.display = 'block';
                    
                    fetch('/files')
                        .then(response => response.json())
                        .then(data => {
                            loading.style.display = 'none';
                            if (data.files && data.files.length > 0) {
                                filesList.innerHTML = data.files.map(file => `
                                    <div class="file-item">
                                        <div>
                                            <span class="file-name">${file.name}</span>
                                            <span class="file-size">(${file.size} bytes)</span>
                                        </div>
                                    </div>
                                `).join('');
                            } else {
                                filesList.innerHTML = '<p>No files found in sources directory</p>';
                            }
                        })
                        .catch(error => {
                            console.error('Error loading files:', error);
                            loading.style.display = 'none';
                            filesList.innerHTML = '<p>Error loading files: ' + error.message + '</p>';
                        });
                }
                
                function showStatus(message, isSuccess = true, duration = 5000) {
                    const status = document.getElementById(isSuccess ? 'status-success' : 'status');
                    status.textContent = message;
                    status.className = 'status ' + (isSuccess ? 'success' : 'error');
                    status.style.display = 'block';
                    
                    setTimeout(() => {
                        status.style.display = 'none';
                    }, duration);
                }
                
                function startIngestion() {
                    showStatus('Starting ingestion process...', true);
                    
                    fetch('/api/ingest', { method: 'POST' })
                        .then(response => response.json())
                        .then(data => {
                            if (data.status === 'success') {
                                showStatus('Ingestion started successfully! Check console for progress.', true);
                                // Reload files in case new ones were added
                                setTimeout(loadFiles, 2000);
                            } else {
                                showStatus('Ingestion failed: ' + (data.error || 'Unknown error'), false);
                            }
                        })
                        .catch(error => {
                            console.error('Error starting ingestion:', error);
                            showStatus('Error starting ingestion: ' + error.message, false);
                        });
                }
                
                // Load files when page loads
                document.addEventListener('DOMContentLoaded', loadFiles);
            </script>
        </body>
        </html>
        """
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(html.encode())

    def serve_files(self):
        """Return list of files in sources directory"""
        try:
            sources_dir = Path("data/sources")
            if sources_dir.exists():
                files = []
                for file_path in sources_dir.iterdir():
                    if file_path.is_file():
                        stat = file_path.stat()
                        files.append({
                            'name': file_path.name,
                            'size': stat.st_size,
                            'path': str(file_path)
                        })
                
                response = {'files': files}
            else:
                # Create sources directory if it doesn't exist
                sources_dir.mkdir(exist_ok=True)
                response = {'files': []}
                
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())
            
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'error': str(e)}).encode())
            
    def start_ingestion(self):
        """Start the ingestion process"""
        try:
            process = subprocess.Popen(
                ['./localwiki', 'ingest'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=Path(__file__).parent
            )
            
            stdout, stderr = process.communicate()
            
            if process.returncode == 0:
                response = {'status': 'success', 'output': stdout.decode()}
            else:
                response = {'status': 'error', 'error': stderr.decode()}
                
        except Exception as e:
            response = {'status': 'error', 'error': str(e)}
            
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(response).encode())

def main():
    """Start the web server"""
    port = 8080
    server_address = ('', port)
    
    # Create a simple HTTP server
    handler = WarlockUIHandler
    
    try:
        httpd = HTTPServer(server_address, handler)
        print(f"warlock_ingester UI started on http://localhost:{port}")
        print("Press Ctrl+C to stop")
        print("\n" + "="*50)
        print("INSTRUCTIONS:")
        print("1. Files in 'data/sources' directory will be listed below")
        print("2. Click 'Start Ingestion Process' to begin processing")
        print("3. The system will process all files in the sources folder")
        print("="*50)
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")

if __name__ == '__main__':
    main()