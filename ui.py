#!/usr/bin/env python3
"""
Simple web UI for warlock_ingester
"""

import os
import sys
import json
from pathlib import Path
import subprocess
from http.server import HTTPServer, BaseHTTPRequestHandler
import urllib.parse

# Add the src directory to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

class WarlockHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            self.serve_home()
        elif self.path == '/files':
            self.serve_files()
        elif self.path == '/ingest':
            self.start_ingestion()
        else:
            self.send_response(404)
            self.end_headers()
            
    def serve_home(self):
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>warlock_ingester UI</title>
            <meta charset="UTF-8">
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
                .container { max-width: 1200px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
                h1 { color: #333; }
                .files-container { margin: 20px 0; }
                .file-item { 
                    padding: 10px; 
                    margin: 5px 0; 
                    background-color: #f8f9fa; 
                    border: 1px solid #dee2e6; 
                    border-radius: 4px; 
                    display: flex; 
                    justify-content: space-between;
                    align-items: center;
                }
                .file-name { font-weight: bold; }
                .file-size { color: #6c757d; font-size: 0.9em; }
                .btn { 
                    background-color: #007bff; 
                    color: white; 
                    padding: 10px 20px; 
                    border: none; 
                    border-radius: 4px; 
                    cursor: pointer; 
                    text-decoration: none;
                    display: inline-block;
                }
                .btn:hover { background-color: #0056b3; }
                .btn-ingest { background-color: #28a745; }
                .btn-ingest:hover { background-color: #1e7e34; }
                .status { 
                    margin-top: 20px; 
                    padding: 15px; 
                    border-radius: 4px; 
                    background-color: #e9ecef;
                }
                .success { background-color: #d4edda; color: #155724; }
                .error { background-color: #f8d7da; color: #721c24; }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>warlock_ingester</h1>
                <p>Source files management and ingestion</p>
                
                <div class="files-container">
                    <h2>Source Files</h2>
                    <div id="files-list">
                        <!-- Files will be loaded here -->
                    </div>
                    <div class="status" id="status" style="display:none;"></div>
                </div>
                
                <div>
                    <button class="btn btn-ingest" onclick="startIngestion()">Start Ingestion</button>
                </div>
            </div>

            <script>
                function loadFiles() {
                    fetch('/files')
                        .then(response => response.json())
                        .then(data => {
                            const container = document.getElementById('files-list');
                            if (data.files && data.files.length > 0) {
                                container.innerHTML = data.files.map(file => `
                                    <div class="file-item">
                                        <div>
                                            <span class="file-name">${file.name}</span>
                                            <span class="file-size">(${file.size} bytes)</span>
                                        </div>
                                    </div>
                                `).join('');
                            } else {
                                container.innerHTML = '<p>No files found</p>';
                            }
                        })
                        .catch(error => {
                            console.error('Error loading files:', error);
                            document.getElementById('files-list').innerHTML = '<p>Error loading files</p>';
                        });
                }
                
                function showStatus(message, isSuccess = true) {
                    const status = document.getElementById('status');
                    status.textContent = message;
                    status.className = 'status ' + (isSuccess ? 'success' : 'error');
                    status.style.display = 'block';
                    setTimeout(() => {
                        status.style.display = 'none';
                    }, 5000);
                }
                
                function startIngestion() {
                    showStatus('Starting ingestion process...', true);
                    
                    fetch('/ingest', { method: 'POST' })
                        .then(response => response.json())
                        .then(data => {
                            if (data.status === 'success') {
                                showStatus('Ingestion started successfully', true);
                            } else {
                                showStatus('Ingestion failed: ' + data.error, false);
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
            # Run the ingestion command
            process = subprocess.Popen(
                ['./localwiki', 'ingest'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=str(Path(__file__).parent)
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
    
    try:
        httpd = HTTPServer(server_address, WarlockHandler)
        print(f"warlock_ingester UI started on http://localhost:{port}")
        print("Press Ctrl+C to stop")
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")

if __name__ == '__main__':
    main()