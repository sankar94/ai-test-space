#!/usr/bin/env python3
from http.server import HTTPServer, SimpleHTTPRequestHandler
import json
import os
from urllib.parse import urlparse

class ReportHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == '/get_reports':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()

            file_path = 'data/user_reports.geojson'
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    self.wfile.write(f.read().encode('utf-8'))
            else:
                empty = {"type": "FeatureCollection", "features": []}
                self.wfile.write(json.dumps(empty).encode('utf-8'))
            return

        # Handle regular files
        return super().do_GET()

    def do_POST(self):
        if self.path == '/save_reports':
            try:
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)

                # Save to file
                os.makedirs('data', exist_ok=True)
                with open('data/user_reports.geojson', 'w', encoding='utf-8') as f:
                    f.write(post_data.decode('utf-8'))

                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                response = {"success": True, "message": "Report saved"}
                self.wfile.write(json.dumps(response).encode('utf-8'))
                print(f"✓ Saved report to data/user_reports.geojson")
            except Exception as e:
                print(f"Error saving report: {e}")
                self.send_response(500)
                self.end_headers()
            return

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

if __name__ == '__main__':
    PORT = 8000
    server = HTTPServer(('localhost', PORT), ReportHandler)
    print(f'🚀 Server running on http://localhost:{PORT}')
    print(f'📁 Reports saved to: data/user_reports.geojson')
    print('Press Ctrl+C to stop')
    server.serve_forever()
