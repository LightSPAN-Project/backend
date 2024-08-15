import json
import sqlite3
from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse

class RequestHandler(BaseHTTPRequestHandler):
    def _set_response(self, response_code=200, content_type='application/json'):
        self.send_response(response_code)
        self.send_header('Content-type', content_type)
        self.end_headers()

    def do_GET(self):
        parsed_path = urllib.parse.urlparse(self.path)
        path_parts = parsed_path.path.split('/')
        
        if len(path_parts) >= 3 and path_parts[1] == 'get_value':
            try:
                user_id = int(path_parts[2])
                value = self.get_value_from_db(user_id)
                if value is not None:
                    self._set_response()
                    self.wfile.write(json.dumps({'light_exposure': value}).encode('utf-8'))
                else:
                    self._set_response(404)
                    self.wfile.write(json.dumps({'error': 'Value not found'}).encode('utf-8'))
            except ValueError:
                self._set_response(400)
                self.wfile.write(json.dumps({'error': 'Invalid user_id'}).encode('utf-8'))
        else:
            self._set_response(404)
            self.wfile.write(json.dumps({'error': 'Endpoint not found'}).encode('utf-8'))

    def get_value_from_db(self, user_id):
        # conn = sqlite3.connect('data.db')
        # cursor = conn.cursor()
        # cursor.execute('SELECT value FROM my_table WHERE user_id = ?', (user_id,))
        # result = cursor.fetchone()
        # conn.close()

        value = None

        if user_id == 1638:
            value = 7.8
        elif user_id == 1637:
            value = 3.1
        
        return value #result[0] if result else None

def run(server_class=HTTPServer, handler_class=RequestHandler, port=8080):
    server_address = ('0.0.0.0', port)
    httpd = server_class(server_address, handler_class)
    print(f'Starting httpd server on port {port}')
    httpd.serve_forever()