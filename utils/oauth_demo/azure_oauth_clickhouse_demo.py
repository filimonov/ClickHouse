#!/usr/bin/env python3
"""Simple OAuth 2.0 demo for ClickHouse.

This script starts a local HTTP server with a page containing a
button to login via Azure (Microsoft Entra ID). After the user
successfully authenticates, the resulting access token is used to
execute `SELECT currentUser()` on a ClickHouse instance via HTTP.

All required configuration parameters are defined as constants below.
"""

import http.server
import json
import socketserver
import urllib.parse
import urllib.request
import webbrowser

# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------
CLIENT_ID = "YOUR_AZURE_CLIENT_ID"
CLIENT_SECRET = "YOUR_AZURE_CLIENT_SECRET"  # Optional if public client
TENANT_ID = "YOUR_TENANT_ID"
SCOPE = "openid profile email"
PORT = 8000
REDIRECT_URI = f"http://localhost:{PORT}/callback"
CLICKHOUSE_QUERY_URL = (
    "http://clickhouse:8123/?query=SELECT%20currentUser()"
)

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
AUTH_ENDPOINT = f"{AUTHORITY}/oauth2/v2.0/authorize"
TOKEN_ENDPOINT = f"{AUTHORITY}/oauth2/v2.0/token"


class OAuthHandler(http.server.SimpleHTTPRequestHandler):
    """HTTP handler implementing the OAuth 2.0 flow."""

    def do_GET(self):
        if self.path == "/":
            html = "<html><body><a href='/login'>Login with Azure</a></body></html>"
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(html.encode())
            return

        if self.path.startswith("/login"):
            params = urllib.parse.urlencode(
                {
                    "client_id": CLIENT_ID,
                    "response_type": "code",
                    "redirect_uri": REDIRECT_URI,
                    "response_mode": "query",
                    "scope": SCOPE,
                }
            )
            url = AUTH_ENDPOINT + "?" + params
            self.send_response(302)
            self.send_header("Location", url)
            self.end_headers()
            return

        if self.path.startswith("/callback"):
            query = urllib.parse.urlparse(self.path).query
            code = urllib.parse.parse_qs(query).get("code", [None])[0]
            if not code:
                self.send_error(400, "Missing authorization code")
                return

            data = urllib.parse.urlencode(
                {
                    "client_id": CLIENT_ID,
                    "client_secret": CLIENT_SECRET,
                    "scope": SCOPE,
                    "code": code,
                    "redirect_uri": REDIRECT_URI,
                    "grant_type": "authorization_code",
                }
            ).encode()
            req = urllib.request.Request(
                TOKEN_ENDPOINT,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            with urllib.request.urlopen(req) as token_resp:
                token_body = json.load(token_resp)
            access_token = token_body.get("access_token")
            if not access_token:
                self.send_error(500, "No access token returned")
                return

            ch_req = urllib.request.Request(
                CLICKHOUSE_QUERY_URL,
                headers={"Authorization": f"Bearer {access_token}"},
            )
            with urllib.request.urlopen(ch_req) as ch_resp:
                result = ch_resp.read().decode()

            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            html = f"<html><body>ClickHouse replied:<pre>{result}</pre></body></html>"
            self.wfile.write(html.encode())
            return

        self.send_error(404)


if __name__ == "__main__":
    with socketserver.TCPServer(("", PORT), OAuthHandler) as httpd:
        print(f"Open http://localhost:{PORT}/ in your browser to start OAuth flow.")
        try:
            webbrowser.open(f"http://localhost:{PORT}/")
        except Exception:
            pass
        httpd.serve_forever()
