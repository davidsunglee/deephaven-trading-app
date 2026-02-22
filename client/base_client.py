"""
Base Deephaven Client
Reusable helper for connecting to the Deephaven Trading Server.
Quants, risk analysts, and PMs import this to avoid boilerplate.
"""

from pydeephaven import Session


class DeephavenClient:
    """Lightweight wrapper around pydeephaven.Session."""

    def __init__(self, host="localhost", port=10000):
        self.host = host
        self.port = port
        self.session = Session(host=host, port=port)
        print(f"Connected to Deephaven server at {host}:{port}")

    def list_tables(self):
        """Return names of all tables in the server's global scope."""
        return self.session.tables

    def open_table(self, name):
        """Open a server-side table by name."""
        return self.session.open_table(name)

    def run_script(self, script):
        """Execute a Python script on the server. Use this to create
        custom derived tables that live server-side."""
        self.session.run_script(script)

    def bind_table(self, name, table):
        """Publish a client-created table to the server's global scope
        so it is visible in the web IDE and to other sessions."""
        self.session.bind_table(name=name, table=table)

    def close(self):
        """Close the session."""
        self.session.close()
        print("Session closed.")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
