import sys
from icedrive_blob.app import ClientApp, BlobApp

def client() -> int:
    """Handler for 'icedrive-blob-client'."""
    app = ClientApp()
    return app.main(sys.argv)

def server() -> int:
    """Handler for 'icedrive-blob-server'."""
    app = BlobApp()
    return app.main(sys.argv)
