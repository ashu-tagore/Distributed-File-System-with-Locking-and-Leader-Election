"""
Client implementation for distributed file system.
Handling file upload and download operations with master and storage nodes.
Supporting file locking for concurrent access control and master failover.
"""

import sys
import argparse
import time
import uuid
from typing import Dict, Any, Optional

from utils import send_message  # Importing our socket communication utility


class DFSClient:
    """Client for interacting with the distributed file system."""

    def __init__(self, master_port: int = 5000):
        """
        Initializing client with connection to master server.

        Args:
            master_port: The port of the master server (default: 5000)
        """
        self.master_port = master_port
        self.client_id = str(uuid.uuid4())  # Generating unique client ID for locking
        self.known_masters = [5000, 5001, 5002]  # All potential masters for failover

    def _try_masters(self, message: Dict, timeout: float = 2.0) -> Any:
        """
        Trying all known masters until one responds.

        Args:
            message: Message to send
            timeout: Connection timeout per master

        Returns:
            Response from first responsive master, or None
        """
        for master_port in self.known_masters:
            try:
                response = send_message("localhost", master_port, message, timeout)
                if response:
                    self.master_port = master_port  # Update current master
                    return response
            except Exception:
                continue  # Try next master
        return None

    def upload(self, filename: str, data: bytes) -> None:
        """
        Uploading a file to the distributed file system with replication.
        Using locking to prevent concurrent modifications.

        Args:
            filename: Name of the file to upload
            data: File content as bytes
        """
        # Acquiring exclusive lock before upload operation
        lock_response = self._try_masters({
            "cmd": "LOCK",
            "filename": filename,
            "client_id": self.client_id
        })

        if not lock_response or lock_response.get("status") != "LOCK_ACQUIRED":
            error_msg = lock_response.get('status') if lock_response else 'NO_RESPONSE'
            raise ConnectionError(f"Cannot upload: {error_msg}")

        print(f"🔒 Lock acquired for '{filename}'. Lock will be held for 10 seconds...")

        try:
            # ARTIFICIAL DELAY: Holding lock for 10 seconds for testing
            time.sleep(10)

            # Getting available storage nodes from master
            response = self._try_masters({
                "cmd": "GET_NODES"
            })

            if not response:
                raise ConnectionError("No master server available")

            nodes = response.get("nodes", [])
            if not nodes:
                raise ConnectionError("No storage nodes available")

            # Storing file on first 2 nodes for replication
            storage_nodes = nodes[:2]
            for node_port in storage_nodes:
                node_response = send_message("localhost", node_port, {
                    "cmd": "STORE_FILE",
                    "filename": filename,
                    "data": data
                })
                if not node_response or node_response.get("status") != "STORED":
                    raise ConnectionError(f"Failed to store file on node {node_port}")

            # Registering file location with master
            register_response = self._try_masters({
                "cmd": "ADD_FILE",
                "filename": filename,
                "node_ports": storage_nodes
            })

            if not register_response or register_response.get("status") != "FILE_REGISTERED":
                raise ConnectionError("Failed to register file with master")

            print(f"Uploaded '{filename}' to nodes {storage_nodes}")

        finally:
            # Always releasing lock, even if upload fails
            self._try_masters({
                "cmd": "UNLOCK",
                "filename": filename,
                "client_id": self.client_id
            })
            print("🔓 Lock released")

    def download(self, filename: str) -> bytes:
        """
        Downloading a file from the distributed file system.

        Args:
            filename: Name of the file to download

        Returns:
            File content as bytes
        """
        # Getting nodes that store this file
        response = self._try_masters({
            "cmd": "GET_FILE_NODES",
            "filename": filename
        })

        if not response:
            raise ConnectionError("No master server available")

        if "error" in response:
            raise FileNotFoundError(f"File '{filename}' not found")

        nodes = response.get("nodes", [])

        # Trying each node until successful
        for node_port in nodes:
            node_response = send_message("localhost", node_port, {
                "cmd": "GET_FILE",
                "filename": filename
            })
            if node_response and "data" in node_response:
                return node_response["data"]

        raise FileNotFoundError(f"File '{filename}' not available on any node")


if __name__ == "__main__":
    """Main execution block - handling command line operations."""
    parser = argparse.ArgumentParser(description="Distributed File System Client")
    parser.add_argument("operation", choices=["upload", "download"], help="Operation to perform")
    parser.add_argument("filename", help="Name of the file")

    args = parser.parse_args()

    client = DFSClient()

    try:
        if args.operation == "upload":
            # Reading file and uploading
            with open(args.filename, "rb") as f:
                file_data = f.read()
            client.upload(args.filename, file_data)
            print(f"Successfully uploaded {args.filename}")

        elif args.operation == "download":
            # Downloading and saving file
            downloaded_data = client.download(args.filename)
            with open(args.filename, "wb") as f:
                f.write(downloaded_data)
            print(f"Successfully downloaded {args.filename}")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
