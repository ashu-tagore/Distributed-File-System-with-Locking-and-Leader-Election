"""
Client implementation for distributed file system.
Handling file upload and download operations with master and storage nodes.
Supporting file locking for concurrent access control.
"""

from utils import send_message
from typing import Optional
import uuid
import sys
import argparse
import time  # Added for artificial delay


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

    def upload(self, filename: str, data: bytes) -> None:
        """
        Uploading a file to the distributed file system with replication.
        Using locking to prevent concurrent modifications.

        Args:
            filename: Name of the file to upload
            data: File content as bytes
        """
        # Acquiring exclusive lock before upload operation
        lock_response = self._acquire_lock(filename)
        if lock_response != "LOCK_ACQUIRED":
            raise Exception(f"Cannot upload: {lock_response}")

        print(f"🔒 Lock acquired for '{filename}'. Lock will be held for 10 seconds...")

        try:
            # ARTIFICIAL DELAY: Holding lock for 10 seconds for testing
            time.sleep(10)

            # Getting available storage nodes from master
            response = send_message("localhost", self.master_port, {
                "cmd": "GET_NODES"
            })

            if not response:
                raise Exception("Master server unavailable")

            nodes = response.get("nodes", [])  # Using .get() with default to avoid KeyError
            if not nodes:
                raise Exception("No storage nodes available")

            # Storing file on first 2 nodes for replication
            storage_nodes = nodes[:2]
            for node_port in storage_nodes:
                response = send_message("localhost", node_port, {
                    "cmd": "STORE_FILE",
                    "filename": filename,
                    "data": data
                })
                if not response or response.get("status") != "STORED":
                    raise Exception(f"Failed to store file on node {node_port}")

            # Registering file location with master
            response = send_message("localhost", self.master_port, {
                "cmd": "ADD_FILE",
                "filename": filename,
                "node_ports": storage_nodes
            })

            if not response or response.get("status") != "FILE_REGISTERED":
                raise Exception("Failed to register file with master")

            print(f"Uploaded '{filename}' to nodes {storage_nodes}")

        finally:
            # Always releasing lock, even if upload fails
            self._release_lock(filename)
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
        response = send_message("localhost", self.master_port, {
            "cmd": "GET_FILE_NODES",
            "filename": filename
        })

        if not response:
            raise Exception("Master server unavailable")

        if "error" in response:
            raise FileNotFoundError(f"File '{filename}' not found")

        nodes = response.get("nodes", [])

        # Trying each node until successful
        for node_port in nodes:
            response = send_message("localhost", node_port, {
                "cmd": "GET_FILE",
                "filename": filename
            })
            if response and "data" in response:
                return response["data"]

        raise FileNotFoundError(f"File '{filename}' not available on any node")

    def _acquire_lock(self, filename: str) -> str:
        """
        Acquiring exclusive lock for a file from master server.

        Args:
            filename: Name of the file to lock

        Returns:
            Lock acquisition status
        """
        response = send_message("localhost", self.master_port, {
            "cmd": "LOCK",
            "filename": filename,
            "client_id": self.client_id
        })
        # FIXED: Proper error handling for lock response
        if not response:
            return "LOCK_DENIED"  # No response from master
        if "status" in response:
            return response["status"]  # Valid response with status
        return "LOCK_DENIED"  # Response without status field

    def _release_lock(self, filename: str) -> None:
        """
        Releasing lock on a file.

        Args:
            filename: Name of the file to unlock
        """
        send_message("localhost", self.master_port, {
            "cmd": "UNLOCK",
            "filename": filename,
            "client_id": self.client_id
        })


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
                data = f.read()
            client.upload(args.filename, data)
            print(f"Successfully uploaded {args.filename}")

        elif args.operation == "download":
            # Downloading and saving file
            data = client.download(args.filename)
            with open(args.filename, "wb") as f:
                f.write(data)
            print(f"Successfully downloaded {args.filename}")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)