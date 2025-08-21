"""
Storage node implementation for distributed file system.
Handling file storage operations and communicating with master server.
"""

from utils import send_message, start_server
from typing import Dict, Any
import time
import sys


class StorageNode:
    """Storage node handling file storage and retrieval operations."""

    def __init__(self, node_port: int, master_port: int):
        """
        Initializing storage node with connection to master server.

        Args:
            node_port: The port this storage node will listen on
            master_port: The port of the master server
        """
        self.node_port = node_port
        self.master_port = master_port
        self.storage: Dict[str, bytes] = {}  # {filename: file_data} - local file storage

    def handle_message(self, message: Dict) -> Dict:
        """
        Processing incoming file operation requests.

        Args:
            message: Dictionary containing command and file information

        Returns:
            Response with operation status or file data
        """
        cmd = message.get("cmd")  # Extracting the command from message

        if cmd == "STORE_FILE":
            # Client requesting to store a file on this node
            filename = message["filename"]
            data = message["data"]
            self.storage[filename] = data  # Storing file data locally
            return {"status": "STORED"}

        elif cmd == "GET_FILE":
            # Client requesting to retrieve a file from this node
            filename = message["filename"]
            if filename in self.storage:
                return {"data": self.storage[filename]}  # Returning file data if found
            else:
                return {"error": "FILE_NOT_FOUND"}

        return {"error": "Invalid command"}  # Default response for unknown commands

    def register_with_master(self):
        """Registering this storage node with the master server."""
        response = send_message("localhost", self.master_port, {
            "cmd": "REGISTER_NODE",
            "node_port": self.node_port
        })
        if response and response.get("status") == "OK":
            print(f"Successfully registered with master server")
        else:
            print(f"Failed to register with master server")

    def start(self):
        """Starting the storage node server."""
        start_server(self.node_port, self.handle_message)  # Starting storage node server
        self.register_with_master()  # Registering with master
        print(f"Storage node running on port {self.node_port}")


if __name__ == "__main__":
    """Main execution block - starting storage node."""
    if len(sys.argv) != 2:
        print("Usage: python storage_node.py <node_port>")
        sys.exit(1)

    node_port = int(sys.argv[1])  # Getting node port from command line argument
    master_port = 5000  # Master server port (fixed)

    node = StorageNode(node_port, master_port)
    node.start()

    # Keeping node running until interrupted
    try:
        while True:
            time.sleep(1)  # Sleeping to reduce CPU usage
    except KeyboardInterrupt:
        print(f"\nShutting down storage node on port {node_port}")
