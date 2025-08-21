"""
Master server for distributed file system management.
Tracking file locations, storage nodes, handling client requests, and managing file locks.
"""

from utils import send_message, start_server
from typing import Dict, List, Set, Any
import time
import threading


class MasterServer:
    """Central coordinator for the distributed file system."""

    def __init__(self, port: int):
        """
        Initializing master server with empty file and node registries.

        Args:
            port: The port number this master server will listen on
        """
        self.port = port
        self.files: Dict[str, List[int]] = {}  # {filename: [node_port1, node_port2, ...]}
        self.nodes: Set[int] = set()           # {5001, 5002, ...} - set of active storage node ports
        self.locks: Dict[str, str] = {}        # {filename: client_id} - file locking system
        self.lock_timeouts: Dict[str, float] = {}  # {filename: timestamp} - lock expiration tracking

    def handle_message(self, message: Dict) -> Dict:
        """
        Processing incoming messages and returning appropriate responses.

        Args:
            message: Dictionary containing command and parameters

        Returns:
            Response dictionary with status or requested data
        """
        cmd = message.get("cmd")  # Extracting the command from message

        if cmd == "REGISTER_NODE":
            # Storage node wanting to join the system
            node_port = message["node_port"]
            self.nodes.add(node_port)  # Adding node to active nodes set
            return {"status": "OK"}

        elif cmd == "GET_NODES":
            # Client requesting list of available storage nodes
            return {"nodes": list(self.nodes)}  # Converting set to list for JSON serialization

        elif cmd == "ADD_FILE":
            # Client reporting where a file is stored
            filename = message["filename"]
            node_ports = message["node_ports"]  # List of nodes storing this file
            self.files[filename] = node_ports    # Updating file tracking
            return {"status": "FILE_REGISTERED"}

        elif cmd == "GET_FILE_NODES":
            # Client asking which nodes store a specific file
            filename = message["filename"]
            if filename in self.files:
                return {"nodes": self.files[filename]}  # Returning the list of nodes
            else:
                return {"error": "FILE_NOT_FOUND"}

        elif cmd == "LOCK":
            # Client requesting exclusive lock on a file
            filename = message["filename"]
            client_id = message["client_id"]

            # Checking if file is already locked by another client
            if filename in self.locks and self.locks[filename] != client_id:
                return {"status": "LOCK_DENIED"}

            # Granting lock to client with 30-second timeout
            self.locks[filename] = client_id
            self.lock_timeouts[filename] = time.time() + 30
            return {"status": "LOCK_ACQUIRED"}

        elif cmd == "UNLOCK":
            # Client releasing lock on a file
            filename = message["filename"]
            client_id = message["client_id"]

            # Verifying client owns the lock before releasing
            if filename in self.locks and self.locks[filename] == client_id:
                del self.locks[filename]
                del self.lock_timeouts[filename]
                return {"status": "UNLOCKED"}
            return {"status": "NOT_YOUR_LOCK"}

        return {"error": "Invalid command"}  # Default response for unknown commands

    def check_lock_timeouts(self):
        """Automatically cleaning up expired locks."""
        current_time = time.time()
        expired_locks = [
            filename for filename, expiry in self.lock_timeouts.items()
            if expiry < current_time
        ]
        for filename in expired_locks:
            if filename in self.locks:
                del self.locks[filename]
            if filename in self.lock_timeouts:
                del self.lock_timeouts[filename]

    def start(self):
        """Starting the master server and beginning listening for incoming connections."""
        start_server(self.port, self.handle_message)  # Starting TCP server with our message handler
        print(f"Master server running on port {self.port}")

        # Starting background thread for lock cleanup
        def lock_cleanup_loop():
            while True:
                time.sleep(5)  # Checking every 5 seconds
                self.check_lock_timeouts()

        threading.Thread(target=lock_cleanup_loop, daemon=True).start()


if __name__ == "__main__":
    """Main execution block - starting the master server."""
    master = MasterServer(5000)  # Creating master server instance on port 5000
    master.start()               # Starting the server

    # Keeping server running until interrupted
    try:
        while True:
            time.sleep(1)  # Sleeping to reduce CPU usage
    except KeyboardInterrupt:
        print("\nShutting down master server")  # Cleaning exit on Ctrl+C
