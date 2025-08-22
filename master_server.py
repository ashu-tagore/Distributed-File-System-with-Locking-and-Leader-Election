"""
Master server for distributed file system management.
Tracking file locations, storage nodes, handling client requests, and managing file locks.
Supporting leader election using bully algorithm.
"""

from utils import send_message, start_server  # Importing our socket communication utilities
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
        self.files: Dict[str, List[int]] = {}  # {filename: [node_port1, node_port2, ...]} - tracks file locations
        self.nodes: Set[int] = set()           # {5001, 5002,...} - set of active storage node ports
        self.locks: Dict[str, str] = {}        # {filename: client_id} - file locking system
        self.lock_timeouts: Dict[str, float] = {} # {filename: timestamp} - lock expiration tracking
        self.is_primary = True                 # This instance is the primary master
        self.backup_ports = [5001, 5002]       # Ports that can become masters
        self.heartbeat_interval = 3            # Seconds between heartbeat checks

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

        elif cmd == "ELECTION":
            # Another node is starting an election
            if message["sender_port"] < self.port:
                # I have higher priority, so I respond
                return {"response": "OK", "port": self.port}
            return {"response": "IGNORE"}

        elif cmd == "COORDINATOR":
            # A new master is announcing itself
            new_master_port = message["new_master_port"]
            if new_master_port > self.port:
                # New master has higher priority, step down
                self.is_primary = False
                print(f"🔄 Stepping down, new master: {new_master_port}")
            return {"status": "ACKNOWLEDGED"}

        elif cmd == "HEARTBEAT_CHECK":
            # Check if master is alive
            return {"status": "ALIVE"}

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

    def check_backup_nodes(self):
        """Checking if backup nodes are alive."""
        for port in self.backup_ports:
            try:
                response = send_message("localhost", port, {
                    "cmd": "HEARTBEAT_CHECK"
                }, timeout=1)
                if response:
                    print(f"✅ Backup node {port} is alive")
            except:
                print(f"❌ Backup node {port} is offline")

    def start(self):
        """Starting the master server and beginning listening for incoming connections."""
        start_server(self.port, self.handle_message)  # Starting TCP server with our message handler
        print(f"🎯 Primary master server running on port {self.port}")

        # Starting background thread for lock cleanup
        def lock_cleanup_loop():
            while True:
                time.sleep(5)
                self.check_lock_timeouts()

        threading.Thread(target=lock_cleanup_loop, daemon=True).start()

        # Starting background thread for node monitoring
        def node_monitor_loop():
            while True:
                time.sleep(self.heartbeat_interval)
                if self.is_primary:
                    self.check_backup_nodes()

        threading.Thread(target=node_monitor_loop, daemon=True).start()


if __name__ == "__main__":
    """Main execution block - starting the master server."""
    master = MasterServer(5000)  # Creating master server instance on port 5000
    master.start()               # Starting the server

    # Keeping server running until interrupted
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down master server")
