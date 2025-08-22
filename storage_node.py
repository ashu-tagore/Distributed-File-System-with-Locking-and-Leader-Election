"""
Storage node implementation for distributed file system.
Handling file storage operations and communicating with master server.
Supporting leader election using bully algorithm.
"""

from utils import send_message, start_server  # Importing our socket communication utilities
from typing import Dict, Any
import time
import sys
import threading


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
        self.is_master_candidate = True      # This node can become master
        self.current_master = master_port    # Current master server port
        self.heartbeat_interval = 3          # Seconds between heartbeat checks
        self.election_timeout = 5            # Seconds to wait before election

        # Register with master and start services
        self.register_with_master()
        self.start_services()

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

        elif cmd == "ELECTION":
            # Election message from another node
            sender_port = message["sender_port"]
            if sender_port < self.node_port:
                # I have higher priority, respond
                return {"response": "OK", "port": self.node_port}
            return {"response": "IGNORE"}

        elif cmd == "COORDINATOR":
            # New master announced
            new_master_port = message["new_master_port"]
            self.current_master = new_master_port
            print(f"🔄 New master elected: {new_master_port}")
            return {"status": "ACKNOWLEDGED"}

        elif cmd == "HEARTBEAT_CHECK":
            # Check if I'm alive
            return {"status": "ALIVE"}

        return {"error": "Invalid command"}  # Default response for unknown commands

    def register_with_master(self):
        """Registering this storage node with the master server."""
        try:
            response = send_message("localhost", self.master_port, {
                "cmd": "REGISTER_NODE",
                "node_port": self.node_port
            })
            if response and response.get("status") == "OK":
                print("✅ Successfully registered with master server")
            else:
                print("❌ Failed to register with master server")
        except:
            print("❌ Master server unavailable for registration")

    def check_master_heartbeat(self):
        """Checking if master is alive."""
        try:
            response = send_message("localhost", self.current_master, {
                "cmd": "HEARTBEAT_CHECK"
            }, timeout=2)
            return response is not None
        except:
            return False

    def start_election_if_needed(self):
        """Starting election if master is unresponsive."""
        if not self.check_master_heartbeat():
            print("⚠️ Master not responding, initiating election...")
            self.initiate_election()

    def initiate_election(self):
        """Starting bully algorithm election."""
        print("🚨 Starting leader election...")
        highest_port = self.node_port

        # Contact all higher-priority nodes
        for port in [5000, 5001, 5002]:  # All potential masters
            if port > self.node_port:
                try:
                    response = send_message("localhost", port, {
                        "cmd": "ELECTION",
                        "sender_port": self.node_port
                    }, timeout=1)
                    if response and response.get("response") == "OK":
                        highest_port = max(highest_port, response["port"])
                except:
                    continue  # Node is down

        # If I have the highest port, I become coordinator
        if highest_port == self.node_port:
            self.become_coordinator()
        else:
            self.current_master = highest_port
            print(f"🔄 New master elected: {highest_port}")

    def become_coordinator(self):
        """Becoming the new coordinator/master."""
        print(f"🎯 Node {self.node_port} is now the primary master!")
        # Announce to all nodes
        for port in [5000, 5001, 5002]:
            if port != self.node_port:
                try:
                    send_message("localhost", port, {
                        "cmd": "COORDINATOR",
                        "new_master_port": self.node_port
                    })
                except:
                    pass  # Node might be down

    def start_services(self):
        """Starting all node services."""
        # Start the server
        start_server(self.node_port, self.handle_message)
        print(f"✅ Storage node running on port {self.node_port}")

        # Start heartbeat monitoring
        def heartbeat_monitor():
            while True:
                time.sleep(self.heartbeat_interval)
                self.start_election_if_needed()

        threading.Thread(target=heartbeat_monitor, daemon=True).start()


if __name__ == "__main__":
    """Main execution block - starting storage node."""
    if len(sys.argv) != 2:
        print("Usage: python storage_node.py <node_port>")
        sys.exit(1)

    node_port = int(sys.argv[1])  # Getting node port from command line argument
    master_port = 5000  # Initial master server port

    node = StorageNode(node_port, master_port)

    # Keeping node running until interrupted
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\nShutting down storage node on port {node_port}")
