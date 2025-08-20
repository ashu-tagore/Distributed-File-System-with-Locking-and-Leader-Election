"""
Master server for distributed file system management.
Tracks file locations, storage nodes, and handles client requests.
"""
from typing import Dict, List, Set, Any
import time
from utils import send_message, start_server



class MasterServer:
    """Central coordinator for the distributed file system."""

    def __init__(self, port: int):
        """
        Initialize master server with empty file and node registries.

        Args:
            port: The port number this master server will listen on
        """
        self.port = port
        self.files: Dict[str, List[int]] = {}  # {filename: [node_port1, node_port2, ...]}
        self.nodes: Set[int] = set()           # {5001, 5002, ...} - set of active storage node ports

    def handle_message(self, message: Dict) -> Dict:
        """
        Process incoming messages and return appropriate responses.

        Args:
            message: Dictionary containing command and parameters

        Returns:
            Response dictionary with status or requested data
        """
        cmd = message.get("cmd")  # Extracting the command from message

        if cmd == "REGISTER_NODE":
            # Storage node wants to join the system
            node_port = message["node_port"]
            self.nodes.add(node_port)  # Adding node to active nodes set
            return {"status": "OK"}

        return {"error": "Invalid command"}  # Default response for unknown commands

    def start(self):
        """Start the master server and begin listening for incoming connections."""
        start_server(self.port, self.handle_message)  # Start TCP server with our message handler
        print(f"Master server running on port {self.port}")


if __name__ == "__main__":
    master = MasterServer(5000)  # Creating master server instance on port 5000
    master.start()               # Starting the server

    # Keeps server running until interrupted
    try:
        while True:
            time.sleep(1)  # Sleep to reduce CPU usage
    except KeyboardInterrupt:
        print("\nShutting down master server")  # Clean exit on Ctrl+C
