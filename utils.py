"""
Socket communication module for client-server messaging using pickle serialization.
"""

import socket     # Allows python to communicate over networks
import pickle     # Converts python objects like lists and dictionaries into bytes for sending over the network
import threading
from typing import Callable, Any


def send_message(host: str, port: int, message: Any) -> Any:
    """
    Send a message to a server and return the response.

    Args:
        host: The server hostname or IP address
        port: The server port number
        message: The message to send (any pickle-serializable object)

    Returns:
        The server's response, or None if connection failed
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.sendall(pickle.dumps(message)) #Converting the message to bytes
            return pickle.loads(s.recv(4096)) # Receiving the response and converting it back to a python object
    except ConnectionError:
        return None


def start_server(port: int, message_handler: Callable[[Any], Any]) -> None:
    """
    Start a server that listens for incoming connections.

    Args:
        port: The port number to listen on
        message_handler: A function that processes incoming messages and returns responses
    """
    def server_loop():
        """Main server loop that handles incoming connections."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('0.0.0.0', port))       # Listening on all interfaces
            s.listen()                     # Accepting connections
            while True:                    # Added Infinite loop to handle multiple clients
                conn, _ = s.accept()
                try:
                    data = conn.recv(4096) # Receiving data
                    response = message_handler(pickle.loads(data)) # Processes the message
                    conn.sendall(pickle.dumps(response))           # Sending the response back
                finally:
                    conn.close()                                   # Closing the connection

    thread = threading.Thread(target=server_loop, daemon=True)  # Server thread will exit when the main program exits
    thread.start()


if __name__ == "__main__":
    # Test echo server
    def echo_handler(msg):
        """Echo handler that returns the received message."""
        print(f"Server received: {msg}")
        return msg

    start_server(5000, echo_handler)
    print("Test message:", send_message("localhost", 5000, "Hello!"))
