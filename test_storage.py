"""
Test script for storage node operations.
Testing file storage and retrieval functionality.
"""

from utils import send_message

# Test storing a file
print("1. Storing file 'test.txt' on storage node...")
response = send_message("localhost", 5001, {
    "cmd": "STORE_FILE",
    "filename": "test.txt",
    "data": b"Hello World!"
})
print("Store response:", response)

# Test retrieving the file
print("\n2. Retrieving file 'test.txt' from storage node...")
response = send_message("localhost", 5001, {
    "cmd": "GET_FILE",
    "filename": "test.txt"
})
print("Get response:", response)

# Test retrieving non-existent file
print("\n3. Trying to retrieve non-existent file...")
response = send_message("localhost", 5001, {
    "cmd": "GET_FILE",
    "filename": "unknown.txt"
})
print("Get response:", response)
