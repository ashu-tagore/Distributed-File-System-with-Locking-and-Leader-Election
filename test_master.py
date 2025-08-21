from utils import send_message

def test_master():
    # Test 1: Register a node
    print("1. Registering node 5001...")
    response = send_message("localhost", 5000, {
        "cmd": "REGISTER_NODE",
        "node_port": 5001
    })
    print("Response:", response)

    # Test 2: Get available nodes
    print("\n2. Getting available nodes...")
    response = send_message("localhost", 5000, {
        "cmd": "GET_NODES"
    })
    print("Response:", response)

    # Test 3: Register a file location
    print("\n3. Registering file 'test.txt' on nodes [5001, 5002]...")
    response = send_message("localhost", 5000, {
        "cmd": "ADD_FILE",
        "filename": "test.txt",
        "node_ports": [5001, 5002]
    })
    print("Response:", response)

    # Test 4: Get file locations
    print("\n4. Getting nodes for 'test.txt'...")
    response = send_message("localhost", 5000, {
        "cmd": "GET_FILE_NODES",
        "filename": "test.txt"
    })
    print("Response:", response)

    # Test 5: Try to get non-existent file
    print("\n5. Getting nodes for non-existent file...")
    response = send_message("localhost", 5000, {
        "cmd": "GET_FILE_NODES",
        "filename": "unknown.txt"
    })
    print("Response:", response)

if __name__ == "__main__":
    test_master()