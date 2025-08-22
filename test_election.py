"""
Test script for leader election functionality.
Testing automatic master failover and recovery.
"""

from utils import send_message  # Importing our socket communication utility
import time

def test_election():
    print("🧪 Testing Leader Election System")
    print("=" * 50)

    # Test 1: Normal operation
    print("1. Testing normal operation with master 5000...")
    response = send_message("localhost", 5000, {"cmd": "GET_NODES"})
    print("   Response:", "SUCCESS" if response else "FAILED")

    # Test 2: Kill master and wait for election
    print("\n2. Please kill the master server (Ctrl+C in terminal 1)")
    input("   Press Enter after killing master: ")

    print("   Waiting 5 seconds for election...")
    time.sleep(5)

    # Test 3: Try new master
    print("\n3. Testing new master (should be 5002)...")
    for port in [5002, 5001, 5000]:
        try:
            response = send_message("localhost", port, {"cmd": "GET_NODES"}, timeout=1)
            if response:
                print(f"   ✅ New master found on port {port}")
                break
        except:
            print(f"   ❌ Port {port} not responding")

    # Test 4: Client operations should work
    print("\n4. Testing client operations with new master...")
    try:
        from client import DFSClient
        client = DFSClient()
        data = client.download("important.txt")
        print("   ✅ Download successful with new master")
    except Exception as e:
        print(f"   ❌ Download failed: {e}")

if __name__ == "__main__":
    test_election()
