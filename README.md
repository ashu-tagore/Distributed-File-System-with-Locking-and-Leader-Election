# Distributed File System (DFS) with Leader Election

A Python-based distributed file system implementation demonstrating core distributed systems concepts including replication, distributed locking, and automatic leader election using the Bully algorithm.

## 🎯 Project Overview

This project implements a fully functional distributed file system that provides:
- **File storage and retrieval** across multiple nodes
- **Automatic replication** for fault tolerance
- **Distributed locking** for concurrent access control
- **Leader election** for automatic failover
- **Client-side failover** for seamless operation during master failures

## 🏗️ System Architecture

### Components
1. **Master Server** (`master_server.py`)
   - Central metadata management
   - Lock coordination
   - Node registration and health monitoring
   - Can step down during leader election

2. **Storage Nodes** (`storage_node.py`)
   - Actual file storage (5001, 5002 ports)
   - Participate in leader election
   - Can become master if elected
   - Heartbeat monitoring

3. **Client** (`client.py`)
   - File upload/download operations
   - Automatic master discovery and failover
   - Lock acquisition/release

4. **Utilities** (`utils.py`)
   - Network communication layer
   - Message serialization/deserialization
   - Socket management with timeout support

## ⚡ Key Features Implemented

### 🔄 Replication
- Files automatically stored on 2+ nodes
- Client retrieves from first available node
- Survives single node failures

### 🔒 Distributed Locking
- Exclusive write locks prevent concurrent modifications
- **10-second artificial delay** for testing concurrent access scenarios
- **30-second automatic timeout** on master server prevents deadlocks (safety net)
- Read operations don't require locks (concurrent reads allowed)

### 👑 Leader Election (Bully Algorithm)
- Automatic detection of master failures
- Nodes with higher port numbers have priority (5002 > 5001 > 5000)
- Coordinated election process with timeout handling
- Seamless client failover to new master

### 🛡️ Fault Tolerance
- Continues operation during node failures
- Automatic recovery after node restarts
- No single point of failure
- Heartbeat monitoring every 3 seconds

## 🚀 Getting Started

### Prerequisites
- Python 3.6+
- No external dependencies (pure Python implementation)

### Installation
```bash
git clone <your-repo-url>
cd "Distributed File System"
```

### Running the System

**Terminal 1 - Master Server:**
```bash
python master_server.py
```

**Terminal 2 - Storage Node 1:**
```bash
python storage_node.py 5001
```

**Terminal 3 - Storage Node 2:**
```bash
python storage_node.py 5002
```

**Terminal 4 - Client Operations:**
```bash
# Create test file
echo "Hello Distributed World!" > test.txt

# Upload file
python client.py upload test.txt

# Download file
python client.py download test.txt
```

## 🧪 Testing Scenarios

### 1. ✅ Basic File Operations
```bash
# Upload and download
python client.py upload important.txt
python client.py download important.txt
```

### 2. ✅ Fault Tolerance Test
```bash
# Kill a storage node (Ctrl+C in Terminal 2 or 3)
# System continues working with remaining node
python client.py download important.txt
```

### 3. ✅ Leader Election Test
```bash
# Kill master server (Ctrl+C in Terminal 1)
# Wait 5 seconds for automatic election
# Watch node 5002 become new master (highest port)
# Client operations continue automatically
python client.py download important.txt
```

### 4. ✅ Concurrent Access Test
```bash
# Terminal 4 - Client 1 (holds lock for 10 seconds)
python client.py upload test.txt

# Terminal 5 - Client 2 (immediately tries same file - gets LOCK_DENIED)
python client.py upload test.txt
```

### 5. ✅ Network Partition Simulation
```bash
# Kill all nodes, then restart in different order
# System automatically recovers and elects new leader
```

## 📊 Technical Implementation

### Algorithm Details
- **Bully Algorithm**: Highest port number wins election (5002 > 5001 > 5000)
- **Heartbeat**: 3-second intervals for failure detection
- **Lock Timeout**: 30-second automatic release (safety)
- **Election Timeout**: 5-second wait before initiating election
- **Socket Timeout**: 2-second default for network operations

## 📝 Project Structure

```
Distributed File System/
├── master_server.py     # Primary coordinator with election support
├── storage_node.py      # Storage nodes with master capability  
├── client.py           # User interface with failover support
├── utils.py            # Network communication utilities
├── test_master.py      # Master server testing
├── test_storage.py     # Storage node testing
├── test_election.py    # Leader election testing
└── README.md          # This file
```

## 👨‍💻 Author

**Ashutosh** - Distributed Systems Course Project  
*Demonstrating comprehensive understanding of distributed systems principles*

## 📄 License

This project is for educational purposes as part of academic coursework.
