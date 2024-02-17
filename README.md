# Seed and Peer Node
This project simply implements a peer-to-peer network using seed and peer nodes.

## Seed Node
Seed nodes are special nodes in a peer-to-peer network that act as initial contact points for other nodes to join the network. They provide a list of known active nodes to new nodes, allowing them to bootstrap and establish connections with other peers in the network.

## Peer Node
Peer nodes are the nodes that are connected to the network and are not seed nodes. They are the nodes that store and retrieve data from the network.

## Seed Node and Peer Node Working
When a peer node starts, it connects to n/2 + 1 seed node to get the network's list of active peer nodes. It then connects to the active peer nodes and starts the process of data storage and retrieval.

## Seed Node and Peer Node Communication
The seed node and peer node communicate using the following messages:
- `Get Peer List`: The peer node sends this message to the seed node to get the list of active peer nodes in the network.
- `Peer List`: The seed node sends this message to the peer node containing the list of active peer nodes in the network.

## How to see the Message List and Peer List
- To see the list of messages and peer nodes, you can enter the following commands in the terminal where the peer node is running:
  - `1` to see the list of active peer nodes in the network connected to the current peer node.
  - `2` to see the list of messages sent and received by the peer nodes.

## Peer Node and Peer Node Communication
The peer node and peer node communicate using the following messages:
- `Peer Request`: The peer node sends this message to another peer node to connect.
- `Peer Reply`: The peer node sends this message to the peer node to accept the connection request.
- `Liveliness`: The peer node sends this message to the peer node to check if it is alive.
- `Liveliness Reply`: The peer node sends this message to the peer node to reply to the liveliness message.
- `getData`: The peer node sends this message to the peer node to get the data.
- `Message`: The peer node sends this message to the peer node to send the data.
- `Death`: The peer node sends this message to the seed node to close the connection.

## How to Run?
- Run '`pip install -r requirements.txt` to install the required packages.
- Run the seed node using the command `python3 seed.py`
- Run the peer node using the command `python3 peer.py`
- Run the second command to run 5 peer nodes (you can change it as per requirements).
- After closing the peer and seed nodes you can run `python3 clean.py` to remove the output files and the bin folder.


## Stopping the Peer Node
- To stop the peer node, press `Ctrl + C` in the terminal where the peer node is running.

## Output Files
- Two output directories will be created `servers` and `clients` in `bin` folder containing the logs of the seed node and peer nodes respectively.
- The logs will contain the messages sent and received by the seed node and peer nodes.
- It will also contain the liveliness check and the close connections details.
- You can differentiate the logs based on colors of logs.

## Contributors
- [Sameer Sharma (B21CS066)](https://github.com/SameerSharma-57)
- [Shalin Jain (B21CS070)](https://github.com/Shalin06)
