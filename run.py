import logging
import raft

logging.basicConfig(level=logging.INFO)


nodes = {i: raft.Node(i) for i in range(5)}
raft.nodes = nodes

for i, n in nodes.items():
    print(i, n.role)
nodes[0].promote()
for i, n in nodes.items():
    print(i, n.role)
for i, n in nodes.items():
    print(i)
    n.broadcast(raft.BroadcastMessage({"node": i}))
for i, n in nodes.items():
    print("Node", i, "has log with entries", *(entry.message for entry in n.log))
