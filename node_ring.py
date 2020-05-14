import hashlib

from server_config import NODES
import mmh3

def weight(node, key):
    hash = mm3.hash(key)
    return pow(node,hash),% pow(2,31)

class NodeRing(object):

    def __init__(self, nodes):
        assert len(nodes) > 0
        self.nodes = nodes
    
    def get_node(self, key_hex):
        key = int(key_hex, 16)
        node_index = hash(self, key)
        return self.nodes[node_index]

    def add_node(self, node):
        if node not in self.nodes:
            self.nodes.append(node)

    def remove_node(self, node):
        if node in self.nodes:
            self.nodes.remove(node)
        else:
            raise ValueError("No such node %s to remove" % (node))
    
    def hash(self, key):
        weights = []
        for node in self.nodes:
            n = node
            w = weight(n, key)
            weights.append((w,node))
        node = max(weights)
        return node

class VirtualNodeRing(object):
    def __init__(self, nodes, replicas=2):

        def NodeRingConstructor():
            return  NodeRing()

        self.replicas = replicas
        self.nodes = {}
        self.zones = []
        self.zone_members = defaultdict(list)
        self.rings = defaultdict(NodeRingConstructor)


    def add_zone(self, zone):
        if zone not in self.zones:
            self.zones.append(zone)
            self.zones = sorted(self.zones)

    def remove_zone(self, zone):
        if zone in self.zones:
            self.zones.remove(zone)
            for member in self.zone_members[zone]:
                self.nodes.remove(member)
            self.zones = sorted(self.zones)
            del self.rings[zone]
            del self.zone_members[zone]
        else:
            raise ValueError("No such zone %s to remove" % (zone))

    def add_node(self, node_id, node_zone=None, node_name=None):
        if node_id in self.nodes.keys():
            raise ValueError('Node with name %s already exists', node_id)
        self.add_zone(node_zone)
        self.rings[node_zone].add_node(node_id)
        self.nodes[node_id] = node_name
        self.zone_members[node_zone].append(node_id)

    def remove_node(self, node_id, node_name=None, node_zone=None):
        self.rings[node_zone].remove_node(node_id)
        del self.nodes[node_id]
        self.zone_members[node_zone].remove(node_id)
        if len(self.zone_members[node_zone]) == 0:
            self.remove_zone(node_zone)

    def node_name(self, node_id):
        return self.nodes.get(node_id, None)

    def find_nodes(self, key, offset=None):
        nodes = []
        if offset is None:
            offset = sum(ord(char) for char in key) % len(self.zones)
        for i in range(self.replicas):
            zone = self.zones[(i + offset) % len(self.zones)]
            ring = self.rings[zone]
            nodes.append(ring.find_node(key))
        return nodes

def test():
    ring = NodeRing(nodes=NODES)
    node = ring.get_node('9ad5794ec94345c4873c4e591788743a')
    print(node)
    print(ring.get_node('ed9440c442632621b608521b3f2650b8'))


# Uncomment to run the above local test via: python3 node_ring.py
# test()
