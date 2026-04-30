import hashlib
import bisect
from collections import defaultdict
from typing import List, Dict

class ConsistentHash:
    """
    Consistent Hashing with Virtual Nodes (VNodes)

    - Each server is mapped to multiple virtual nodes
    - Keys are hashed and mapped to the nearest vnode clockwise
    """

    def __init__(self, replicas: int = 100):
        self.replicas = replicas          # number of virtual nodes per server
        self.ring: List[int] = []         # sorted hash ring
        self.nodes: Dict[int, str] = {}   # hash -> server IP

    def _hash(self, key: str) -> int:
        """Generate a hash value using SHA-1"""
        return int(hashlib.sha1(key.encode()).hexdigest(), 16)

    def add_server(self, ip: str) -> None:
        """Add a server with multiple virtual nodes"""
        for i in range(self.replicas):
            vnode_key = f"{ip}#{i}"
            h = self._hash(vnode_key)

            self.ring.append(h)
            self.nodes[h] = ip

        self.ring.sort()

    def remove_server(self, ip: str) -> None:
        """Remove a server and all its virtual nodes"""
        to_remove = [h for h, server in self.nodes.items() if server == ip]

        for h in to_remove:
            self.ring.remove(h)
            del self.nodes[h]

    def get_server(self, key: str) -> str:
        """
        Find the server responsible for a given key
        using clockwise lookup
        """
        if not self.ring:
            return None

        h = self._hash(key)

        # Find insertion point
        idx = bisect.bisect_right(self.ring, h)

        # Wrap around if needed
        if idx == len(self.ring):
            idx = 0

        vnode_hash = self.ring[idx]
        return self.nodes[vnode_hash]


# -------------------------------
# Utility Functions
# -------------------------------

def get_distribution(ch: ConsistentHash, keys: List[str]) -> Dict[str, List[str]]:
    """Group keys by their assigned server"""
    distribution = defaultdict(list)

    for key in keys:
        server = ch.get_server(key)
        distribution[server].append(key)

    return distribution


def visualize_ring(ch: ConsistentHash, keys: List[str] = None, limit: int = 30) -> None:
    """
    Print a sorted view of the hash ring

    S(...) = server vnode
    K(...) = key position
    """
    print("\n=== RING VISUALIZATION ===")

    items = []

    # Add server vnodes
    for h in ch.ring:
        items.append((h, f"S({ch.nodes[h]})"))

    # Add keys
    if keys:
        for key in keys:
            h = ch._hash(key)
            items.append((h, f"K({key})"))

    # Sort all items by hash value
    items.sort(key=lambda x: x[0])

    # Print limited results
    for h, label in items[:limit]:
        print(f"{h} -> {label}")

def find_moved_keys(ch: ConsistentHash, keys: List[str], new_server: str):
    """
    Add a new server and detect which keys are moved
    """

    # 1. mapping BEFORE
    before = {k: ch.get_server(k) for k in keys}

    # 2. add new server
    ch.add_server(new_server)

    # 3. mapping AFTER
    after = {k: ch.get_server(k) for k in keys}

    print("\n=== AFTER ADD SERVER ===")
    for k, v in after.items():
        print(f"{k} -> {v}")

    # 4. find moved keys
    print("\n=== MOVED KEYS ===")
    moved = []

    for k in keys:
        if before[k] != after[k]:
            moved.append(k)
            print(f"{k}: {before[k]} → {after[k]}")

    print(f"\nTotal moved: {len(moved)} / {len(keys)}")

    print("\n=== CHECK CHANGE ===")
    for k in keys:
        print(k, ":", before[k], "→", after[k])

    return moved


def compare_distribution(ch, keys, new_server):
    """
    Compare distribution before and after adding a server
    """

    # BEFORE
    before_dist = get_distribution(ch, keys)

    print("\n=== BEFORE DISTRIBUTION ===")
    for server, ks in before_dist.items():
        print(f"{server}: {len(ks)} keys -> {ks}")

    # Add server
    ch.add_server(new_server)

    # AFTER
    after_dist = get_distribution(ch, keys)

    print("\n=== AFTER DISTRIBUTION ===")
    for server, ks in after_dist.items():
        print(f"{server}: {len(ks)} keys -> {ks}")

    # CHANGE PER SERVER
    print("\n=== CHANGES (ARRAY STYLE) ===")

    all_servers = set(before_dist.keys()) | set(after_dist.keys())

    for server in all_servers:
        before_keys = set(before_dist.get(server, []))
        after_keys = set(after_dist.get(server, []))

        added = list(after_keys - before_keys)
        removed = list(before_keys - after_keys)

        print(f"\n{server}:")
        print(f"  + added   -> {added}")
        print(f"  - removed -> {removed}")
# -------------------------------
# Main Execution
# -------------------------------

if __name__ == "__main__":
    ch = ConsistentHash(replicas=50)

    # Add servers
    ch.add_server("192.168.1.1")
    ch.add_server("192.168.1.2")
    ch.add_server("192.168.1.3")

    # Test keys
    keys = ["user1", "user2", "user3", "user4"]

    # BEFORE
    print("\n=== BEFORE DISTRIBUTION ===")
    before_dist = get_distribution(ch, keys)
    for server, ks in before_dist.items():
        print(f"{server}: {len(ks)} keys -> {ks}")

    # ADD SERVER (only once)
    new_server = "192.168.1.4"
    ch.add_server(new_server)

    # AFTER
    print("\n=== AFTER DISTRIBUTION ===")
    after_dist = get_distribution(ch, keys)
    for server, ks in after_dist.items():
        print(f"{server}: {len(ks)} keys -> {ks}")

    # COMPARE
    print("\n=== CHANGES (ARRAY STYLE) ===")

    all_servers = set(before_dist.keys()) | set(after_dist.keys())

    for server in all_servers:
        before_keys = set(before_dist.get(server, []))
        after_keys = set(after_dist.get(server, []))

        added = list(after_keys - before_keys)
        removed = list(before_keys - after_keys)

        print(f"\n{server}:")
        print(f"  + added   -> {added}")
        print(f"  - removed -> {removed}")