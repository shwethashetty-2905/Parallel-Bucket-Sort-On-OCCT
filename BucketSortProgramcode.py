import multiprocessing
import random
import matplotlib.pyplot as plt
from math import ceil


class OCCTNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.bucket = []

    def receive_data(self, data):
        self.bucket = data

    def sort_bucket(self):
        print(f"Node {self.node_id} sorting: {self.bucket}")
        self.bucket.sort()
        return self.bucket


def simulate_occt_network(data, num_nodes):
    print("\n--- Initializing OCCT Nodes ---")
    min_val, max_val = min(data), max(data)
    range_size = (max_val - min_val + 1) / num_nodes

    nodes = [OCCTNode(i) for i in range(num_nodes)]
    buckets = [[] for _ in range(num_nodes)]

    # Distribute data into buckets (simulate inter-node communication)
    for num in data:
        index = min(num_nodes - 1, int((num - min_val) / range_size))
        buckets[index].append(num)

    # Assign buckets to nodes
    for i in range(num_nodes):
        nodes[i].receive_data(buckets[i])

    return nodes, buckets


def node_sort(node, return_dict):
    sorted_bucket = node.sort_bucket()
    return_dict[node.node_id] = sorted_bucket


def parallel_sort_on_occt(nodes):
    manager = multiprocessing.Manager()
    return_dict = manager.dict()

    processes = []
    for node in nodes:
        p = multiprocessing.Process(target=node_sort, args=(node, return_dict))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    sorted_data = []
    for i in range(len(nodes)):
        sorted_data.extend(return_dict[i])

    return sorted_data


def visualize(data, buckets, sorted_data):
    fig, axs = plt.subplots(3, 1, figsize=(10, 8))

    axs[0].bar(range(len(data)), data)
    axs[0].set_title('Original Unsorted Data')
    axs[0].set_xlabel('Index')
    axs[0].set_ylabel('Value')
    
    
    bucket_labels = [f"Node {i}" for i in range(len(buckets))]
    bucket_lengths = [len(b) for b in buckets]
    axs[1].bar(bucket_labels, bucket_lengths)
    axs[1].set_title('Data Distribution Across OCCT Nodes (Buckets)')
    axs[1].set_xlabel('Node')
    axs[1].set_ylabel('Number of element')


    axs[2].bar(range(len(sorted_data)), sorted_data, color='green')
    axs[2].set_title('Final Sorted Data')
    axs[2].set_xlabel('Index')
    axs[2].set_ylabel('Value')
    

    plt.tight_layout()
    plt.show()


def main():
    data = [random.randint(0, 100) for _ in range(40)]
    num_nodes = 5

    print("Unsorted Data:", data)
    nodes, buckets = simulate_occt_network(data, num_nodes)
    sorted_data = parallel_sort_on_occt(nodes)
    print("\nSorted Data:", sorted_data)

    visualize(data, buckets, sorted_data)


if __name__ == "__main__":
    main()
