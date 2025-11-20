#!/usr/bin/env python3
"""
Graph Generator
Generates a graph file with a specified number of edges in the format:
source_node|target_node|edge_label
"""

import random
import argparse
import sys
from typing import Set, Tuple

def generate_graph(num_edges: int, output_file: str, max_nodes: int = None, include_header: bool = True):
    """
    Generate a graph with specified number of edges.
    
    Args:
        num_edges: Number of edges to generate
        output_file: Output file path
        max_nodes: Maximum number of nodes (if None, will be calculated based on edges)
        include_header: Whether to include header row
    """
    
    # Define possible edge labels
    edge_labels = ["friendship", "follows", "likes", "shares", "mentions", "collaborates", "works_with"]
    
    # Calculate reasonable number of nodes if not specified
    if max_nodes is None:
        # Use a formula to get reasonable node count based on edges
        # For sparse graphs: nodes ≈ sqrt(2 * edges)
        # For denser graphs: nodes ≈ edges / 3
        max_nodes = max(10, min(int((2 * num_edges) ** 0.5), num_edges // 3))
    
    print(f"Generating graph with {num_edges} edges and up to {max_nodes} nodes...")
    
    # Set to track generated edges to avoid duplicates
    generated_edges: Set[Tuple[int, int]] = set()
    
    with open(output_file, 'w') as f:
        # Write header if requested
        if include_header:
            f.write("source_node|target_node|edge_label\n")
        
        edges_written = 0
        attempts = 0
        max_attempts = num_edges * 10  # Prevent infinite loops
        
        while edges_written < num_edges and attempts < max_attempts:
            attempts += 1
            
            # Generate random source and target nodes
            source = random.randint(1, max_nodes)
            target = random.randint(1, max_nodes)
            
            # Avoid self-loops
            if source == target:
                continue
            
            # Avoid duplicate edges (considering undirected nature)
            edge_tuple = (min(source, target), max(source, target))
            if edge_tuple in generated_edges:
                continue
            
            # Add edge to set
            generated_edges.add(edge_tuple)
            
            # Choose random edge label
            label = random.choice(edge_labels)
            
            # Write edge to file
            f.write(f"{source}|{target}|{label}\n")
            edges_written += 1
            
            # Progress indicator for large graphs
            if edges_written % 10000 == 0:
                print(f"Generated {edges_written} edges...")
    
    print(f"Graph generation complete! Generated {edges_written} edges in {output_file}")
    
    # Print some statistics
    unique_nodes = set()
    for source, target in generated_edges:
        unique_nodes.add(source)
        unique_nodes.add(target)
    
    print(f"Statistics:")
    print(f"  - Total edges: {edges_written}")
    print(f"  - Unique nodes: {len(unique_nodes)}")
    print(f"  - Density: {edges_written / (len(unique_nodes) * (len(unique_nodes) - 1) / 2):.4f}")

def generate_connected_graph(num_edges: int, output_file: str, max_nodes: int = None, include_header: bool = True):
    """
    Generate a connected graph with specified number of edges.
    Ensures the graph is connected by first creating a spanning tree.
    
    Args:
        num_edges: Number of edges to generate
        output_file: Output file path
        max_nodes: Maximum number of nodes
        include_header: Whether to include header row
    """
    
    edge_labels = ["friendship", "follows", "likes", "shares", "mentions", "collaborates", "works_with"]
    
    if max_nodes is None:
        max_nodes = max(10, min(int((2 * num_edges) ** 0.5), num_edges // 3))
    
    # Ensure we have enough edges to connect the graph
    if num_edges < max_nodes - 1:
        print(f"Warning: {num_edges} edges is not enough to connect {max_nodes} nodes.")
        print(f"Minimum edges needed for connectivity: {max_nodes - 1}")
        max_nodes = num_edges + 1
    
    print(f"Generating connected graph with {num_edges} edges and {max_nodes} nodes...")
    
    generated_edges: Set[Tuple[int, int]] = set()
    
    # First, create a spanning tree to ensure connectivity
    for i in range(2, max_nodes + 1):
        parent = random.randint(1, i - 1)
        edge_tuple = (min(parent, i), max(parent, i))
        generated_edges.add(edge_tuple)
    
    print(f"Created spanning tree with {len(generated_edges)} edges for connectivity")
    
    # Now add remaining edges randomly
    attempts = 0
    max_attempts = (num_edges - len(generated_edges)) * 10
    
    while len(generated_edges) < num_edges and attempts < max_attempts:
        attempts += 1
        
        source = random.randint(1, max_nodes)
        target = random.randint(1, max_nodes)
        
        if source == target:
            continue
        
        edge_tuple = (min(source, target), max(source, target))
        if edge_tuple not in generated_edges:
            generated_edges.add(edge_tuple)
    
    # Write to file
    with open(output_file, 'w') as f:
        if include_header:
            f.write("source_node|target_node|edge_label\n")
        
        for i, (source, target) in enumerate(generated_edges):
            label = random.choice(edge_labels)
            f.write(f"{source}|{target}|{label}\n")
            
            if (i + 1) % 10000 == 0:
                print(f"Written {i + 1} edges...")
    
    print(f"Connected graph generation complete! Generated {len(generated_edges)} edges in {output_file}")
    print(f"Graph is guaranteed to be connected with {max_nodes} nodes")

def main():
    parser = argparse.ArgumentParser(description="Generate graph files with specified number of edges")
    parser.add_argument("num_edges", type=int, help="Number of edges to generate")
    parser.add_argument("-o", "--output", default="generated_graph.txt", 
                        help="Output file name (default: generated_graph.txt)")
    parser.add_argument("-n", "--max-nodes", type=int, 
                        help="Maximum number of nodes (auto-calculated if not specified)")
    parser.add_argument("--no-header", action="store_true", 
                        help="Don't include header row")
    parser.add_argument("-c", "--connected", action="store_true",
                        help="Generate a connected graph (spanning tree + random edges)")
    parser.add_argument("--seed", type=int, help="Random seed for reproducible results")
    
    args = parser.parse_args()
    
    # Set random seed if provided
    if args.seed:
        random.seed(args.seed)
        print(f"Using random seed: {args.seed}")
    
    # Validate input
    if args.num_edges <= 0:
        print("Error: Number of edges must be positive")
        sys.exit(1)
    
    # Generate graph
    try:
        if args.connected:
            generate_connected_graph(
                args.num_edges, 
                args.output, 
                args.max_nodes, 
                not args.no_header
            )
        else:
            generate_graph(
                args.num_edges, 
                args.output, 
                args.max_nodes, 
                not args.no_header
            )
    except Exception as e:
        print(f"Error generating graph: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()