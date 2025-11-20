# Graph Generator

This repository contains tools to generate graphs with a specified number of edges in the format used by the Spark graph processing application.

## Files

- **`generate_graph.py`** - Main graph generator with command-line interface
- **`graph_utils.py`** - Utility functions for quick graph generation
- **`demo_graph_generation.py`** - Example script showing different graph generation methods

## Quick Usage

### Command Line Interface

Generate a graph with 1000 edges:
```bash
python3 generate_graph.py 1000
```

Generate a connected graph with 500 edges:
```bash
python3 generate_graph.py 500 -c -o data/my_connected_graph.txt
```

Generate with specific parameters:
```bash
python3 generate_graph.py 2000 -o data/large_graph.txt -n 100 --seed 42
```

### Python API

```python
from graph_utils import quick_graph, quick_connected_graph

# Generate simple graphs
quick_graph(100, "data/my_graph.txt")                    # 100 edges
quick_connected_graph(500, "data/connected.txt")         # 500 edges, guaranteed connected

# With custom parameters
quick_graph(edges=1000, filename="data/custom.txt", nodes=50, seed=123)
```

### Convenience Functions

```python
from graph_utils import small_graph, medium_graph, large_graph

small_graph()   # 100 edges
medium_graph()  # 1000 edges  
large_graph()   # 10000 edges
```

## Command Line Options

```
python3 generate_graph.py <num_edges> [options]

Arguments:
  num_edges              Number of edges to generate

Options:
  -o, --output FILE      Output file name (default: generated_graph.txt)
  -n, --max-nodes N      Maximum number of nodes (auto-calculated if not specified)
  -c, --connected        Generate a connected graph
  --no-header           Don't include header row
  --seed N              Random seed for reproducible results
```

## Graph Format

Generated graphs use the pipe-separated format:
```
source_node|target_node|edge_label
1|2|friendship
2|3|follows
3|4|likes
```

Available edge labels: `friendship`, `follows`, `likes`, `shares`, `mentions`, `collaborates`, `works_with`

## Examples

### Generate Different Sizes

```bash
# Small graph (50 edges)
python3 generate_graph.py 50 -o data/small.txt

# Medium graph (1000 edges) 
python3 generate_graph.py 1000 -o data/medium.txt

# Large graph (50000 edges)
python3 generate_graph.py 50000 -o data/large.txt

# Extra large with specific node count
python3 generate_graph.py 100000 -o data/extra_large.txt -n 1000
```

### Generate Connected Graphs

```bash
# Ensure graph connectivity
python3 generate_graph.py 500 -c -o data/connected_500.txt

# Connected graph with seed for reproducibility
python3 generate_graph.py 1000 -c --seed 42 -o data/reproducible.txt
```

### Run Demo

```bash
python3 demo_graph_generation.py
```

This will generate several example graphs in the `data/` directory.

## Graph Properties

- **Nodes**: Auto-calculated based on edge count for reasonable density
- **Edges**: No duplicate edges, no self-loops
- **Labels**: Randomly assigned from predefined set
- **Connectivity**: Optional (use `-c` flag to guarantee connectivity)

## Performance

- Can generate graphs with 100K+ edges efficiently
- Progress indicators for large graphs (10K+ edges)
- Memory-efficient streaming writes to file

## Integration with Spark

The generated graphs are compatible with the existing `graph_processing.py` Spark application:

```bash
# Generate a test graph
python3 generate_graph.py 10000 -o data/test_input.txt

# Process with Spark
./submit-spark-job.sh /opt/spark/apps/graph_processing.py
```