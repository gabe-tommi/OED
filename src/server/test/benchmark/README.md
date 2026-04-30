# Benchmarking - Raw Readings Query Performance

This directory contains tools and results used to evaluate the performance of raw meter readings queries in the Open Energy Dashboard (OED) system.

The purpose of this benchmarking work is to measure how query performance changes across different time ranges and to visualize those results.

NOTE: Queries are meant to be run through PGAdmin first and have their information converted to json. Examples are provided within the data folder. 

---

## Directory Structure

benchmark/
│
├── tools/
│   └── generate_graphs.py      # Python script to generate graphs
│
├── data/
│   ├── baseline_raw.json      # Collected performance data
│   ├── start_query.json       # Collected performance data
│   └── end_index.json         # Collected performance data
│
├── outputs/                   # Generated graphs (auto-created)
│
└── README.md

---

## Python Environment Setup

This project uses a Python virtual environment to safely install dependencies.

### 1. Navigate to tools folder

cd src/server/test/benchmark/tools

---

### 2. Create a virtual environment

python3 -m venv venv

---

### 3. Activate the environment

source venv/bin/activate

You should now see:

(venv)

---

### 4. Install required packages

pip install matplotlib

---

## Running the Graph Generator

From the tools directory, run:

python generate_graphs.py ../data/<file>.json <output_name>

---

### Examples

Generate baseline graph:

python generate_graphs.py ../data/baseline_raw.json baseline

Generate start-query graph:

python generate_graphs.py ../data/start_query.json start_query

Generate end-index graph:

python generate_graphs.py ../data/end_index.json end_index

---

## Output

Graphs are saved in:

src/server/test/benchmark/outputs/

Each graph shows:

- Planning Time (orange)
- Execution Time (blue)
- Performance across different date ranges

---

## Notes

- Data is collected manually from PostgreSQL using:
  EXPLAIN (ANALYZE, BUFFERS)

- JSON files store measured values for graph generation

- The Python tool is independent of the main OED application and is used only for performance visualization

---

## Summary

This benchmarking setup provides a reproducible way to:

- Measure query performance
- Visualize execution and planning times
- Generate consistent graphs for analysis