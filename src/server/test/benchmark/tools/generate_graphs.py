import json
import sys
import matplotlib.pyplot as plt
import numpy as np

if len(sys.argv) != 3:
    print("Usage: python generate_graphs.py input.json output_name")
    sys.exit(1)

input_file = sys.argv[1]
output_name = sys.argv[2]

with open(input_file, "r") as f:
    data = json.load(f)

labels = list(data.keys())

execution = [data[k]["execution"] for k in labels]
planning = [data[k]["planning"] for k in labels]

x = np.arange(len(labels))
width = 0.35

plt.figure()

# Planning (orange)
plt.bar(x - width/2, planning, width, color='orange', label='Planning Time')

# Execution (blue)
plt.bar(x + width/2, execution, width, color='blue', label='Execution Time')

plt.xticks(x, labels)
plt.xlabel("Date Range")
plt.ylabel("Time (ms)")
plt.title(output_name.replace("_", " ").title())
plt.legend()
plt.grid(axis='y')

# Add labels above bars
for i in range(len(labels)):
    plt.text(x[i] - width/2, planning[i] + 0.02, f"{planning[i]:.3f}", ha='center')
    plt.text(x[i] + width/2, execution[i] + 0.5, f"{execution[i]:.2f}", ha='center')

output_path = f"../outputs/{output_name}.png"
plt.savefig(output_path)

print(f"Saved graph to {output_path}")