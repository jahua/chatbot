# evaluation/benchmark_simulation.py

import random
import logging
import time
import statistics

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Simulation Functions for different RAG Configurations ---

def simulate_rag_run(query: str, config_type: str) -> dict:
    """
    Simulates running a query on a specific RAG configuration.
    Returns simulated performance metrics.
    """
    logging.debug(f"Simulating run for query '{query[:30]}...' on config: {config_type}")
    # Simulate latency based on complexity (very basic)
    base_latency = 0.5 + len(query) / 100.0
    latency_multiplier = {
        "Generic RAG": 1.5,
        "Tourism RAG (Basic)": 1.1,
        "Tourism RAG (Full)": 1.0
    }
    latency = base_latency * latency_multiplier[config_type] * random.uniform(0.8, 1.2)
    time.sleep(latency / 10) # Simulate some delay

    # Simulate metrics based on the paper's table + some randomness
    if config_type == "Generic RAG":
        factual_accuracy = random.uniform(0.70, 0.82) # Around 76%
        sql_accuracy = random.uniform(0.58, 0.70)     # Around 64%
        user_satisfaction = random.uniform(2.8, 3.4)  # Around 3.1
    elif config_type == "Tourism RAG (Basic)":
        factual_accuracy = random.uniform(0.80, 0.90) # Around 85%
        sql_accuracy = random.uniform(0.74, 0.84)     # Around 79%
        user_satisfaction = random.uniform(3.5, 4.1)  # Around 3.8
    else: # Tourism RAG (Full)
        factual_accuracy = random.uniform(0.90, 0.98) # Around 94%
        sql_accuracy = random.uniform(0.82, 0.92)     # Around 87%
        user_satisfaction = random.uniform(4.2, 4.8)  # Around 4.5

    return {
        "config_type": config_type,
        "latency_s": round(latency, 2),
        "factual_accuracy": round(factual_accuracy, 3),
        "sql_accuracy": round(sql_accuracy, 3),
        "user_satisfaction": round(user_satisfaction, 2)
    }

# --- Test Cases (Example - Can reuse from other scripts) ---
BENCHMARK_QUERIES = [
    "Show me the total visitors for July 2023",
    "Which are the top 5 regions by visitor count?",
    "Create a pie chart of spending distribution across different industries",
    "Show monthly trends for swiss and foreign visitors in 2023 on a line chart",
    "Compare spending between 'Eating Places' and 'Accommodations'",
    "What is the average spending per visitor from Germany?",
    "List hotels near Lugano station with high ratings.",
    "Show visitor density per canton."
    # Add more queries
]

# --- Evaluation Loop ---
def run_benchmark_simulation():
    logging.info("Starting Benchmark Simulation")
    configs_to_test = ["Generic RAG", "Tourism RAG (Basic)", "Tourism RAG (Full)"]
    all_results = {config: [] for config in configs_to_test}

    num_queries = len(BENCHMARK_QUERIES)
    logging.info(f"Running {num_queries} queries against {len(configs_to_test)} configurations...")

    for i, query in enumerate(BENCHMARK_QUERIES):
        logging.info(f"Processing query {i+1}/{num_queries}: '{query}'")
        for config_type in configs_to_test:
            result = simulate_rag_run(query, config_type)
            all_results[config_type].append(result)
        print("-" * 30)

    # --- Calculate and Print Summary ---
    print("\n" + "=" * 70)
    print("Benchmark Simulation Summary")
    print("=" * 70)
    print(f"{'Metric':<25} | {'Generic RAG':<15} | {'Tourism (Basic)':<15} | {'Tourism (Full)':<15}")
    print("-" * 70)

    avg_metrics = {config: {} for config in configs_to_test}
    for config in configs_to_test:
        results_list = all_results[config]
        if not results_list: continue
        avg_metrics[config]["factual_accuracy"] = statistics.mean(r["factual_accuracy"] for r in results_list)
        avg_metrics[config]["sql_accuracy"] = statistics.mean(r["sql_accuracy"] for r in results_list)
        avg_metrics[config]["user_satisfaction"] = statistics.mean(r["user_satisfaction"] for r in results_list)
        avg_metrics[config]["latency_s"] = statistics.mean(r["latency_s"] for r in results_list)

    print(f"{'Avg Factual Accuracy':<25} | {avg_metrics['Generic RAG'].get('factual_accuracy', 0):<15.3f} | {avg_metrics['Tourism RAG (Basic)'].get('factual_accuracy', 0):<15.3f} | {avg_metrics['Tourism RAG (Full)'].get('factual_accuracy', 0):<15.3f}")
    print(f"{'Avg SQL Accuracy':<25} | {avg_metrics['Generic RAG'].get('sql_accuracy', 0):<15.3f} | {avg_metrics['Tourism RAG (Basic)'].get('sql_accuracy', 0):<15.3f} | {avg_metrics['Tourism RAG (Full)'].get('sql_accuracy', 0):<15.3f}")
    print(f"{'Avg User Satisfaction':<25} | {avg_metrics['Generic RAG'].get('user_satisfaction', 0):<15.2f} | {avg_metrics['Tourism RAG (Basic)'].get('user_satisfaction', 0):<15.2f} | {avg_metrics['Tourism RAG (Full)'].get('user_satisfaction', 0):<15.2f}")
    print(f"{'Avg Latency (s)':<25} | {avg_metrics['Generic RAG'].get('latency_s', 0):<15.2f} | {avg_metrics['Tourism RAG (Basic)'].get('latency_s', 0):<15.2f} | {avg_metrics['Tourism RAG (Full)'].get('latency_s', 0):<15.2f}")
    print("-" * 70)
    logging.info("Benchmark Simulation Finished")

if __name__ == "__main__":
    run_benchmark_simulation() 