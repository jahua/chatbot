import requests
import json
import sseclient
import uuid
import time
import logging
import sys

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
API_BASE_URL = "http://localhost:8081" # Make sure this matches your running backend
CHAT_STREAM_ENDPOINT = f"{API_BASE_URL}/chat/stream"
REQUEST_TIMEOUT = 90 # Increased timeout for potentially long RAG/LLM calls

# --- Test Cases ---
# Define test cases with expected outcomes
# expected_viz_type can be 'plotly_json', 'table', 'no_data', or None if no viz is expected
TEST_CASES = [
    {
        "id": "TC001",
        "query": "Show me the total visitors for July 2023",
        "expected_sql_contains": ["SUM(", "total_visitors", "year = 2023", "month = 7"],
        "expected_viz_type": "plotly_json" # Expecting single value plot
    },
    {
        "id": "TC002",
        "query": "Which are the top 5 regions by visitor count?",
        "expected_sql_contains": ["region_name", "total_visitors", "GROUP BY", "ORDER BY", "DESC", "LIMIT 5"],
        "expected_viz_type": "plotly_json" # Expecting a bar chart (as plotly_json)
    },
    {
        "id": "TC003",
        "query": "Create a pie chart of spending distribution across different industries",
        "expected_sql_contains": ["industry_name", "total_spending", "GROUP BY", "ORDER BY"],
        "expected_viz_type": "plotly_json" # Expecting a pie chart (as plotly_json)
    },
    {
        "id": "TC004",
        "query": "Show monthly trends for swiss and foreign visitors in 2023 on a line chart",
        "expected_sql_contains": ["month_name", "swiss_tourists", "foreign_tourists", "GROUP BY", "ORDER BY"],
        "expected_viz_type": "plotly_json" # Expecting line chart (or fallback bar) as plotly_json
                                      # TODO: Add check for actual line chart presence later
    },
     {
        "id": "TC005",
        "query": "Show me demographic information of Swiss guests for January 2023?",
        "expected_sql_contains": ["demographics", "WHERE", "month = 1", "year = 2023"],
        "expected_viz_type": "table" # Expecting table for raw JSONB data
    },
    {
        "id": "TC006",
        "query": "Compare spending between 'Eating Places' and 'Accommodations'",
        "expected_sql_contains": ["industry_name", "total_spending", "WHERE", "GROUP BY"], # Simplified check
        "expected_viz_type": "plotly_json" # Expecting a bar chart
    }
]

def run_single_test(test_case):
    """Runs a single test case against the API."""
    query = test_case["query"]
    session_id = f"eval-{uuid.uuid4()}"
    logging.info(f"Running {test_case['id']}: '{query}'")

    start_time = time.time()
    results = {
        "sql_query": None,
        "visualization": None,
        "final_content": None,
        "error": None,
        "sql_check": "FAIL",
        "viz_check": "FAIL",
        "duration": 0
    }

    try:
        response = requests.post(
            CHAT_STREAM_ENDPOINT,
            json={"message": query, "session_id": session_id},
            headers={"Content-Type": "application/json", "Accept": "text/event-stream"},
            stream=True,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

        client = sseclient.SSEClient(response)
        for event in client.events():
            if not event.data:
                continue

            try:
                data = json.loads(event.data)
                event_type = data.get("type")

                if event_type == "sql_query":
                    results["sql_query"] = data.get("sql_query")
                    logging.debug(f"  SQL Query Received: {results['sql_query'][:100]}...")
                elif event_type == "visualization":
                     # This event might not always be sent if viz created at the end
                     results["visualization"] = data.get("visualization")
                     logging.debug(f"  Intermediate Viz Received: {type(results['visualization'])}")
                elif event_type == "final_response":
                    results["final_content"] = data.get("content")
                    # Overwrite visualization with the final one sent
                    results["visualization"] = data.get("visualization")
                    logging.debug(f"  Final Viz Received: {type(results['visualization'])}")
                    logging.info(f"  Final Response Received. Status: {data.get('status')}")
                    break # Stop after final response
            except json.JSONDecodeError:
                logging.warning(f"  Could not decode JSON from event data: {event.data}")
            except Exception as e:
                 logging.error(f"  Error processing event data: {e}")


        client.close()
        response.close()

    except requests.exceptions.RequestException as e:
        logging.error(f"  API Request failed: {e}")
        results["error"] = str(e)
    except Exception as e:
        logging.error(f"  An unexpected error occurred: {e}")
        results["error"] = str(e)

    results["duration"] = time.time() - start_time

    # --- Perform Checks ---
    # ---> ADD TEMP LOGGING FOR TC005 SQL <--- 
    if test_case["id"] == "TC005" and results["sql_query"]:
        logging.info(f"  Actual SQL for TC005:\n{results['sql_query']}")
    # ---> END TEMP LOGGING <---
        
    if results["sql_query"]:
        contains_all = all(
            expected in results["sql_query"]
            for expected in test_case["expected_sql_contains"]
        )
        if not test_case["expected_sql_contains"] or contains_all: # Pass if no expectation or all found
             results["sql_check"] = "PASS" if test_case["expected_sql_contains"] else "N/A"


    # Check visualization type
    viz_type_actual = None
    if isinstance(results["visualization"], dict):
        viz_type_actual = results["visualization"].get("type")

    expected_viz = test_case["expected_viz_type"]
    if viz_type_actual == expected_viz:
        results["viz_check"] = "PASS"
    elif expected_viz is None and viz_type_actual is None:
         results["viz_check"] = "PASS (None)"
    else:
        # Special case: Check if fallback to table happened when plot expected
        if expected_viz in ['bar', 'pie', 'time_series', 'plotly_json'] and viz_type_actual == 'table':
            results["viz_check"] = "FAIL (Fallback Table)"
        else:
             results["viz_check"] = f"FAIL (Got {viz_type_actual})"


    logging.info(f"  Completed in {results['duration']:.2f}s. SQL: {results['sql_check']}, Viz: {results['viz_check']}")
    return results


def run_evaluation():
    """Runs all test cases and prints a summary."""
    logging.info(f"Starting evaluation against {API_BASE_URL}")
    all_results = []
    passed_sql = 0
    passed_viz = 0
    total_tests = len(TEST_CASES)

    # Health Check
    try:
        health_res = requests.get(f"{API_BASE_URL}/health", timeout=5)
        if health_res.status_code == 200:
             logging.info("API Health Check PASSED.")
        else:
             logging.error(f"API Health Check FAILED with status {health_res.status_code}. Aborting evaluation.")
             return
    except requests.exceptions.RequestException as e:
         logging.error(f"API Health Check FAILED: Cannot connect to {API_BASE_URL}. Is the server running? Error: {e}")
         return


    for case in TEST_CASES:
        result = run_single_test(case)
        all_results.append({"id": case["id"], **result})
        if result["sql_check"].startswith("PASS") or result["sql_check"] == "N/A":
            passed_sql += 1
        if result["viz_check"].startswith("PASS"):
            passed_viz += 1
        print("-" * 50) # Separator

    # --- Print Summary ---
    print("\n" + "=" * 60)
    print(f"Evaluation Summary ({total_tests} tests)")
    print("=" * 60)
    print(f"{'ID':<6} | {'SQL Check':<15} | {'Viz Check':<20} | {'Duration (s)':<12}")
    print("-" * 60)
    for res in all_results:
        print(f"{res['id']:<6} | {res['sql_check']:<15} | {res['viz_check']:<20} | {res['duration']:<12.2f}")
        if res["error"]:
             print(f"       Error: {res['error']}")
    print("-" * 60)
    print(f"SQL Correctness: {passed_sql}/{total_tests} ({passed_sql/total_tests:.1%}) passed")
    print(f"Viz Correctness: {passed_viz}/{total_tests} ({passed_viz/total_tests:.1%}) passed")
    print("=" * 60)


if __name__ == "__main__":
    run_evaluation() 