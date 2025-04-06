#!/usr/bin/env python3
import os
import sys
import subprocess
import uvicorn

def ensure_dependencies():
    """Ensure all dependencies are installed using uv."""
    try:
        # Check if requirements.txt exists
        if os.path.exists("requirements.txt"):
            print("Checking dependencies with uv...")
            # Use uv to install dependencies from requirements.txt
            subprocess.run(
                ["uv", "pip", "install", "-r", "requirements.txt", "--quiet"],
                check=True
            )
            print("Dependencies verified.")
        else:
            print("No requirements.txt found, skipping dependency check.")
    except subprocess.CalledProcessError:
        print("Warning: Failed to verify dependencies with uv.")
    except Exception as e:
        print(f"Warning: Error checking dependencies: {e}")

if __name__ == "__main__":
    # Set up Python path
    sys.path.insert(0, os.path.abspath('.'))
    
    # Check dependencies
    ensure_dependencies()
    
    # Start uvicorn server with the main app
    print("Starting server on port 8080...")
    uvicorn.run("app.main:app", 
                host="0.0.0.0", 
                port=8080, 
                reload=True, 
                log_level="info") 