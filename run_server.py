#!/usr/bin/env python3
import os
import sys
import uvicorn

if __name__ == "__main__":
    # Set up Python path
    sys.path.insert(0, os.path.abspath('.'))
    
    # Start uvicorn server with the main app
    uvicorn.run("app.main:app", 
                host="0.0.0.0", 
                port=8080, 
                reload=True, 
                log_level="info") 