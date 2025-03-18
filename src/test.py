import time
import os
from cloudrun.setup import create_infrastructure
from cloudrun import run

def main():
    print("Starting test script...")
    print(f"Running in container: {os.getenv('HOSTNAME', 'unknown')}")
    
    # Simulate some work
    for i in range(5):
        print(f"Processing step {i+1}/5...")
        time.sleep(1)
    
    print("Test completed successfully!")

