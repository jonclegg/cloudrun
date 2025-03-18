import os
import time
import cloudrun

def test_cloud_run():
    # Ensure we have the required environment variables
    
    print("Starting cloud run test...")
    
    # cloudrun.create_infrastructure()

    # Run the test script in the cloud
    script_path = "src/test.py"
    job_id = cloudrun.run(script_path)
    
    print(f"Job started with ID: {job_id}")
    print("Test completed!")

if __name__ == "__main__":
    test_cloud_run() 