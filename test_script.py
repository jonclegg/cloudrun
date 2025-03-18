from cloudrun import run
from cloudrun.setup import create_infrastructure

def main():
    # First time only: create the infrastructure
    # create_infrastructure()
    
    # Run your script
    task_id = run("main.py")
    print(f"Task ID: {task_id}")

if __name__ == "__main__":
    main() 