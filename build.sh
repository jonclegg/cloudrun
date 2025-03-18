#!/bin/bash
set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
SKIP_TESTS=false

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -s|--skip-tests) SKIP_TESTS=true ;;
        -h|--help)
            echo "Usage: $0 [-s|--skip-tests] [-h|--help]"
            echo
            echo "Options:"
            echo "  -s, --skip-tests    Skip running tests"
            echo "  -h, --help          Show this help message"
            exit 0
            ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

echo -e "${GREEN}Starting build process for aws-cloudrun...${NC}"

# Ensure we're in a clean virtual environment
echo -e "\n${GREEN}Setting up virtual environment...${NC}"
rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate

# Install build dependencies
echo -e "\n${GREEN}Installing build dependencies...${NC}"
python -m pip install --upgrade pip
pip install build twine

# Run tests unless skipped
if [ "$SKIP_TESTS" = false ]; then
    echo -e "\n${GREEN}Running tests...${NC}"
    pip install -e ".[dev]"
    pytest tests/ -v

    if [ $? -ne 0 ]; then
        echo -e "${RED}Tests failed! Aborting build.${NC}"
        exit 1
    fi
else
    echo -e "\n${YELLOW}Skipping tests as requested...${NC}"
    pip install -e "."
fi

# Clean previous builds
echo -e "\n${GREEN}Cleaning previous builds...${NC}"
rm -rf dist/ build/ *.egg-info

# Build package
echo -e "\n${GREEN}Building package...${NC}"
python -m build

# Verify the distribution
echo -e "\n${GREEN}Verifying distribution...${NC}"
twine check dist/*

if [ $? -ne 0 ]; then
    echo -e "${RED}Distribution verification failed! Aborting.${NC}"
    exit 1
fi

# Ask if we should publish
echo -e "\n${GREEN}Would you like to publish to PyPI? (y/N)${NC}"
read -r response

if [[ "$response" =~ ^([yY][eE][sS]|[yY])+$ ]]; then
    echo -e "\n${GREEN}Publishing to PyPI...${NC}"
    twine upload dist/*
else
    echo -e "\n${GREEN}Skipping PyPI upload. Distribution files are in ./dist/${NC}"
fi

# Deactivate virtual environment
deactivate

echo -e "\n${GREEN}Build process completed!${NC}" 