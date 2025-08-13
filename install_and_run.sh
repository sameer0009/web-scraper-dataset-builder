#!/bin/bash

# Web Scraper & Dataset Builder - Unix/Linux/macOS Installation and Run Script
# This script automatically installs dependencies and runs the application

set -e  # Exit on any error

echo "========================================"
echo "Web Scraper & Dataset Builder"
echo "Automated Installation and Launch"
echo "========================================"
echo

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    if ! command -v python &> /dev/null; then
        print_error "Python is not installed or not in PATH"
        echo "Please install Python 3.8+ from your package manager or https://python.org"
        echo
        echo "Ubuntu/Debian: sudo apt update && sudo apt install python3 python3-pip python3-venv"
        echo "CentOS/RHEL:   sudo yum install python3 python3-pip"
        echo "macOS:         brew install python3"
        exit 1
    else
        PYTHON_CMD="python"
    fi
else
    PYTHON_CMD="python3"
fi

print_status "Python found"
$PYTHON_CMD --version

# Check Python version
PYTHON_VERSION=$($PYTHON_CMD -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
REQUIRED_VERSION="3.8"

if [ "$(printf '%s\n' "$REQUIRED_VERSION" "$PYTHON_VERSION" | sort -V | head -n1)" != "$REQUIRED_VERSION" ]; then
    print_error "Python $PYTHON_VERSION found, but Python $REQUIRED_VERSION+ is required"
    exit 1
fi

print_status "Python version $PYTHON_VERSION is compatible"

# Check if we're in the correct directory
if [ ! -f "main.py" ] || [ ! -f "requirements.txt" ]; then
    print_error "Please run this script from the web-scraper-dataset-builder directory"
    echo "Current directory: $(pwd)"
    echo "Expected files: main.py, requirements.txt"
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo
    print_info "Creating virtual environment..."
    $PYTHON_CMD -m venv venv
    if [ $? -ne 0 ]; then
        print_error "Failed to create virtual environment"
        echo "Make sure you have the venv module installed:"
        echo "Ubuntu/Debian: sudo apt install python3-venv"
        exit 1
    fi
    print_status "Virtual environment created"
else
    print_status "Virtual environment already exists"
fi

# Activate virtual environment
echo
print_info "Activating virtual environment..."
source venv/bin/activate
if [ $? -ne 0 ]; then
    print_error "Failed to activate virtual environment"
    exit 1
fi
print_status "Virtual environment activated"

# Upgrade pip
echo
print_info "Upgrading pip..."
python -m pip install --upgrade pip
if [ $? -ne 0 ]; then
    print_warning "Failed to upgrade pip, continuing anyway..."
fi

# Install system dependencies for GUI (if needed)
if command -v apt-get &> /dev/null; then
    print_info "Checking system dependencies for GUI..."
    # Check if tkinter is available
    python -c "import tkinter" 2>/dev/null || {
        print_warning "tkinter not found, attempting to install..."
        sudo apt-get update && sudo apt-get install -y python3-tk
    }
fi

# Install requirements
echo
print_info "Installing Python dependencies..."
echo "This may take a few minutes..."
pip install -r requirements.txt
if [ $? -ne 0 ]; then
    print_error "Failed to install dependencies"
    echo "Please check your internet connection and try again"
    echo "You may also need to install additional system packages:"
    echo "Ubuntu/Debian: sudo apt install python3-dev build-essential"
    echo "CentOS/RHEL:   sudo yum groupinstall 'Development Tools'"
    exit 1
fi
print_status "Dependencies installed successfully"

# Run basic tests to verify installation
echo
print_info "Running basic tests to verify installation..."
python test_basic.py
if [ $? -ne 0 ]; then
    print_warning "Basic tests failed, but attempting to run application anyway..."
    print_warning "You may experience issues during operation"
    sleep 3
else
    print_status "Basic tests passed"
fi

# Check if we're in a GUI environment
if [ -z "$DISPLAY" ] && [ -z "$WAYLAND_DISPLAY" ]; then
    print_warning "No GUI display detected"
    echo "This application requires a graphical interface"
    echo "If you're using SSH, try: ssh -X username@hostname"
    echo "Or run the application on a system with a desktop environment"
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Launch the application
echo
echo "========================================"
echo "Launching Web Scraper & Dataset Builder"
echo "========================================"
echo
print_info "The application window should open shortly..."
print_info "If you encounter any issues, check the console output above"
echo
print_info "To run the application again later:"
echo "  1. Navigate to this directory"
echo "  2. Run: ./install_and_run.sh"
echo "  3. Or activate venv and run: python main.py"
echo

# Run the application
python main.py

# Check exit status
if [ $? -ne 0 ]; then
    echo
    echo "========================================"
    print_error "Application exited with an error"
    echo "========================================"
    echo "Please check the error messages above"
    echo
    read -p "Press Enter to exit..."
else
    echo
    print_status "Application closed successfully"
    print_status "Thank you for using Web Scraper & Dataset Builder!"
    sleep 2
fi