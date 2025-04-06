#!/usr/bin/env python3
"""
Tourism Data Chatbot Environment Setup Script

This script sets up or updates the project environment using uv for faster dependency management.
"""

import os
import subprocess
import sys
import platform
from pathlib import Path

# Configuration
VENV_DIR = "venv311"
REQUIREMENTS_FILE = "requirements.txt"

def run_command(cmd, check=True, shell=False):
    """Run a command and return its output"""
    print(f"Running: {' '.join(cmd) if isinstance(cmd, list) else cmd}")
    result = subprocess.run(
        cmd, 
        capture_output=True, 
        text=True, 
        check=check, 
        shell=shell
    )
    return result.stdout.strip()

def is_venv_activated():
    """Check if running in an activated virtual environment"""
    return hasattr(sys, 'real_prefix') or sys.base_prefix != sys.prefix

def ensure_uv_installed():
    """Make sure uv is installed in the current environment"""
    try:
        run_command(["uv", "--version"])
        print("✅ uv is already installed")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("🔄 Installing uv...")
        run_command([sys.executable, "-m", "pip", "install", "uv"])
        print("✅ uv installed successfully")

def setup_venv():
    """Set up the virtual environment using uv"""
    venv_path = Path(VENV_DIR)
    
    # Check if virtual environment already exists
    if venv_path.exists():
        print(f"🔍 Virtual environment found at {venv_path}")
        return
    
    print(f"🔄 Creating virtual environment at {venv_path}...")
    run_command(["uv", "venv", VENV_DIR, "--python", f"python{sys.version_info.major}.{sys.version_info.minor}"])
    print(f"✅ Virtual environment created")

def install_dependencies():
    """Install project dependencies using uv"""
    print("🔄 Installing dependencies with uv...")
    
    # Install regular dependencies
    if os.path.exists(REQUIREMENTS_FILE):
        run_command(["uv", "pip", "install", "-r", REQUIREMENTS_FILE])
        print(f"✅ Installed dependencies from {REQUIREMENTS_FILE}")
    else:
        print(f"⚠️ Warning: {REQUIREMENTS_FILE} not found")
    
    # Install dev dependencies if pyproject.toml exists
    if os.path.exists("pyproject.toml"):
        try:
            run_command(["uv", "pip", "install", "-e", ".[dev]"], check=False)
            print("✅ Installed development dependencies")
        except subprocess.CalledProcessError:
            print("⚠️ Editable install failed - this is normal for initial setup")
            # Try to install just the dev dependencies without editable mode
            try:
                # Create setup.py if it doesn't exist for backwards compatibility
                if not os.path.exists("setup.py"):
                    with open("setup.py", "w") as f:
                        f.write("""
from setuptools import setup, find_packages
setup(
    name="tourism-data-chatbot",
    version="1.0.0",
    packages=find_packages(),
)
""")
                # Install dev dependencies directly
                dev_packages = ["pytest>=7.0.0", "black>=23.0.0", "isort>=5.12.0", 
                               "mypy>=1.0.0", "ruff>=0.0.230"]
                run_command(["uv", "pip", "install"] + dev_packages)
                print("✅ Installed development dependencies directly")
            except Exception as e:
                print(f"⚠️ Could not install development dependencies: {e}")
                print("You can manually install them with: uv pip install pytest black isort mypy ruff")

def print_activation_instructions():
    """Print instructions for activating the virtual environment"""
    venv_path = Path(VENV_DIR).absolute()
    
    if platform.system() == "Windows":
        activate_script = venv_path / "Scripts" / "activate"
        command = f"{activate_script}"
    else:
        activate_script = venv_path / "bin" / "activate"
        command = f"source {activate_script}"
    
    print("\n" + "=" * 60)
    print(f"To activate the virtual environment, run:\n\n    {command}")
    print("=" * 60 + "\n")

def main():
    """Main function to set up the environment"""
    print("🚀 Setting up Tourism Data Chatbot environment...")
    
    # Check if we're in a virtual environment
    if not is_venv_activated():
        print("⚠️ Not running in a virtual environment.")
        ensure_uv_installed()
        setup_venv()
        print_activation_instructions()
        print("Please activate the virtual environment and run this script again.")
        return
    
    # If we're in a virtual environment, install dependencies
    ensure_uv_installed()
    install_dependencies()
    
    print("\n✨ Environment setup complete! ✨")
    print("You can now run the application with: python run_server.py")

if __name__ == "__main__":
    main() 