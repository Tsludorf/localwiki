#!/usr/bin/env python3
"""
warlock_ingester - Main CLI Entry Point
"""

import sys
import os
from pathlib import Path

# Add the project root to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.cli import main as cli_main

def main():
    """Main entry point - delegates to CLI."""
    cli_main()

if __name__ == "__main__":
    main()