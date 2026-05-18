#!/usr/bin/env python3
"""
Simple verification that warlock_ingester is working
"""

import os
import sys
from pathlib import Path

def verify_system():
    print("warlock_ingester - System Verification")
    print("=" * 50)
    
    # Check main directories
    directories = [
        "data/sources",
        "data/registry", 
        "data/processed",
        "data/logs"
    ]
    
    for directory in directories:
        if os.path.exists(directory):
            print(f"✅ {directory} - EXISTS")
        else:
            print(f"❌ {directory} - MISSING")
            try:
                os.makedirs(directory, exist_ok=True)
                print(f"   Created {directory}")
            except Exception as e:
                print(f"   Failed to create: {e}")
    
    # Check if localwiki exists and is executable
    if os.path.exists("localwiki") and os.access("localwiki", os.X_OK):
        print("✅ localwiki - EXECUTABLE")
    else:
        print("❌ localwiki - NOT EXECUTABLE OR MISSING")
        
    # Show sources directory contents
    sources_dir = Path("data/sources")
    if sources_dir.exists():
        files = list(sources_dir.rglob("*"))
        print(f"📁 Sources directory contains {len(files)} items")
        for f in files:
            print(f"   {f}")
    else:
        print("📁 Sources directory does not exist yet")
        
    print("=" * 50)
    print("Verification complete")
    
if __name__ == "__main__":
    verify_system()