#!/usr/bin/env python3
"""
Configuration module for warlock_ingester
"""

import os
import json
from pathlib import Path
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class ConfigManager:
    """Manages configuration for warlock_ingester"""
    
    def __init__(self, config_file: str = None):
        self.config_file = config_file or os.path.expanduser("~/.warlock/config.json")
        self.default_config = {
            "ingestion": {
                "mode": "mode2",  # mode1 or mode2
                "resume_enabled": True,
                "deduplication": True,
                "chunk_size": 800,
                "chunk_overlap": 150
            },
            "embedding": {
                "model": "bge-m3:latest",
                "dimensions": 1024,
                "ollama_url": "http://127.0.0.1:11434"
            },
            "qdrant": {
                "url": "http://127.0.0.1:6333",
                "timeout": 5.0
            },
            "anythingllm": {
                "workspace_slug": "default",
                "collection_name": "warlock_collection"
            },
            "sources": {
                "supported_types": ["folder", "file", "zim", "wikidump"]
            }
        }
        self.config = self.load_config()
        
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from file or return default"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    return json.load(f)
            else:
                # Create config directory if it doesn't exist
                os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
                self.save_config(self.default_config)
                return self.default_config
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return self.default_config
            
    def save_config(self, config: Dict[str, Any]) -> None:
        """Save configuration to file"""
        try:
            os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
            with open(self.config_file, 'w') as f:
                json.dump(config, f, indent=2)
            self.config = config
        except Exception as e:
            logger.error(f"Failed to save config: {e}")
            
    def get(self, key_path: str, default: Any = None) -> Any:
        """Get configuration value using dot notation (e.g., 'embedding.model')"""
        keys = key_path.split('.')
        value = self.config
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
            
    def set(self, key_path: str, value: Any) -> None:
        """Set configuration value using dot notation"""
        keys = key_path.split('.')
        config = self.config
        for key in keys[:-1]:
            if key not in config:
                config[key] = {}
            config = config[key]
        config[keys[-1]] = value
        self.save_config(self.config)

# Global configuration instance
config = ConfigManager()

if __name__ == "__main__":
    print("warlock_ingester Configuration System")
    print("==================================")
    
    # Test configuration
    print(f"Ingestion Mode: {config.get('ingestion.mode')}")
    print(f"Embedding Model: {config.get('embedding.model')}")
    print(f"Qdrant URL: {config.get('qdrant.url')}")
    
    # Show configuration structure  
    print("\nFull Configuration:")
    print(json.dumps(config.config, indent=2))
