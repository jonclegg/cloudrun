import os
import json
from pathlib import Path
from typing import Dict, Any, Optional

###############################################################################

def get_config_dir() -> Path:
    """Get the path to the .cloudrun configuration directory."""
    return Path.home() / '.cloudrun'

###############################################################################

def get_config_path() -> Path:
    """Get the path to the config.json file."""
    return get_config_dir() / 'config.json'

###############################################################################

def load_config() -> Dict[str, Any]:
    """
    Load configuration from the config.json file.
    
    Returns:
        Dict[str, Any]: Configuration dictionary with environments
    """
    config_path = get_config_path()
    if not config_path.exists():
        return {'environments': {}}
    
    with open(config_path, 'r') as f:
        config = json.load(f)
        if 'environments' not in config:
            config['environments'] = {}
        return config

###############################################################################

def save_config(config: Dict[str, Any]) -> None:
    """
    Save configuration to the config.json file.
    
    Args:
        config: Configuration dictionary with environments
    """
    config_dir = get_config_dir()
    config_dir.mkdir(parents=True, exist_ok=True)
    
    config_path = get_config_path()
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)

###############################################################################

def get_environment_config(env_name: str = 'default') -> Dict[str, Any]:
    """
    Get configuration for a specific environment.
    
    Args:
        env_name: Name of the environment to get configuration for
        
    Returns:
        Dict[str, Any]: Environment configuration
    """
    config = load_config()
    environments = config.get('environments', {})
    return environments.get(env_name, {})

###############################################################################

def save_environment_config(env_name: str, env_config: Dict[str, Any]) -> None:
    """
    Save configuration for a specific environment.
    
    Args:
        env_name: Name of the environment
        env_config: Environment configuration to save
    """
    config = load_config()
    environments = config.get('environments', {})
    environments[env_name] = env_config
    config['environments'] = environments
    save_config(config)

###############################################################################

def get_config_value(key: str, env_name: str = 'default', default: Any = None) -> Any:
    """
    Get a configuration value by key for a specific environment.
    
    Args:
        key: Configuration key
        env_name: Name of the environment
        default: Default value if key doesn't exist
        
    Returns:
        Any: Configuration value
    """
    env_config = get_environment_config(env_name)
    return env_config.get(key, default)

###############################################################################

def set_config_value(key: str, value: Any, env_name: str = 'default') -> None:
    """
    Set a configuration value by key for a specific environment.
    
    Args:
        key: Configuration key
        value: Value to set
        env_name: Name of the environment
    """
    env_config = get_environment_config(env_name)
    env_config[key] = value
    save_environment_config(env_name, env_config)

###############################################################################

def delete_config_value(key: str, env_name: str = 'default') -> None:
    """
    Delete a configuration value by key for a specific environment.
    
    Args:
        key: Configuration key to delete
        env_name: Name of the environment
    """
    env_config = get_environment_config(env_name)
    if key in env_config:
        del env_config[key]
        save_environment_config(env_name, env_config)

###############################################################################

def clear_environment(env_name: str = 'default') -> None:
    """
    Clear all configuration values for a specific environment.
    
    Args:
        env_name: Name of the environment to clear
    """
    save_environment_config(env_name, {})

###############################################################################

def list_environments() -> list[str]:
    """
    List all configured environments.
    
    Returns:
        list[str]: List of environment names
    """
    config = load_config()
    environments = config.get('environments', {})
    return list(environments.keys())

###############################################################################

def validate_environment(env_name: str = 'default') -> None:
    """
    Validate required configuration values for a specific environment.
    
    Args:
        env_name: Name of the environment to validate
        
    Raises:
        RuntimeError: If required configuration values are missing
    """
    required_keys = [
        'CLOUDRUN_BUCKET_NAME',
        'CLOUDRUN_TASK_ROLE_ARN',
        'CLOUDRUN_TASK_DEFINITION_ARN',
        'CLOUDRUN_INITIALIZED',
        'CLOUDRUN_SUBNET_ID'
    ]
    
    env_config = get_environment_config(env_name)
    missing_keys = [key for key in required_keys if key not in env_config]
    
    if missing_keys:
        raise RuntimeError(
            f"Missing required configuration values for environment '{env_name}': "
            f"{', '.join(missing_keys)}. "
            "Please run create_infrastructure() first"
        ) 