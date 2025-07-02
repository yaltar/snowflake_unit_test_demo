"""
Logging utilities for controlling output verbosity.

This module provides centralized logging control to enable/disable
verbose output across the entire test framework.
"""

import os
from typing import Optional


# Global verbosity state
_VERBOSE_MODE: Optional[bool] = None


def is_verbose_mode() -> bool:
    """
    Check if verbose mode is enabled.
    
    Verbose mode is controlled by environment variables:
    - VERBOSE=1 or VERBOSE=true: Enable verbose output
    - QUIET=1 or QUIET=true: Disable verbose output  
    - PYTEST_VERBOSE=0: Disable verbose output during tests
    - Default: True (verbose enabled)
    
    Returns:
        True if verbose mode is enabled, False otherwise
    """
    global _VERBOSE_MODE
    
    # Cache the result to avoid repeated environment variable checks
    if _VERBOSE_MODE is not None:
        return _VERBOSE_MODE
    
    # Check for explicit quiet mode
    quiet_mode = os.getenv('QUIET', '0').lower() in ('1', 'true', 'yes', 'on')
    if quiet_mode:
        _VERBOSE_MODE = False
        return False
    
    # Check for pytest quiet mode
    pytest_quiet = os.getenv('PYTEST_VERBOSE', '1') == '0'
    if pytest_quiet:
        _VERBOSE_MODE = False
        return False
    
    # Check for explicit verbose mode
    verbose_mode = os.getenv('VERBOSE', '1').lower() in ('1', 'true', 'yes', 'on')
    _VERBOSE_MODE = verbose_mode
    return verbose_mode


def set_verbose_mode(enabled: bool) -> None:
    """
    Programmatically set verbose mode.
    
    Args:
        enabled: True to enable verbose output, False to disable
    """
    global _VERBOSE_MODE
    _VERBOSE_MODE = enabled


def get_logger(name: str = "sql_testing"):
    """
    Get a logger that respects the verbose mode setting.
    
    Args:
        name: Logger name (default: "sql_testing")
        
    Returns:
        Logger-like object that prints only when verbose mode is enabled
    """
    return VerboseLogger(name)


class VerboseLogger:
    """
    Logger that respects the global verbose mode setting.
    
    This logger provides the same interface as print() but only
    outputs when verbose mode is enabled.
    """
    
    def __init__(self, name: str):
        """Initialize with a logger name."""
        self.name = name
    
    def info(self, message: str) -> None:
        """Log an info message if verbose mode is enabled."""
        if is_verbose_mode():
            print(message)
    
    def debug(self, message: str) -> None:
        """Log a debug message if verbose mode is enabled."""
        if is_verbose_mode():
            print(f"🐛 {message}")
    
    def warning(self, message: str) -> None:
        """Log a warning message (always shown)."""
        print(f"⚠️  {message}")
    
    def error(self, message: str) -> None:
        """Log an error message (always shown)."""
        print(f"❌ {message}")
    
    def success(self, message: str) -> None:
        """Log a success message if verbose mode is enabled."""
        if is_verbose_mode():
            print(f"✅ {message}")


# Convenience function for quick logging
def vprint(*args, **kwargs) -> None:
    """
    Verbose print - only prints if verbose mode is enabled.
    
    Args:
        *args: Arguments to pass to print()
        **kwargs: Keyword arguments to pass to print()
    """
    if is_verbose_mode():
        print(*args, **kwargs)


# Create a default logger instance
logger = get_logger()