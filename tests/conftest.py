"""
Pytest configuration for Orderly project tests
"""

import pytest
import sys
from pathlib import Path

# Add the project root to Python path so we can import modules
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

@pytest.fixture(scope="session")
def project_root_path():
    """Provide the project root path for tests"""
    return Path(__file__).parent.parent

@pytest.fixture(scope="session") 
def sample_aliases():
    """Sample aliases for testing across multiple test files"""
    return [
        "TechFlow Wireless Keyboard Black",
        "tf wireless keybord black",
        "wireless kb black", 
        "techflow wireless keyboard - black",
        "TF Wireless Keyboard (Black)",
        "wireless keybord black tf",
        "brand new techflow keyboard wireless black",
        "techflow wireless mouse",
        "tf wireless mouse black",
        "wireless mouse techflow"
    ]

@pytest.fixture
def sample_typo_aliases():
    """Sample aliases with common typos for testing"""
    return [
        "wireless keyboard",
        "wireless keybord", 
        "wireless kayboard",
        "wireles keyboard",
        "wireless keyboard",
        "wireless keyboard black",
        "wireless keybord black"
    ]
