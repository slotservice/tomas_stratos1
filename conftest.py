"""
Root conftest.py - ensures the project root is importable by pytest.
"""

import sys
from pathlib import Path

# Add the project root to sys.path so tests can import modules
# without requiring an editable install.
_PROJECT_ROOT = str(Path(__file__).resolve().parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
