#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys
from pathlib import Path

# Ensure project root is importable
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

def main():
    """Run Django's management command entry point for this project."""
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "etl_core.settings")
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Make sure it's installed and available on your "
            "PYTHONPATH environment variable. Did you forget to activate a virtual env?"
        ) from exc
    execute_from_command_line(sys.argv)

if __name__ == "__main__":
    main()
