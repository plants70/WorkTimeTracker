import logging
import os

from logging_setup import setup_logging


logger = logging.getLogger(__name__)

EXCLUDE = {'.venv', '__pycache__', '.git', '.idea', 'dist', 'build'}
def tree(dir_path, prefix=''):
    entries = [e for e in os.listdir(dir_path) if e not in EXCLUDE]
    entries.sort()
    for i, name in enumerate(entries):
        path = os.path.join(dir_path, name)
        connector = '└── ' if i == len(entries) - 1 else '├── '
        logger.info("%s", prefix + connector + name)
        if os.path.isdir(path):
            extension = '    ' if i == len(entries) - 1 else '│   '
            tree(path, prefix + extension)

if __name__ == "__main__":
    setup_logging(app_name="wtt-map-project", force_console=True)
    tree('.')
