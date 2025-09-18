# Инициализация пакета user_app
from .version import __version__

__all__ = [
    'main',
    'gui', 
    'login_window',
    'db_local',
    'sheets_api',
    'sync'
]

# Инициализация путей
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))