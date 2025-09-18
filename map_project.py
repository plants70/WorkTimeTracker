import os

EXCLUDE = {'.venv', '__pycache__', '.git', '.idea', 'dist', 'build'}
def tree(dir_path, prefix=''):
    entries = [e for e in os.listdir(dir_path) if e not in EXCLUDE]
    entries.sort()
    for i, name in enumerate(entries):
        path = os.path.join(dir_path, name)
        connector = '└── ' if i == len(entries) - 1 else '├── '
        print(prefix + connector + name)
        if os.path.isdir(path):
            extension = '    ' if i == len(entries) - 1 else '│   '
            tree(path, prefix + extension)

tree('.')
