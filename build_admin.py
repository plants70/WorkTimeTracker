# build_admin.py
import logging
import shutil
import sys
from pathlib import Path

from PyInstaller.__main__ import run

from logging_setup import setup_logging


logger = logging.getLogger(__name__)


def main() -> None:
    try:
        logger.info("🚀 Сборка админки...")
        app_name = "WorkTimeTracker_Admin"
        main_script = "admin_app/main_admin.py"  # Путь от корня
        icon_file = "user_app/sberhealf.ico"  # Используем ту же иконку

        for dir_name in ['dist', 'build']:
            if Path(dir_name).exists():
                shutil.rmtree(dir_name)
                logger.info("🧹 Очищена директория: %s", dir_name)

        options = [
            main_script,
            f'--name={app_name}',
            '--onedir',
            '--windowed',
            '--clean',
            '--noconfirm',
            '--log-level=WARN',
            f'--icon={icon_file}' if Path(icon_file).exists() else None,
            '--paths=.',  # Ключевая строка
            '--add-data=secret_creds.zip;.',
            '--add-data=config.py;.',
            '--add-data=user_app/sberhealf.png;user_app',
            '--hidden-import=auto_sync',
            '--hidden-import=sheets_api',
            '--hidden-import=user_app.db_local',
        ]

        options = [opt for opt in options if opt is not None]

        logger.info("⚙️  Запуск: %s", ' '.join(options))
        run(options)

        exe_path = Path('dist') / app_name / f"{app_name}.exe"
        if exe_path.exists():
            logger.info("✅ Успех! %s", exe_path)
        else:
            raise RuntimeError("Сборка прошла, но exe не найден.")

    except Exception as exc:
        logger.critical("❌ Ошибка: %s", exc, exc_info=exc)
        sys.exit(1)


if __name__ == "__main__":
    setup_logging(app_name="build_admin", log_dir=Path.cwd(), force_console=True)
    main()
