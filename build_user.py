from __future__ import annotations

# build_user.py
import logging
import shutil
import sys
from pathlib import Path

from PyInstaller.__main__ import run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("build_user.log", mode="w", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def main():
    try:
        logger.info("🚀 Сборка пользовательской части...")
        app_name = "WorkTimeTracker_User"
        main_script = "user_app/main.py"
        icon_file = "user_app/sberhealf.ico"

        # Очистка
        for dir_name in ["dist", "build"]:
            if Path(dir_name).exists():
                shutil.rmtree(dir_name)
                logger.info(f"🧹 Очищена директория: {dir_name}")

        # Проверка существования файлов
        required_files = [
            "secret_creds.zip",
            "config.py",
            "auto_sync.py",
            "sheets_api.py",
            "user_app",
            "sync",
        ]
        for file in required_files:
            if not Path(file).exists():
                logger.critical(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {file} не найден!")
                sys.exit(1)

        options = [
            main_script,
            f"--name={app_name}",
            "--onedir",
            "--windowed",
            "--clean",
            "--noconfirm",
            "--log-level=WARN",
            f"--icon={icon_file}" if Path(icon_file).exists() else None,
            "--paths=.",
            "--add-data=secret_creds.zip;.",
            "--add-data=config.py;.",
            "--add-data=auto_sync.py;.",
            "--add-data=sheets_api.py;.",
            "--add-data=user_app;user_app",
            "--add-data=sync;sync",
            "--hidden-import=PyQt5.sip",
            "--hidden-import=gspread",
            "--hidden-import=oauth2client",
            "--hidden-import=google.auth",
            "--hidden-import=googleapiclient",
            "--hidden-import=google.oauth2",
            "--hidden-import=googleapiclient.discovery",
            "--hidden-import=httplib2",
            "--hidden-import=OpenSSL",
            "--hidden-import=requests",
        ]

        options = [opt for opt in options if opt is not None]
        logger.info(f"⚙️ Запуск: {' '.join(options)}")
        run(options)

        exe_path = Path("dist") / app_name / f"{app_name}.exe"
        if exe_path.exists():
            logger.info(f"✅ Успех! {exe_path}")
        else:
            raise RuntimeError("Сборка прошла, но exe не найден.")

    except Exception as e:
        logger.critical(f"❌ Ошибка: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
