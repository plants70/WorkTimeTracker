
# Проект: WorkTimeTracker (WTT)

## Стек / окружение
- Python ≥ 3.12 (целевой: 3.13)
- PyQt5 (клиент и админка), SQLite (локальная БД), Google Sheets API (gspread / google-api-python-client)
- Offline-first: локальная БД + очередь синхронизации

## Как запустить локально
1. Создайте и активируйте виртуальное окружение:
   - Windows PowerShell:
     ```powershell
     python -m venv .venv
     .\.venv\Scripts\Activate.ps1
     ```
   - Linux/macOS:
     ```bash
     python -m venv .venv
     source .venv/bin/activate
     ```
2. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```
3. Создайте файл `.env` по образцу `.env.example` и заполните значения.
4. Запуск пользовательского приложения:
   ```bash
   python -m user_app.main
   ```
   Запуск админки (если есть модуль):
   ```bash
   python -m admin_app.main
   ```

## Инварианты (нельзя ломать)
- Структуры листов Google Sheets: `Users`, `Groups`, `ActiveSessions`, `WorkLog_*`.
- Триггер SQLite, предотвращающий повторный LOGOUT для одной и той же сессии.
- Логи: единый человекочитаемый формат, без «шумных» трасс (см. logging_setup.py).
- Совместимость `sheets_api.py` с существующей логикой ретраев/квот/кэша.

## Приоритетные цели
1) Стабилизировать завершение сессий:
   - `finish_active_session`: мягкий поиск строки по (Email, SessionID), устойчивость к заголовкам, ретраи, корректный `Status` и `LogoutTime`.
   - Исключить «двойной LOGOUT»: учитывать триггер SQLite и причины завершения (user/admin/timeout).
2) Улучшить логирование и диагностику: разборчивые сообщения, единый формат.
3) Покрыть доменные методы `sheets_api.py` модульными тестами (моки API).

## Стиль, качество, CI
- Форматирование: `black`, линтинг: `ruff`.
- Коммиты: атомарные, осмысленные заголовки (`type(scope): short`).
- PR: небольшой объём, описание «что и зачем», чек-лист тестирования.

## Подсказки по коду
- Главный модуль интеграции с Google Sheets: `sheets_api.py`.
- Действия администратора: `admin_app/repo.py` (например, `force_logout()`).
- Локальная БД и логи: `user_app/db_local.py`, триггеры и таблицы `logs`/`sessions`.
- Взаимодействие с UI: модули `user_app/*`, `admin_app/*`.

## Критерии приёмки PR
- Тесты зелёные: `pytest -q`.
- Логическая проверка сценариев (ручной прогон): логин → работа → logout / force_logout → корректные записи в `ActiveSessions` и `WorkLog_*`.
- Нет регрессий в триггерах и форматах листов/таблиц.
