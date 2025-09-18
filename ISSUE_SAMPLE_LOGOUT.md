
# [stability] Исправить дублирующиеся LOGOUT и завершение активных сессий

**Контекст**
- SQLite: таблица `logs`, триггер предотвращает повторный LOGOUT.
- Google Sheets: лист `ActiveSessions`; `finish_active_session` должен ставить `Status=finished`, `LogoutTime`.
- Код: `sheets_api.py`, `admin_app/repo.py:force_logout()`, `user_app/db_local.py`.

**Требования**
1. Улучшить `finish_active_session`: поиск строки по (Email, SessionID), устойчивые заголовки, ретраи.
2. Исключить «двойной LOGOUT»: учитывать триггер SQLite и причины завершения (`user/admin/timeout`).
3. Юнит-тесты с моками API + «сухой» интеграционный прогон без сети.
4. Логи — человекочитаемые сообщения (см. `logging_setup.py`).

**Где править**
- `sheets_api.py` (доменные методы: `get_active_session`, `finish_active_session`, `kick_active_session`).
- `admin_app/repo.py` (`force_logout()` — проверить сортировку/формат времени).
- `user_app/db_local.py` (маршрут логирования причин/статусов).

**Критерии готовности**
- `pytest -q` зелёный.
- Ручной прогон: корректные записи в `ActiveSessions`/`WorkLog_*` после logout/force_logout.
- Нет ошибок «Duplicate LOGOUT» в таблице `logs`.
