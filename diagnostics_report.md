# Diagnostics Report — 2025-09-17T11:11:26

## Credentials
- State: **C:\Temp\wtt_creds\service_account.json**

## SQLite
| Table | Rows |
|---|---:|
| app_logs | 0 |
| app_logs_legacy_20250826175446 | 0 |
| logs | 129 |
| offline_actions | 0 |
| rule_last_sent | 0 |
| status_events | 31 |
| users_cache | 2 |

**Extra metrics:**
- logs_unsynced: 0
- offline_actions_pending: 0

## Google Sheets
**Missing columns:**
- `ActiveSessions`: рекомендуется добавить RemoteCommandAck

### Admins
**Header:** `Login`, `Password`
**Rows:** 1000

### Users
**Header:** `Email`, `Name`, `Phone`, `Role`, `Telegram`, `ShiftHours`, `Hours`, `NotifyTelegram`, `Group`
**Rows:** 997

### Groups
**Header:** `Group`, `Sheet`, `Statuses`, `Возможные статусы: "В работе",
    "Чат",
    "Аудио",
    "Запись",
    "Анкеты",
    "Перерыв",
    "Обед",
    "ЦИТО",
    "Обучение" 
Указывать через запятую, без ковычек`
**Rows:** 1000

### WorkLog_Запись
**Header:** `Timestamp`, `Email`, `Action`, `Status`, `Group`, `Start`, `End`, `Duration`, `SessionID`, `EventID`, `GroupAtStart`
**Rows:** 1000

### WorkLog_Входящие
**Header:** `Timestamp`, `Email`, `Action`, `Status`, `Group`, `Start`, `End`, `Duration`, `SessionID`, `EventID`, `GroupAtStart`
**Rows:** 29

### ActiveSessions
**Header:** `Email`, `Name`, `SessionID`, `LoginTime`, `Status`, `LogoutTime`, `RemoteCommand`, `LastHeartbeat`, `Reason`
**Rows:** 696

### WorkLog_Стоматология
**Header:** `Timestamp`, `Email`, `Action`, `Status`, `Group`, `Start`, `End`, `Duration`, `SessionID`, `EventID`, `GroupAtStart`
**Rows:** 628

### WorkLog_Почта
**Header:** `Timestamp`, `Email`, `Action`, `Status`, `Group`, `Start`, `End`, `Duration`, `SessionID`, `EventID`, `GroupAtStart`
**Rows:** 862

### AccessControl
**Header:** `KeyType`, `KeyValue`, `AccessStatus`, `BlockUntil`, `Reason`, `UpdatedAt`
**Rows:** 1000

### NotificationsLog
**Header:** `Ts`, `Kind`, `Target`, `Status`, `Preview`, `Error`
**Rows:** 2000

### NotificationRules
**Header:** `ID`, `Enabled`, `Kind`, `Scope`, `GroupTag`, `Statuses`, `MinDurationMin`, `WindowMin`, `Limit`, `RateLimitSec`, `Silent`, `MessageTemplate`
**Rows:** 998

### WorkLog_General
**Header:** `Timestamp`, `Email`, `Action`, `Status`, `Group`, `Start`, `End`, `Duration`, `SessionID`, `EventID`, `GroupAtStart`, `Reason`, `Comment`, `Name`
**Rows:** 1000

### WorkLog_Тест
**Header:** `Timestamp`, `Email`, `Action`, `Status`, `Group`, `Start`, `End`, `Duration`, `SessionID`, `EventID`, `GroupAtStart`, `Reason`, `Comment`, `Name`
**Rows:** 1000
