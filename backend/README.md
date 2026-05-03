# Soholms Backend

Мини-бэкенд для автоматической загрузки данных из Soholms без ручного переноса в Google Sheets.

## Что делает

- Берет Bearer/API-токен из переменной окружения `SOHOLMS_TOKEN`.
- Получает дерево групп через `POST /api/v1/learning_group/get_tree`.
- Фильтрует учебные группы `purpose = PhysicalLearning` с учениками и преподавателями.
- Если есть `groups.config.json`, берет список нужных групп оттуда.
- Скачивает XLSX-журнал по каждой группе через `attendance-sheet/excel/data`.
- Парсит учеников, группы, преподавателей, дневные оценки и считает поля для рейтинга.
- Отдает JSON для сайта.
- Формирует PDF-отчеты учеников и отправляет их через Telegram-бота.

## Запуск локально

```bash
cd backend
python3 -m pip install -r requirements.txt
SOHOLMS_TOKEN='TOKEN_ИЛИ_Bearer_TOKEN' python3 soholms_backend.py
```

По умолчанию сервер стартует на:

```text
http://127.0.0.1:8787
```

## Endpoints

Проверка:

```text
GET /health
```

Список групп:

```text
GET /api/groups
GET /api/groups?search=2030
GET /api/groups?configured=1
GET /api/groups?subjects=математика,физика
GET /api/groups?groupIds=267928,267929
GET /api/groups?subjects=математика&origins=manual
```

Рейтинговые строки:

```text
GET /api/ratings?periodFrom=2026-04-01&periodTo=2026-04-30
GET /api/ratings?periodFrom=2026-04-01&periodTo=2026-04-30&groupIds=267928
GET /api/ratings?periodFrom=2026-04-01&periodTo=2026-04-30&subjects=математика
GET /api/ratings?periodFrom=2026-04-01&periodTo=2026-04-30&subjects=математика&origins=manual&limit=10
```

Облегченный публичный ответ без админских дневных данных:

```text
GET /api/ratings?periodFrom=2026-04-01&periodTo=2026-04-30&public=1
```

Ответ `/api/ratings` содержит:

- `rows` - ученики в формате, близком к текущему `proxy.gs`.
- `groups` - загруженные группы.
- `errors` - группы, которые не удалось скачать или распарсить.
- `missingConfigNames` - имена из `groups.config.json`, которые не нашлись в Soholms.
- `missingConfigCandidates` - похожие названия из Soholms для быстрой правки конфига.

Если `groupIds`, `subjects` и `origins` не указаны, `/api/ratings` использует список из `groups.config.json`.

## Список нужных групп

По умолчанию бэкенд читает файл:

```text
backend/groups.config.json
```

Там сейчас указан список групп из скринов, три онлайн-группы по ID и только выбранные подгруппы тотального повторения по ID.

Проверить, какие группы реально нашлись:

```bash
curl 'http://127.0.0.1:8787/api/groups?configured=1' > /tmp/selected-groups.json
python3 - <<'PY'
import json
data = json.load(open('/tmp/selected-groups.json'))
print('groups:', len(data.get('groups', [])))
print('missing:', data.get('missingConfigNames', []))
for group in data.get('groups', []):
    print(group['id'], group['subject'], group['name'], group['teacher'], group['student_count'])
PY
```

Если в `missing` есть имена, значит в Soholms название чуть отличается от скрина. Исправь строку в `groups.config.json` и повтори проверку.
Посмотреть подсказки можно так:

```bash
python3 - <<'PY'
import json
data = json.load(open('/tmp/selected-groups.json'))
for name, candidates in data.get('missingConfigCandidates', {}).items():
    print(name)
    for candidate in candidates:
        print('  ->', candidate)
PY
```

Проверить полный рейтинг за выбранный период:

```bash
SOHOLMS_PERIOD_FROM=2026-04-01 \
SOHOLMS_PERIOD_TO=2026-04-30 \
python3 check_ratings.py
```

Скрипт покажет количество групп, учеников, ошибок загрузки XLSX и разбивку по предметам.

## Переменные окружения

```text
SOHOLMS_TOKEN             общий Authorization header value, если один токен работает везде
SOHOLMS_API_TOKEN         Authorization для get_tree, если токены разные
SOHOLMS_EXCEL_TOKEN       Authorization для XLSX-экспорта, если токены разные
SOHOLMS_GROUP_CONFIG      путь к JSON-файлу со списком групп, по умолчанию backend/groups.config.json
PORT                      порт, по умолчанию 8787
HOST                      хост, по умолчанию 127.0.0.1
SOHOLMS_PERIOD_FROM       период по умолчанию, если query пустой
SOHOLMS_PERIOD_TO         период по умолчанию, если query пустой
SOHOLMS_CACHE_SECONDS     кеш в памяти, по умолчанию 900 секунд
SOHOLMS_CONCURRENCY       параллельные XLSX-загрузки, по умолчанию 4
SOHOLMS_MAX_GROUPS        лимит групп за запрос, по умолчанию 80
CORS_ORIGIN               CORS, по умолчанию *
BACKEND_ADMIN_KEY         ключ для служебных endpoint: очистка кеша и debug
TELEGRAM_BOT_TOKEN        токен Telegram-бота от BotFather
TELEGRAM_CHAT_CONFIG      JSON с chat_id учеников, по умолчанию backend/telegram_chats.json
TELEGRAM_CHATS_JSON       тот же mapping, но прямо JSON-строкой для Railway Variables
```

Токены должны быть ровно тем значением, которое Soholms ожидает в заголовке `Authorization`.
Если нужен префикс `Bearer`, укажи переменную вместе с ним:

```bash
SOHOLMS_TOKEN='Bearer eyJ...'
```

Если API-док копирует токен без `Bearer`, укажи без него.

Если токен из API-доки работает для `get_tree`, а токен из Network работает только для Excel, запускай так:

```bash
SOHOLMS_API_TOKEN='ТОКЕН_ИЗ_API_ДОКИ' \
SOHOLMS_EXCEL_TOKEN='ТОКЕН_ИЗ_EXCEL_GET' \
python3 soholms_backend.py
```

## Постоянный запуск

В папке `backend` подготовлены файлы:

```text
.env.example
start_server.bat
start_server.ps1
start_server.sh
Procfile
marathon-soholms.service.example
```

Если бэкенд будет работать на твоем ПК, смотри короткую инструкцию:

```text
backend/PC_SERVER.md
```

Для VPS:

```bash
cd /opt/marathon/backend
cp .env.example .env
nano .env
python3 -m pip install -r requirements.txt
HOST=0.0.0.0 python3 soholms_backend.py
```

Для systemd:

```bash
sudo cp marathon-soholms.service.example /etc/systemd/system/marathon-soholms.service
sudo systemctl daemon-reload
sudo systemctl enable --now marathon-soholms
sudo systemctl status marathon-soholms
```

Для Render/Railway-подобного хостинга используй `Procfile`; токены и остальные настройки задаются как environment variables. После деплоя на сайте нужно один раз указать URL бэкенда:

## Нормальный публичный backend

Для GitHub Pages нужен публичный backend с `https://`, потому что страница GitHub Pages открывается по HTTPS и браузер блокирует запросы к домашнему `http://192.168...`.

В папке `backend` есть `Dockerfile`, поэтому backend можно деплоить как Docker/Web Service. В настройках сервиса укажи environment variables:

```text
HOST=0.0.0.0
PORT=8787
CORS_ORIGIN=https://YOUR_GITHUB_USERNAME.github.io
SOHOLMS_API_TOKEN=...
SOHOLMS_EXCEL_TOKEN=Bearer ...
BACKEND_ADMIN_KEY=длинный_случайный_ключ
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_CONFIG=telegram_chats.json
TELEGRAM_CHATS_JSON=
SOHOLMS_CACHE_SECONDS=900
SOHOLMS_CONCURRENCY=2
SOHOLMS_MAX_GROUPS=80
SOHOLMS_DEADLINE_SHIFT_DAYS=1
```

## Telegram PDF-отчеты

В админке во вкладке `PDF` есть две кнопки:

- `Telegram PDF по ученику` - отправляет отчет выбранному ученику.
- `Telegram PDF всем` - отправляет отдельный PDF каждому ученику, у которого есть `chat_id`.

Для отправки нужны:

```text
TELEGRAM_BOT_TOKEN=токен_бота_от_BotFather
TELEGRAM_CHATS_JSON={"students":[{"name":"Аникин Денис","parents":["Аникина Мария"],"chatIds":["123456789"],"enabled":true}]}
```

Имя в `TELEGRAM_CHATS_JSON` должно совпадать с именем ученика в рейтинге. Если у ученика несколько родителей, можно указать несколько `chatIds`. Старый формат с одним `chatId` тоже поддерживается. На Railway удобнее хранить именно `TELEGRAM_CHATS_JSON`, а локально можно использовать файл `backend/telegram_chats.json` по примеру `backend/telegram_chats.example.json`.

PDF создается на backend через `reportlab` в родительском формате: шапка с логотипом, описание марафона, имя ученика, таблица статистики по предметам и поясняющий блок для родителей. В Dockerfile добавлен шрифт DejaVu, чтобы русские буквы в PDF отображались нормально.

Если есть XLSX с колонками `Ученик`, `Родитель`, `Чат ID`, можно собрать локальный файл автоматически:

```bash
cd /Users/hrabrovoleg/marathon/backend
python3 import_telegram_chats.py "/Users/hrabrovoleg/Downloads/Отчет (апрель).xlsx"
```

Скрипт создаст `backend/telegram_chats.json`. Этот файл содержит реальные `chat_id`, поэтому он добавлен в `.gitignore` и не должен попадать в GitHub.

После деплоя проверь:

```text
https://YOUR-BACKEND-DOMAIN/health
```

Должно вернуться:

```json
{"ok":true}
```

Служебные endpoint защищаются `BACKEND_ADMIN_KEY`. Для проверки:

```bash
curl -H 'x-admin-key: ТВОЙ_BACKEND_ADMIN_KEY' https://YOUR-BACKEND-DOMAIN/api/cache/clear
```

На сайте ключ для кнопки `Очистить кеш` можно сохранить только у себя в браузере:

```js
localStorage.setItem('backendAdminKey', 'ТВОЙ_BACKEND_ADMIN_KEY');
```

Если не задать `BACKEND_ADMIN_KEY` на сервере, служебные endpoint останутся открытыми. Для публичного backend так делать не стоит.

## Telegram рассылка

Backend умеет отправлять компактный отчёт ученику через Telegram Bot API `sendMessage`.

Что нужно:

1. Создать бота через `@BotFather` и взять `TELEGRAM_BOT_TOKEN`.
2. Ученик или родитель должен написать боту `/start`, иначе бот не сможет отправить сообщение.
3. Узнать `chat_id` и добавить его в `backend/telegram_chats.json`.

Пример файла:

```json
{
  "students": [
    {
      "name": "Аникин Денис",
      "parents": ["Аникина Мария"],
      "chatIds": ["123456789"],
      "enabled": true
    }
  ]
}
```

В GitHub кладём только `telegram_chats.example.json`, а реальный `telegram_chats.json` с chat_id не коммитим.

На Railway удобнее задать mapping прямо переменной `TELEGRAM_CHATS_JSON`:

```json
{"students":[{"name":"Аникин Денис","parents":["Аникина Мария"],"chatIds":["123456789"],"enabled":true}]}
```

Отправка из админки находится во вкладке `PDF`: кнопки `Telegram по ученику` и `Telegram всем`. Они требуют `BACKEND_ADMIN_KEY`, сохранённый в браузере:

```js
localStorage.setItem('backendAdminKey', 'ТВОЙ_BACKEND_ADMIN_KEY');
```

В постоянной версии лучше прописать URL в корневом `config.js`:

```js
window.SOHOLMS_BACKEND_URL = 'https://YOUR-BACKEND-DOMAIN';
```

Для быстрой локальной проверки можно использовать консоль браузера:

```js
localStorage.setItem('soholmsBackendUrl', 'https://YOUR-BACKEND-DOMAIN');
location.reload();
```

## Важные допущения

- Итоговый балл сейчас считается как `качество * коэффициент`, где:
  - `качество` - средняя дневная оценка по заполненным дням;
  - `коэффициент` - `дней сделано / дней всего`;
  - `дней всего` - максимальный номер урока `День N` в XLSX группы.
- Штраф за просрочку: `-1` к итоговому баллу за каждый день просрочки. Если ученик переделал задание, берется самая ранняя/лучшая сдача по этому дню, поэтому поздняя переделка не добавляет новый штраф.
- Дата дневной оценки сдвигается на `+1 день`, потому что в текущей Google-таблице `День 1` из XLSX `2026-04-04` отображался как `05.апр`.

## Подключение фронта

Сайт использует backend только если задан `soholmsBackendUrl`. Иначе остается старый Google Sheets / Apps Script источник.

Для локальной проверки открой сайт и выполни в консоли браузера:

```js
localStorage.setItem('soholmsBackendUrl', 'http://127.0.0.1:8787');
localStorage.setItem('soholmsPeriodFrom', '2026-04-01');
localStorage.setItem('soholmsPeriodTo', '2026-04-30');
localStorage.removeItem('soholmsGroupIds');
localStorage.removeItem('soholmsSubjects');
localStorage.removeItem('soholmsOrigins');
localStorage.removeItem('soholmsLimit');
location.reload();
```

В админке при подключенном Soholms backend теперь можно выбрать период датами. Кнопка `Сброс` возвращает период по умолчанию из бэкенда или `groups.config.json`.

Для проверки нескольких ручных групп по предмету без долгой загрузки всех XLSX:

```js
localStorage.removeItem('soholmsGroupIds');
localStorage.setItem('soholmsSubjects', 'математика');
localStorage.setItem('soholmsOrigins', 'manual');
localStorage.setItem('soholmsLimit', '5');
location.reload();
```

Чтобы вернуться на Google Sheets:

```js
localStorage.removeItem('soholmsBackendUrl');
localStorage.removeItem('soholmsPeriodFrom');
localStorage.removeItem('soholmsPeriodTo');
localStorage.removeItem('soholmsGroupIds');
localStorage.removeItem('soholmsSubjects');
localStorage.removeItem('soholmsOrigins');
localStorage.removeItem('soholmsLimit');
location.reload();
```
