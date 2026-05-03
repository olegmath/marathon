# Backend на своем ПК

Этот вариант подходит, если бэкенд должен жить на твоем компьютере, а сайт будет брать данные с него.

## 1. Подготовить папку

Скопируй папку проекта на ПК, где будет сервер. Нужна папка:

```text
marathon/backend
```

Установи Python 3.11+:

```text
https://www.python.org/downloads/
```

На Windows при установке включи галочку `Add python.exe to PATH`.

## 2. Настроить токены

В папке `backend` создай файл `.env` из примера:

```bash
cp .env.example .env
```

На Windows можно просто скопировать `.env.example` и переименовать копию в `.env`.

Заполни минимум:

```text
HOST=0.0.0.0
PORT=8787
SOHOLMS_API_TOKEN=ТОКЕН_ИЗ_API_ДОКИ
SOHOLMS_EXCEL_TOKEN=ТОКЕН_ИЗ_NETWORK_EXCEL
```

`HOST=0.0.0.0` важен: так бэкенд будет доступен с других устройств в твоей сети.

## 3. Запуск

Windows:

```powershell
cd путь\к\marathon\backend
.\start_server.ps1
```

Если PowerShell ругается на запуск скриптов:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

Или запусти двойным кликом:

```text
start_server.bat
```

Mac/Linux:

```bash
cd /path/to/marathon/backend
chmod +x start_server.sh
./start_server.sh
```

## 4. Проверить сервер

На самом ПК:

```bash
curl http://127.0.0.1:8787/health
```

Должно быть:

```json
{"ok":true}
```

Потом узнай локальный IP ПК.

Windows:

```powershell
ipconfig
```

Ищи `IPv4 Address`, например:

```text
192.168.1.25
```

Mac:

```bash
ipconfig getifaddr en0
```

Проверь с другого устройства в этой же сети:

```bash
curl http://192.168.1.25:8787/health
```

## 5. Подключить сайт

Для локальной проверки на сайте в консоли браузера:

```js
localStorage.setItem('soholmsBackendUrl', 'http://192.168.1.25:8787');
location.reload();
```

Для постоянной настройки пропиши в корневом `config.js`:

```js
window.SOHOLMS_BACKEND_URL = 'http://192.168.1.25:8787';
```

## 6. Автозапуск

Windows:

1. Открой `Task Scheduler`.
2. `Create Basic Task`.
3. Trigger: `When the computer starts` или `At log on`.
4. Action: `Start a program`.
5. Program: `powershell.exe`.
6. Arguments:

```text
-ExecutionPolicy Bypass -File "C:\path\to\marathon\backend\start_server.ps1"
```

Mac/Linux можно использовать `systemd` или запускать `start_server.sh` вручную.

## Важно про HTTPS

Если сайт открыт через `https://`, браузер может заблокировать запросы к `http://192.168...`.

Для локальной сети проще открыть сам сайт тоже через `http://` или локальный файл. Если нужен доступ извне и HTTPS, лучше подключить Cloudflare Tunnel/ngrok к этому ПК.
