import os
import json
import asyncio
import threading
from datetime import datetime, date
from pathlib import Path

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError, UserBannedInChannelError,
    ChatWriteForbiddenError, ChannelPrivateError, PeerIdInvalidError
)

load_dotenv()

# static_url_path='/static' — Flask роздає файли ТІЛЬКИ з /static/...
# Це НЕ конфліктує з /api/ роутами
app = Flask(__name__, static_folder='static', static_url_path='/static')
app.secret_key = os.getenv('SECRET_KEY', 'cargo2026')
CORS(app, origins='*')

# ═══════════════════════════════════════
# КОНФІГ
# ═══════════════════════════════════════
API_ID   = int(os.getenv('TG_API_ID', '0'))
API_HASH = os.getenv('TG_API_HASH', '')
PHONE    = os.getenv('TG_PHONE', '')
DATA_FILE = Path('data.json')
SESSION_FILE = 'cargo_session'

# ═══════════════════════════════════════
# БД (JSON файл)
# ═══════════════════════════════════════
DEFAULT_DATA = {
    'settings': {
        'name': 'Максим',
        'phone': '+380633885088',
        'start': '08:00',
        'end': '19:00',
    },
    'orders': [],
    'ads': [],
    'chats1': [],
    'chats2': [],
    'log': [],
    'stats': {
        'sent_today': 0,
        't1_today': 0,
        't2_today': 0,
        'last_date': '',
    },
    'dispatch': {
        'running': False,
        'mode': 'auto',
    }
}

def load_data():
    if DATA_FILE.exists():
        try:
            return json.loads(DATA_FILE.read_text(encoding='utf-8'))
        except Exception:
            pass
    return json.loads(json.dumps(DEFAULT_DATA))

def save_data(data):
    DATA_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')

def get_db():
    db = load_data()
    today_str = date.today().isoformat()
    if db.get('stats', {}).get('last_date') != today_str:
        db.setdefault('stats', {})
        db['stats']['sent_today'] = 0
        db['stats']['t1_today'] = 0
        db['stats']['t2_today'] = 0
        db['stats']['last_date'] = today_str
        save_data(db)
    return db

# Ініціалізуємо data.json при першому запуску
if not DATA_FILE.exists():
    save_data(DEFAULT_DATA)
    print('[DB] Створено data.json')

# ═══════════════════════════════════════
# TELEGRAM CLIENT
# ═══════════════════════════════════════
tg_loop = asyncio.new_event_loop()
tg_client = None
tg_connected = False
auth_pending = False
auth_phone_hash = None

def run_tg_loop():
    asyncio.set_event_loop(tg_loop)
    tg_loop.run_forever()

tg_thread = threading.Thread(target=run_tg_loop, daemon=True)
tg_thread.start()

def tg_run(coro):
    future = asyncio.run_coroutine_threadsafe(coro, tg_loop)
    return future.result(timeout=30)

async def _init_client():
    global tg_client, tg_connected
    tg_client = TelegramClient(SESSION_FILE, API_ID, API_HASH)
    await tg_client.connect()
    if await tg_client.is_user_authorized():
        tg_connected = True
        me = await tg_client.get_me()
        print(f'[TG] Підключено як: {me.first_name} ({me.phone})')
    else:
        print('[TG] Потрібна авторизація')

async def _send_code(phone):
    global auth_phone_hash
    result = await tg_client.send_code_request(phone)
    auth_phone_hash = result.phone_code_hash
    return auth_phone_hash

async def _sign_in(phone, code, phone_hash, password=None):
    global tg_connected
    try:
        await tg_client.sign_in(phone, code, phone_code_hash=phone_hash)
    except Exception as e:
        if 'password is required' in str(e).lower() or 'SessionPasswordNeeded' in type(e).__name__:
            if not password:
                raise Exception('2FA_REQUIRED')
            await tg_client.sign_in(password=password)
        else:
            raise
    tg_connected = True

async def _send_message(chat_id, text):
    await tg_client.send_message(chat_id, text)

async def _get_dialogs():
    dialogs = []
    async for d in tg_client.iter_dialogs(limit=50):
        if d.is_group or d.is_channel:
            dialogs.append({
                'id': str(d.id),
                'name': d.name,
                'username': d.entity.username if hasattr(d.entity, 'username') else None,
            })
    return dialogs

try:
    tg_run(_init_client())
except Exception as e:
    print(f'[TG] Помилка ініціалізації: {e}')

# ═══════════════════════════════════════
# РОЗСИЛКА
# ═══════════════════════════════════════
dispatch_lock = threading.Lock()

def build_contact(settings):
    name  = settings.get('name', '')
    phone = settings.get('phone', '')
    if phone and name:
        return f'{phone} ({name})'
    return phone or name or ''

def fmt_date(d):
    if not d:
        return ''
    try:
        parts = d.split('-')
        return f'{parts[2]}.{parts[1]}.{parts[0]}'
    except Exception:
        return d

def build_order_text(order, num, contact):
    lines = [f'🚛 ЗАЯВКА #{num}']
    from_parts = [order.get('fromCity', ''), order.get('fromVil', '')]
    to_parts   = [order.get('toCity', ''), order.get('toVil', '')]
    from_str = ', '.join(p for p in from_parts if p)
    to_str   = ', '.join(p for p in to_parts   if p)
    if from_str or to_str:
        lines.append(f'📍 Маршрут: {" → ".join(p for p in [from_str, to_str] if p)}')
    if order.get('dist'):
        lines.append(f'📏 Відстань: {order["dist"]} км')
    cargo_parts = [order.get('cargo', ''), order.get('weight', '')]
    cargo_str = ' — '.join(p for p in cargo_parts if p)
    if cargo_str:
        lines.append(f'📦 Вантаж: {cargo_str}')
    trucks = order.get('trucks', [])
    if trucks:
        lines.append(f'🚛 Тип авто: {", ".join(trucks)}')
    count = order.get('count', '1')
    if count and str(count) != '1':
        lines.append(f'🔢 Кількість машин: {count}')
    tariff = order.get('tariff', 'pdv')
    if tariff == 'ask':
        lines.append('💰 Ціна: запит (перевізник називає свою ціну)')
    else:
        price_parts = []
        if order.get('pdv'):
            price_parts.append(f'{order["pdv"]} грн (ПДВ)')
        if order.get('fop'):
            price_parts.append(f'{order["fop"]} грн (ФОП)')
        if price_parts:
            lines.append(f'💰 Ціна: {" / ".join(price_parts)}')
    d1 = fmt_date(order.get('date1', ''))
    d2 = fmt_date(order.get('date2', ''))
    dates = ' — '.join(p for p in [d1, d2] if p)
    if dates:
        lines.append(f'📅 Дати: {dates}')
    if order.get('note'):
        lines.append(f'📝 Примітка: {order["note"]}')
    if contact:
        lines.append(f'📞 Контакт: {contact}')
    return '\n'.join(lines)

def build_t1_message(orders, contact):
    active = [o for o in orders if o.get('active') and o.get('status') == 'active']
    if not active:
        return None
    return '\n\n——————————\n\n'.join(
        build_order_text(o, i + 1, contact) for i, o in enumerate(active)
    )

def can_send(chat):
    last = chat.get('lastSent')
    if not last:
        return True
    limit_ms = chat.get('limit', 30) * 60 * 1000
    now_ms = int(datetime.now().timestamp() * 1000)
    return (now_ms - last) >= limit_ms

def add_log(db, log_type, chat_name, status, orders=None):
    now = datetime.now()
    entry = {
        'type': log_type,
        'chat': chat_name,
        'status': status,
        'orders': orders or [],
        'time': now.strftime('%H:%M'),
        'date': now.strftime('%d.%m.%Y'),
        'ts': int(now.timestamp() * 1000),
    }
    db['log'].insert(0, entry)
    if len(db['log']) > 300:
        db['log'] = db['log'][:300]

async def _do_send(chat_identifier, text):
    try:
        entity = chat_identifier
        try:
            entity = int(chat_identifier)
        except (ValueError, TypeError):
            pass
        await tg_client.send_message(entity, text, parse_mode=None)
        return True, None
    except FloodWaitError as e:
        return False, f'FloodWait {e.seconds}с'
    except (UserBannedInChannelError, ChatWriteForbiddenError):
        return False, 'Немає прав на відправку'
    except ChannelPrivateError:
        return False, 'Приватний канал'
    except PeerIdInvalidError:
        return False, 'Невірний ID чату'
    except Exception as e:
        return False, str(e)[:80]

def dispatch_tick():
    if not tg_connected:
        return
    with dispatch_lock:
        db = get_db()
        if not db['dispatch']['running']:
            return
        settings = db['settings']
        contact  = build_contact(settings)

        t1_msg = build_t1_message(db['orders'], contact)
        if t1_msg:
            ready1 = [c for c in db['chats1'] if c.get('active') and can_send(c)]
            if ready1:
                chat = ready1[0]
                ok, err = tg_run(_do_send(chat.get('user') or chat.get('id', ''), t1_msg))
                now_ms = int(datetime.now().timestamp() * 1000)
                chat['lastSent'] = now_ms
                if ok:
                    db['stats']['sent_today'] += 1
                    db['stats']['t1_today']   += 1
                    order_labels = [
                        f'#{i+1} {o.get("cargo", "")}'
                        for i, o in enumerate(db['orders'])
                        if o.get('active') and o.get('status') == 'active'
                    ]
                    add_log(db, 't1', chat['name'], '✅ Відправлено', order_labels)
                    print(f'[T1] → {chat["name"]} ✅')
                else:
                    add_log(db, 't1', chat['name'], f'❌ Помилка: {err}')
                    print(f'[T1] → {chat["name"]} ❌ {err}')

        active_ads = [a for a in db['ads'] if a.get('active')]
        if active_ads:
            ad_text = active_ads[0]['text']
            ready2 = [c for c in db['chats2'] if c.get('active') and can_send(c)]
            if ready2:
                chat = ready2[0]
                ok, err = tg_run(_do_send(chat.get('user') or chat.get('id', ''), ad_text))
                now_ms = int(datetime.now().timestamp() * 1000)
                chat['lastSent'] = now_ms
                if ok:
                    db['stats']['sent_today'] += 1
                    db['stats']['t2_today']   += 1
                    add_log(db, 't2', chat['name'], '✅ Відправлено')
                    print(f'[T2] → {chat["name"]} ✅')
                else:
                    add_log(db, 't2', chat['name'], f'❌ Помилка: {err}')
                    print(f'[T2] → {chat["name"]} ❌ {err}')

        save_data(db)

def check_auto_schedule():
    db = get_db()
    if db['dispatch']['mode'] != 'auto':
        return
    settings = db['settings']
    now_time = datetime.now().strftime('%H:%M')
    start = settings.get('start', '08:00')
    end   = settings.get('end',   '19:00')
    should_run = start <= now_time < end
    if should_run and not db['dispatch']['running']:
        db['dispatch']['running'] = True
        add_log(db, 'sys', '⚙️ Система', f'✅ Авто-запуск ({start})')
        save_data(db)
        print(f'[AUTO] Запуск о {now_time}')
    elif not should_run and db['dispatch']['running'] and db['dispatch']['mode'] == 'auto':
        db['dispatch']['running'] = False
        add_log(db, 'sys', '⚙️ Система', f'⏹ Авто-зупинка ({end}). Відправлено: {db["stats"]["sent_today"]}')
        save_data(db)
        print(f'[AUTO] Зупинка о {now_time}')

# ═══════════════════════════════════════
# ПЛАНУВАЛЬНИК
# ═══════════════════════════════════════
scheduler = BackgroundScheduler(timezone='Europe/Kiev')
scheduler.add_job(dispatch_tick,        'interval', seconds=30, id='dispatch')
scheduler.add_job(check_auto_schedule,  'interval', minutes=1,  id='schedule')
scheduler.start()

# ═══════════════════════════════════════
# ГЛОБАЛЬНА ОБРОБКА ПОМИЛОК
# ═══════════════════════════════════════
@app.errorhandler(404)
def not_found(e):
    return jsonify({'ok': False, 'error': 'Not found'}), 404

@app.errorhandler(500)
def server_error(e):
    return jsonify({'ok': False, 'error': 'Internal server error'}), 500

@app.errorhandler(Exception)
def handle_exception(e):
    print(f'[ERROR] {e}')
    return jsonify({'ok': False, 'error': str(e)}), 500

# ═══════════════════════════════════════
# HEALTHCHECK
# ═══════════════════════════════════════
@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'tg': tg_connected})

# ═══════════════════════════════════════
# ГОЛОВНА СТОРІНКА — тільки /
# ═══════════════════════════════════════
@app.route('/')
def serve_index():
    return send_from_directory('static', 'index.html')

# ═══════════════════════════════════════
# API РОУТИ
# ═══════════════════════════════════════

# ── СТАН ──
@app.route('/api/state', methods=['GET'])
def api_state():
    try:
        db = get_db()
        return jsonify({
            'tg_connected': tg_connected,
            'running':  db['dispatch']['running'],
            'mode':     db['dispatch']['mode'],
            'stats':    db['stats'],
            'settings': db['settings'],
        })
    except Exception as e:
        print(f'[API] /api/state error: {e}')
        return jsonify({
            'tg_connected': False, 'running': False, 'mode': 'auto',
            'stats': {'sent_today': 0, 't1_today': 0, 't2_today': 0},
            'settings': {'name': '', 'phone': '', 'start': '08:00', 'end': '19:00'},
        })

# ── НАЛАШТУВАННЯ ──
@app.route('/api/settings', methods=['GET', 'POST'])
def api_settings():
    try:
        db = get_db()
        if request.method == 'POST':
            db['settings'].update(request.json)
            save_data(db)
            return jsonify({'ok': True})
        return jsonify(db['settings'])
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

# ── ЗАЯВКИ ──
@app.route('/api/orders', methods=['GET'])
def api_orders_get():
    try:
        return jsonify(get_db()['orders'])
    except Exception as e:
        return jsonify([])

@app.route('/api/orders', methods=['POST'])
def api_orders_post():
    try:
        db = get_db()
        order = request.json
        order['id'] = int(datetime.now().timestamp() * 1000)
        order.setdefault('active', True)
        order.setdefault('status', 'active')
        db['orders'].append(order)
        save_data(db)
        return jsonify({'ok': True, 'id': order['id']})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/orders/<int:oid>', methods=['PUT'])
def api_orders_put(oid):
    try:
        db = get_db()
        for o in db['orders']:
            if o['id'] == oid:
                o.update(request.json)
                break
        save_data(db)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/orders/<int:oid>', methods=['DELETE'])
def api_orders_delete(oid):
    try:
        db = get_db()
        db['orders'] = [o for o in db['orders'] if o['id'] != oid]
        save_data(db)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

# ── РЕКЛАМА ──
@app.route('/api/ads', methods=['GET'])
def api_ads_get():
    try:
        return jsonify(get_db()['ads'])
    except Exception as e:
        return jsonify([])

@app.route('/api/ads', methods=['POST'])
def api_ads_post():
    try:
        db = get_db()
        ad = request.json
        ad['id'] = int(datetime.now().timestamp() * 1000)
        ad.setdefault('active', True)
        db['ads'].append(ad)
        save_data(db)
        return jsonify({'ok': True, 'id': ad['id']})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/ads/<int:aid>', methods=['PUT'])
def api_ads_put(aid):
    try:
        db = get_db()
        for a in db['ads']:
            if a['id'] == aid:
                a.update(request.json)
                break
        save_data(db)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/ads/<int:aid>', methods=['DELETE'])
def api_ads_delete(aid):
    try:
        db = get_db()
        db['ads'] = [a for a in db['ads'] if a['id'] != aid]
        save_data(db)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

# ── ЧАТИ ──
@app.route('/api/chats/<int:t>', methods=['GET'])
def api_chats_get(t):
    try:
        return jsonify(get_db().get(f'chats{t}', []))
    except Exception as e:
        return jsonify([])

@app.route('/api/chats/<int:t>', methods=['POST'])
def api_chats_post(t):
    try:
        db = get_db()
        chat = request.json
        chat['id'] = int(datetime.now().timestamp() * 1000)
        chat.setdefault('active', True)
        chat.setdefault('lastSent', None)
        db.setdefault(f'chats{t}', []).append(chat)
        save_data(db)
        return jsonify({'ok': True, 'id': chat['id']})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/chats/<int:t>/<int:cid>', methods=['PUT'])
def api_chats_put(t, cid):
    try:
        db = get_db()
        for c in db.get(f'chats{t}', []):
            if c['id'] == cid:
                c.update(request.json)
                break
        save_data(db)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/chats/<int:t>/<int:cid>', methods=['DELETE'])
def api_chats_delete(t, cid):
    try:
        db = get_db()
        key = f'chats{t}'
        db[key] = [c for c in db.get(key, []) if c['id'] != cid]
        save_data(db)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

# ── РОЗСИЛКА ──
@app.route('/api/dispatch/start', methods=['POST'])
def api_dispatch_start():
    try:
        db = get_db()
        db['dispatch']['running'] = True
        add_log(db, 'sys', '⚙️ Система', '✅ Розсилку запущено вручну')
        save_data(db)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/dispatch/stop', methods=['POST'])
def api_dispatch_stop():
    try:
        db = get_db()
        db['dispatch']['running'] = False
        add_log(db, 'sys', '⚙️ Система', f'⏹ Розсилку зупинено. Відправлено: {db["stats"]["sent_today"]}')
        save_data(db)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/dispatch/mode', methods=['POST'])
def api_dispatch_mode():
    try:
        db = get_db()
        db['dispatch']['mode'] = request.json.get('mode', 'auto')
        save_data(db)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

# ── ЖУРНАЛ ──
@app.route('/api/log', methods=['GET'])
def api_log():
    try:
        return jsonify(get_db().get('log', [])[:100])
    except Exception as e:
        return jsonify([])

# ── TELEGRAM AUTH ──
@app.route('/api/tg/status', methods=['GET'])
def api_tg_status():
    return jsonify({'connected': tg_connected, 'phone': PHONE})

@app.route('/api/tg/send_code', methods=['POST'])
def api_tg_send_code():
    global auth_pending
    phone = request.json.get('phone', PHONE)
    try:
        tg_run(_send_code(phone))
        auth_pending = True
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 400

@app.route('/api/tg/confirm', methods=['POST'])
def api_tg_confirm():
    global auth_pending, tg_connected
    code     = request.json.get('code', '')
    phone    = request.json.get('phone', PHONE)
    password = request.json.get('password', '')
    try:
        tg_run(_sign_in(phone, code, auth_phone_hash, password or None))
        auth_pending = False
        tg_connected = True
        return jsonify({'ok': True})
    except Exception as e:
        err_str = str(e)
        if '2FA_REQUIRED' in err_str:
            return jsonify({'ok': False, 'error': '2FA_REQUIRED'}), 400
        return jsonify({'ok': False, 'error': err_str}), 400

@app.route('/api/tg/dialogs', methods=['GET'])
def api_tg_dialogs():
    if not tg_connected:
        return jsonify([])
    try:
        dialogs = tg_run(_get_dialogs())
        return jsonify(dialogs)
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# ═══════════════════════════════════════
# ЗАПУСК
# ═══════════════════════════════════════
if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    print(f'[SERVER] Запуск на порту {port}')
    app.run(host='0.0.0.0', port=port, debug=False)
