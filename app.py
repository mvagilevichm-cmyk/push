import os
import json
import asyncio
import threading
import random
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
import psycopg2
from psycopg2.extras import RealDictCursor
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    FloodWaitError, UserBannedInChannelError,
    ChatWriteForbiddenError, ChannelPrivateError, PeerIdInvalidError
)

load_dotenv()

app = Flask(__name__, static_folder='static', static_url_path='/static')
app.secret_key = os.getenv('SECRET_KEY', 'cargo2026')
CORS(app, origins='*')

# Часовий пояс Україна
UA_TZ = ZoneInfo('Europe/Kyiv')
def now_ua():
    """Поточний час в Україні"""
    return datetime.now(UA_TZ)

# ═══════════════════════════════════════
# КОНФІГ
# ═══════════════════════════════════════
API_ID   = int(os.getenv('TG_API_ID', '0'))
API_HASH = os.getenv('TG_API_HASH', '')
PHONE    = os.getenv('TG_PHONE', '')
DATABASE_URL = os.getenv('DATABASE_URL', '')

# ═══════════════════════════════════════
# POSTGRESQL
# ═══════════════════════════════════════
def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def init_db():
    """Створити таблиці якщо не існують"""
    conn = get_conn()
    cur = conn.cursor()

    # Міграція: видаляємо старі JSONB таблиці якщо існують
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='orders' AND column_name='data'")
    if cur.fetchone():
        cur.execute('DROP TABLE IF EXISTS orders CASCADE')
        cur.execute('DROP TABLE IF EXISTS ads CASCADE')
        cur.execute('DROP TABLE IF EXISTS chats CASCADE')
        print('[DB] Міграція: видалено старі JSONB таблиці')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            id BIGINT PRIMARY KEY,
            cargo TEXT DEFAULT '',
            weight TEXT DEFAULT '',
            from_city TEXT DEFAULT '',
            from_vil TEXT DEFAULT '',
            to_city TEXT DEFAULT '',
            to_vil TEXT DEFAULT '',
            dist TEXT DEFAULT '',
            count TEXT DEFAULT '1',
            trucks TEXT DEFAULT '',
            tariff TEXT DEFAULT 'pdv',
            pdv TEXT DEFAULT '',
            fop TEXT DEFAULT '',
            date1 TEXT DEFAULT '',
            date2 TEXT DEFAULT '',
            note TEXT DEFAULT '',
            status TEXT DEFAULT 'active',
            active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW()
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS ads (
            id BIGINT PRIMARY KEY,
            name TEXT DEFAULT '',
            text TEXT DEFAULT '',
            active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW()
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS chats (
            id BIGINT PRIMARY KEY,
            chat_type INTEGER NOT NULL,
            name TEXT DEFAULT '',
            username TEXT DEFAULT '',
            topic_id TEXT DEFAULT NULL,
            topic_name TEXT DEFAULT '',
            chat_limit INTEGER DEFAULT 30,
            slowmode INTEGER DEFAULT 0,
            active BOOLEAN DEFAULT TRUE,
            last_sent BIGINT DEFAULT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS log (
            id SERIAL PRIMARY KEY,
            type TEXT NOT NULL,
            chat TEXT NOT NULL,
            status TEXT NOT NULL,
            orders_info TEXT DEFAULT '',
            time TEXT NOT NULL,
            log_date TEXT NOT NULL,
            ts BIGINT NOT NULL
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS dispatch (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS stats (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS tg_session (
            id INTEGER PRIMARY KEY DEFAULT 1,
            session_string TEXT NOT NULL
        )
    ''')

    # Міграція: додаємо колонку slowmode якщо ще не існує
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='chats' AND column_name='slowmode'")
    if not cur.fetchone():
        cur.execute('ALTER TABLE chats ADD COLUMN slowmode INTEGER DEFAULT 0')
        print('[DB] Міграція: додано колонку slowmode')

    # Дефолтні налаштування
    defaults = {
        'name': 'Максим',
        'phone': '+380633885088',
        'start': '08:00',
        'end': '19:00',
    }
    for k, v in defaults.items():
        cur.execute(
            'INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING',
            (k, v)
        )
    cur.execute("INSERT INTO dispatch (key, value) VALUES ('running', 'false') ON CONFLICT (key) DO NOTHING")
    cur.execute("INSERT INTO dispatch (key, value) VALUES ('mode', 'auto') ON CONFLICT (key) DO NOTHING")
    cur.execute("INSERT INTO stats (key, value) VALUES ('sent_today', '0') ON CONFLICT (key) DO NOTHING")
    cur.execute("INSERT INTO stats (key, value) VALUES ('t1_today', '0') ON CONFLICT (key) DO NOTHING")
    cur.execute("INSERT INTO stats (key, value) VALUES ('t2_today', '0') ON CONFLICT (key) DO NOTHING")
    cur.execute("INSERT INTO stats (key, value) VALUES ('last_date', '') ON CONFLICT (key) DO NOTHING")
    conn.commit()
    cur.close()
    conn.close()
    print('[DB] PostgreSQL ініціалізовано')

# ── Хелпери БД ──
def db_get_settings():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('SELECT key, value FROM settings')
    result = {row['key']: row['value'] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return result

def db_set_settings(data):
    conn = get_conn()
    cur = conn.cursor()
    for k, v in data.items():
        cur.execute(
            'INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = %s',
            (k, str(v), str(v))
        )
    conn.commit()
    cur.close()
    conn.close()

def db_get_orders():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('''SELECT id, cargo, weight, from_city, from_vil, to_city, to_vil,
                   dist, count, trucks, tariff, pdv, fop, date1, date2, note, status, active
                   FROM orders ORDER BY created_at''')
    result = []
    for row in cur.fetchall():
        o = dict(row)
        o['fromCity'] = o.pop('from_city', '')
        o['fromVil'] = o.pop('from_vil', '')
        o['toCity'] = o.pop('to_city', '')
        o['toVil'] = o.pop('to_vil', '')
        trucks_str = o.get('trucks', '')
        o['trucks'] = [t.strip() for t in trucks_str.split(',') if t.strip()] if trucks_str else []
        result.append(o)
    cur.close()
    conn.close()
    return result

def db_add_order(order):
    oid = int(now_ua().timestamp() * 1000)
    conn = get_conn()
    cur = conn.cursor()
    trucks = ','.join(order.get('trucks', [])) if isinstance(order.get('trucks'), list) else order.get('trucks', '')
    cur.execute('''
        INSERT INTO orders (id, cargo, weight, from_city, from_vil, to_city, to_vil,
                           dist, count, trucks, tariff, pdv, fop, date1, date2, note, status, active)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ''', (
        oid,
        order.get('cargo', ''), order.get('weight', ''),
        order.get('fromCity', ''), order.get('fromVil', ''),
        order.get('toCity', ''), order.get('toVil', ''),
        order.get('dist', ''), order.get('count', '1'),
        trucks, order.get('tariff', 'pdv'),
        order.get('pdv', ''), order.get('fop', ''),
        order.get('date1', ''), order.get('date2', ''),
        order.get('note', ''), order.get('status', 'active'),
        order.get('active', True),
    ))
    conn.commit()
    cur.close()
    conn.close()
    return oid

def db_update_order(oid, data):
    conn = get_conn()
    cur = conn.cursor()
    field_map = {
        'cargo': 'cargo', 'weight': 'weight',
        'fromCity': 'from_city', 'fromVil': 'from_vil',
        'toCity': 'to_city', 'toVil': 'to_vil',
        'dist': 'dist', 'count': 'count',
        'tariff': 'tariff', 'pdv': 'pdv', 'fop': 'fop',
        'date1': 'date1', 'date2': 'date2',
        'note': 'note', 'status': 'status', 'active': 'active',
    }
    for js_key, db_col in field_map.items():
        if js_key in data:
            val = data[js_key]
            cur.execute(f'UPDATE orders SET {db_col} = %s WHERE id = %s', (val, oid))
    if 'trucks' in data:
        trucks = ','.join(data['trucks']) if isinstance(data['trucks'], list) else data['trucks']
        cur.execute('UPDATE orders SET trucks = %s WHERE id = %s', (trucks, oid))
    conn.commit()
    cur.close()
    conn.close()

def db_delete_order(oid):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('DELETE FROM orders WHERE id = %s', (oid,))
    conn.commit()
    cur.close()
    conn.close()

def db_get_ads():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('SELECT id, name, text, active FROM ads ORDER BY created_at')
    result = [dict(row) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return result

def db_add_ad(ad):
    aid = int(now_ua().timestamp() * 1000)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        'INSERT INTO ads (id, name, text, active) VALUES (%s, %s, %s, %s)',
        (aid, ad.get('name', ''), ad.get('text', ''), ad.get('active', True))
    )
    conn.commit()
    cur.close()
    conn.close()
    return aid

def db_update_ad(aid, data):
    conn = get_conn()
    cur = conn.cursor()
    for col in ['name', 'text', 'active']:
        if col in data:
            cur.execute(f'UPDATE ads SET {col} = %s WHERE id = %s', (data[col], aid))
    conn.commit()
    cur.close()
    conn.close()

def db_delete_ad(aid):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('DELETE FROM ads WHERE id = %s', (aid,))
    conn.commit()
    cur.close()
    conn.close()

def db_get_chats(chat_type):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('''SELECT id, name, username, topic_id, topic_name, chat_limit, slowmode, active, last_sent
                   FROM chats WHERE chat_type = %s ORDER BY created_at''', (chat_type,))
    result = []
    for row in cur.fetchall():
        c = dict(row)
        c['user'] = c.pop('username', '')
        c['limit'] = c.pop('chat_limit', 30)
        c['slowmode'] = c.get('slowmode', 0) or 0
        c['lastSent'] = c.pop('last_sent', None)
        result.append(c)
    cur.close()
    conn.close()
    return result

def db_add_chat(chat_type, chat):
    cid = int(now_ua().timestamp() * 1000)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO chats (id, chat_type, name, username, topic_id, topic_name, chat_limit, slowmode, active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', (
        cid, chat_type,
        chat.get('name', ''), chat.get('user', ''),
        chat.get('topic_id'), chat.get('topic_name', ''),
        chat.get('limit', 30), chat.get('slowmode', 0),
        chat.get('active', True),
    ))
    conn.commit()
    cur.close()
    conn.close()
    return cid

def db_update_chat(chat_type, cid, data):
    conn = get_conn()
    cur = conn.cursor()
    field_map = {
        'active': 'active', 'lastSent': 'last_sent',
        'name': 'name', 'user': 'username',
        'topic_id': 'topic_id', 'topic_name': 'topic_name',
        'limit': 'chat_limit', 'slowmode': 'slowmode',
    }
    for js_key, db_col in field_map.items():
        if js_key in data:
            cur.execute(f'UPDATE chats SET {db_col} = %s WHERE id = %s AND chat_type = %s',
                        (data[js_key], cid, chat_type))
    conn.commit()
    cur.close()
    conn.close()

def db_delete_chat(chat_type, cid):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('DELETE FROM chats WHERE id = %s AND chat_type = %s', (cid, chat_type))
    conn.commit()
    cur.close()
    conn.close()

def db_get_dispatch():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('SELECT key, value FROM dispatch')
    result = {row['key']: row['value'] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return {
        'running': result.get('running', 'false') == 'true',
        'mode': result.get('mode', 'auto'),
    }

def db_set_dispatch(key, value):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        'INSERT INTO dispatch (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = %s',
        (key, str(value), str(value))
    )
    conn.commit()
    cur.close()
    conn.close()

def db_get_stats():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('SELECT key, value FROM stats')
    result = {row['key']: row['value'] for row in cur.fetchall()}
    cur.close()
    conn.close()
    today_str = now_ua().date().isoformat()
    if result.get('last_date') != today_str:
        db_set_stat('sent_today', '0')
        db_set_stat('t1_today', '0')
        db_set_stat('t2_today', '0')
        db_set_stat('last_date', today_str)
        return {'sent_today': 0, 't1_today': 0, 't2_today': 0, 'last_date': today_str}
    return {
        'sent_today': int(result.get('sent_today', 0)),
        't1_today': int(result.get('t1_today', 0)),
        't2_today': int(result.get('t2_today', 0)),
        'last_date': result.get('last_date', ''),
    }

def db_set_stat(key, value):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        'INSERT INTO stats (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = %s',
        (key, str(value), str(value))
    )
    conn.commit()
    cur.close()
    conn.close()

def db_inc_stat(key):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('SELECT value FROM stats WHERE key = %s', (key,))
    row = cur.fetchone()
    new_val = int(row['value']) + 1 if row else 1
    cur.execute(
        'INSERT INTO stats (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = %s',
        (key, str(new_val), str(new_val))
    )
    conn.commit()
    cur.close()
    conn.close()

def db_add_log(log_type, chat_name, status, orders=None):
    now = now_ua()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        'INSERT INTO log (type, chat, status, orders_info, time, log_date, ts) VALUES (%s,%s,%s,%s,%s,%s,%s)',
        (log_type, chat_name, status, json.dumps(orders or []), now.strftime('%H:%M'),
         now.strftime('%d.%m.%Y'), int(now.timestamp() * 1000))
    )
    cur.execute('DELETE FROM log WHERE id NOT IN (SELECT id FROM log ORDER BY ts DESC LIMIT 300)')
    conn.commit()
    cur.close()
    conn.close()

def db_get_log(limit=100):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('SELECT type, chat, status, orders_info, time, log_date as date, ts FROM log ORDER BY ts DESC LIMIT %s', (limit,))
    result = []
    for row in cur.fetchall():
        r = dict(row)
        try:
            r['orders'] = json.loads(r.pop('orders_info', '[]'))
        except Exception:
            r['orders'] = []
            r.pop('orders_info', None)
        result.append(r)
    cur.close()
    conn.close()
    return result

def db_get_session():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('SELECT session_string FROM tg_session WHERE id = 1')
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row['session_string'] if row else None

def db_save_session(session_string):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        'INSERT INTO tg_session (id, session_string) VALUES (1, %s) ON CONFLICT (id) DO UPDATE SET session_string = %s',
        (session_string, session_string)
    )
    conn.commit()
    cur.close()
    conn.close()

# Ініціалізуємо БД
try:
    init_db()
except Exception as e:
    print(f'[DB] Помилка ініціалізації: {e}')

# ═══════════════════════════════════════
# TELEGRAM CLIENT (StringSession — зберігається в БД)
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
    saved_session = None
    try:
        saved_session = db_get_session()
    except Exception:
        pass
    session = StringSession(saved_session or '')
    tg_client = TelegramClient(session, API_ID, API_HASH)
    await tg_client.connect()
    if await tg_client.is_user_authorized():
        tg_connected = True
        me = await tg_client.get_me()
        print(f'[TG] Підключено як: {me.first_name} ({me.phone})')
        try:
            db_save_session(tg_client.session.save())
        except Exception:
            pass
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
    try:
        db_save_session(tg_client.session.save())
        print('[TG] Сесію збережено в БД')
    except Exception as e:
        print(f'[TG] Помилка збереження сесії: {e}')

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

async def _get_topics(chat_identifier):
    """Отримати список топіків (тем) в групі"""
    from telethon.tl.functions.channels import GetForumTopicsRequest
    entity = chat_identifier
    try:
        entity = int(chat_identifier)
    except (ValueError, TypeError):
        pass
    target = await tg_client.get_entity(entity)
    result = await tg_client(GetForumTopicsRequest(
        channel=target,
        offset_date=0,
        offset_id=0,
        offset_topic=0,
        limit=100,
    ))
    topics = []
    for t in result.topics:
        topics.append({
            'id': t.id,
            'title': t.title,
        })
    return topics

async def _get_slowmode(chat_identifier):
    """Отримати slowmode_seconds з чату/каналу"""
    from telethon.tl.functions.channels import GetFullChannelRequest
    entity = chat_identifier
    try:
        entity = int(chat_identifier)
    except (ValueError, TypeError):
        pass
    target = await tg_client.get_entity(entity)
    full = await tg_client(GetFullChannelRequest(target))
    slowmode = getattr(full.full_chat, 'slowmode_seconds', 0) or 0
    return slowmode

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
    lines = []
    lines.append('🚨🚨🚨🚨🚨🚨')
    lines.append(f'🚛  ЗАЯВКА #{num}')
    lines.append('🚨🚨🚨🚨🚨🚨')
    from_parts = [order.get('fromCity', ''), order.get('fromVil', '')]
    to_parts   = [order.get('toCity', ''), order.get('toVil', '')]
    from_str = ', '.join(p for p in from_parts if p)
    to_str   = ', '.join(p for p in to_parts   if p)
    route = ' → '.join(p for p in [from_str, to_str] if p)
    dist = order.get('dist', '')
    if route and dist:
        lines.append(f'📍 {route} ({dist} км)')
    elif route:
        lines.append(f'📍 {route}')
    elif dist:
        lines.append(f'📍 {dist} км')
    cargo_parts = [order.get('cargo', ''), order.get('weight', '')]
    cargo_str = ' — '.join(p for p in cargo_parts if p)
    if cargo_str:
        lines.append(f'📦 {cargo_str}')
    trucks = order.get('trucks', [])
    count = order.get('count', '1')
    if trucks:
        truck_str = ', '.join(trucks)
        if count and str(count) != '1':
            lines.append(f'🚚 {truck_str} · {count} маш.')
        else:
            lines.append(f'🚚 {truck_str}')
    elif count and str(count) != '1':
        lines.append(f'🚚 {count} маш.')
    tariff = order.get('tariff', 'pdv')
    if tariff == 'ask':
        lines.append('💰 Ціна за запитом')
    else:
        price_parts = []
        if order.get('pdv'):
            price_parts.append(f'ПДВ {order["pdv"]}')
        if order.get('fop'):
            price_parts.append(f'ФОП {order["fop"]}')
        if price_parts:
            lines.append(f'💰 {" / ".join(price_parts)} грн')
    d1 = fmt_date(order.get('date1', ''))
    d2 = fmt_date(order.get('date2', ''))
    dates = ' — '.join(p for p in [d1, d2] if p)
    if dates:
        lines.append(f'📅 {dates}')
    if order.get('note'):
        lines.append(f'📝 {order["note"]}')
    if contact:
        lines.append(f'📞 {contact}')
    return '\n'.join(lines)

def build_t1_message(orders, contact):
    active = [o for o in orders if o.get('active') and o.get('status') == 'active']
    if not active:
        return None
    invisible = '\u200b' * random.randint(1, 5)
    msg = '\n\n❇️❇️❇️❇️❇️❇️\n\n'.join(
        build_order_text(o, i + 1, contact) for i, o in enumerate(active)
    )
    return msg + invisible

def can_send(chat):
    last = chat.get('lastSent')
    if not last:
        return True
    limit_ms = chat.get('limit', 30) * 60 * 1000
    now_ms = int(now_ua().timestamp() * 1000)
    return (now_ms - last) >= limit_ms

async def _do_send(chat_identifier, text, topic_id=None):
    try:
        entity = chat_identifier
        try:
            entity = int(chat_identifier)
        except (ValueError, TypeError):
            pass
        kwargs = {'parse_mode': None}
        if topic_id:
            kwargs['reply_to'] = int(topic_id)
        await tg_client.send_message(entity, text, **kwargs)
        return True, None
    except FloodWaitError as e:
        print(f'[TG] FloodWait: чекаємо {e.seconds} сек')
        await asyncio.sleep(e.seconds + 1)
        return False, f'FloodWait {e.seconds}с — чекаємо'
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
        dispatch = db_get_dispatch()
        if not dispatch['running']:
            return
        settings = db_get_settings()
        contact  = build_contact(settings)
        orders   = db_get_orders()
        ads      = db_get_ads()
        chats1   = db_get_chats(1)
        chats2   = db_get_chats(2)

        # ── ТИП 1: заявки → перевізники ──
        t1_msg = build_t1_message(orders, contact)
        if t1_msg:
            ready1 = [c for c in chats1 if c.get('active') and can_send(c)]
            if ready1:
                chat = ready1[0]
                topic_id = chat.get('topic_id')
                ok, err = tg_run(_do_send(
                    chat.get('user') or chat.get('id', ''),
                    t1_msg,
                    topic_id
                ))
                now_ms = int(now_ua().timestamp() * 1000)
                db_update_chat(1, chat['id'], {'lastSent': now_ms})
                if ok:
                    db_inc_stat('sent_today')
                    db_inc_stat('t1_today')
                    order_labels = [
                        f'#{i+1} {o.get("cargo", "")}'
                        for i, o in enumerate(orders)
                        if o.get('active') and o.get('status') == 'active'
                    ]
                    db_add_log('t1', chat.get('name', ''), '✅ Відправлено', order_labels)
                    print(f'[T1] → {chat.get("name", "")} ✅')
                else:
                    db_add_log('t1', chat.get('name', ''), f'❌ Помилка: {err}')
                    print(f'[T1] → {chat.get("name", "")} ❌ {err}')

                import time
                time.sleep(random.randint(5, 15))

        # ── ТИП 2: реклама → клієнти ──
        active_ads = [a for a in ads if a.get('active')]
        if active_ads:
            ad_text = active_ads[0].get('text', '')
            if ad_text:
                ready2 = [c for c in chats2 if c.get('active') and can_send(c)]
                if ready2:
                    chat = ready2[0]
                    topic_id = chat.get('topic_id')
                    ok, err = tg_run(_do_send(
                        chat.get('user') or chat.get('id', ''),
                        ad_text,
                        topic_id
                    ))
                    now_ms = int(now_ua().timestamp() * 1000)
                    db_update_chat(2, chat['id'], {'lastSent': now_ms})
                    if ok:
                        db_inc_stat('sent_today')
                        db_inc_stat('t2_today')
                        db_add_log('t2', chat.get('name', ''), '✅ Відправлено')
                        print(f'[T2] → {chat.get("name", "")} ✅')
                    else:
                        db_add_log('t2', chat.get('name', ''), f'❌ Помилка: {err}')
                        print(f'[T2] → {chat.get("name", "")} ❌ {err}')

def check_auto_schedule():
    dispatch = db_get_dispatch()
    if dispatch['mode'] != 'auto':
        return
    settings = db_get_settings()
    now_time = now_ua().strftime('%H:%M')
    start = settings.get('start', '08:00')
    end   = settings.get('end',   '19:00')
    should_run = start <= now_time < end
    if should_run and not dispatch['running']:
        db_set_dispatch('running', 'true')
        db_add_log('sys', '⚙️ Система', f'✅ Авто-запуск ({start})')
        print(f'[AUTO] Запуск о {now_time}')
    elif not should_run and dispatch['running'] and dispatch['mode'] == 'auto':
        db_set_dispatch('running', 'false')
        stats = db_get_stats()
        db_add_log('sys', '⚙️ Система', f'⏹ Авто-зупинка ({end}). Відправлено: {stats["sent_today"]}')
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
# ГОЛОВНА СТОРІНКА
# ═══════════════════════════════════════
@app.route('/')
def serve_index():
    return send_from_directory('static', 'index.html')

# ═══════════════════════════════════════
# API РОУТИ
# ═══════════════════════════════════════

@app.route('/api/state', methods=['GET'])
def api_state():
    try:
        dispatch = db_get_dispatch()
        stats = db_get_stats()
        settings = db_get_settings()
        return jsonify({
            'tg_connected': tg_connected,
            'running':  dispatch['running'],
            'mode':     dispatch['mode'],
            'stats':    stats,
            'settings': settings,
        })
    except Exception as e:
        print(f'[API] /api/state error: {e}')
        return jsonify({
            'tg_connected': False, 'running': False, 'mode': 'auto',
            'stats': {'sent_today': 0, 't1_today': 0, 't2_today': 0},
            'settings': {'name': '', 'phone': '', 'start': '08:00', 'end': '19:00'},
        })

@app.route('/api/settings', methods=['GET', 'POST'])
def api_settings():
    try:
        if request.method == 'POST':
            db_set_settings(request.json)
            return jsonify({'ok': True})
        return jsonify(db_get_settings())
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/orders', methods=['GET'])
def api_orders_get():
    try:
        return jsonify(db_get_orders())
    except Exception as e:
        print(f'[API] orders GET error: {e}')
        return jsonify([])

@app.route('/api/orders', methods=['POST'])
def api_orders_post():
    try:
        order = request.json
        oid = db_add_order(order)
        return jsonify({'ok': True, 'id': oid})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/orders/<int:oid>', methods=['PUT'])
def api_orders_put(oid):
    try:
        db_update_order(oid, request.json)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/orders/<int:oid>', methods=['DELETE'])
def api_orders_delete(oid):
    try:
        db_delete_order(oid)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/ads', methods=['GET'])
def api_ads_get():
    try:
        return jsonify(db_get_ads())
    except Exception as e:
        return jsonify([])

@app.route('/api/ads', methods=['POST'])
def api_ads_post():
    try:
        ad = request.json
        aid = db_add_ad(ad)
        return jsonify({'ok': True, 'id': aid})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/ads/<int:aid>', methods=['PUT'])
def api_ads_put(aid):
    try:
        db_update_ad(aid, request.json)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/ads/<int:aid>', methods=['DELETE'])
def api_ads_delete(aid):
    try:
        db_delete_ad(aid)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/chats/<int:t>', methods=['GET'])
def api_chats_get(t):
    try:
        return jsonify(db_get_chats(t))
    except Exception as e:
        return jsonify([])

@app.route('/api/chats/<int:t>', methods=['POST'])
def api_chats_post(t):
    try:
        chat = request.json
        cid = db_add_chat(t, chat)
        return jsonify({'ok': True, 'id': cid})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/chats/<int:t>/<int:cid>', methods=['PUT'])
def api_chats_put(t, cid):
    try:
        db_update_chat(t, cid, request.json)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/chats/<int:t>/<int:cid>', methods=['DELETE'])
def api_chats_delete(t, cid):
    try:
        db_delete_chat(t, cid)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/dispatch/start', methods=['POST'])
def api_dispatch_start():
    try:
        db_set_dispatch('running', 'true')
        db_add_log('sys', '⚙️ Система', '✅ Розсилку запущено вручну')
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/dispatch/stop', methods=['POST'])
def api_dispatch_stop():
    try:
        db_set_dispatch('running', 'false')
        stats = db_get_stats()
        db_add_log('sys', '⚙️ Система', f'⏹ Розсилку зупинено. Відправлено: {stats["sent_today"]}')
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/dispatch/mode', methods=['POST'])
def api_dispatch_mode():
    try:
        db_set_dispatch('mode', request.json.get('mode', 'auto'))
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/log', methods=['GET'])
def api_log():
    try:
        return jsonify(db_get_log())
    except Exception as e:
        return jsonify([])

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

@app.route('/api/tg/topics', methods=['POST'])
def api_tg_topics():
    if not tg_connected:
        return jsonify({'ok': False, 'error': 'Telegram не підключений'}), 400
    chat_id = request.json.get('chat', '')
    if not chat_id:
        return jsonify({'ok': False, 'error': 'Вкажи username або ID чату'}), 400
    try:
        topics = tg_run(_get_topics(chat_id))
        return jsonify({'ok': True, 'topics': topics})
    except Exception as e:
        err = str(e)
        if 'CHANNEL_FORUM_MISSING' in err or 'not a forum' in err.lower():
            return jsonify({'ok': True, 'topics': [], 'message': 'Цей чат не має тем (топіків)'})
        return jsonify({'ok': False, 'error': err}), 400

@app.route('/api/tg/slowmode', methods=['POST'])
def api_tg_slowmode():
    """Отримати slowmode_seconds для чату"""
    if not tg_connected:
        return jsonify({'ok': False, 'error': 'Telegram не підключений'}), 400
    chat_id = request.json.get('chat', '')
    if not chat_id:
        return jsonify({'ok': False, 'error': 'Вкажи username або ID чату'}), 400
    try:
        slowmode = tg_run(_get_slowmode(chat_id))
        return jsonify({'ok': True, 'slowmode': slowmode})
    except Exception as e:
        err = str(e)
        if 'CHANNEL_INVALID' in err or 'megagroup' in err.lower():
            return jsonify({'ok': True, 'slowmode': 0, 'message': 'Slowmode не підтримується для цього чату'})
        if 'ChatIdInvalidError' in err or 'PeerIdInvalid' in err:
            return jsonify({'ok': True, 'slowmode': 0})
        return jsonify({'ok': False, 'error': err}), 400

# ═══════════════════════════════════════
# ЗАПУСК
# ═══════════════════════════════════════
if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    print(f'[SERVER] Запуск на порту {port}')
    app.run(host='0.0.0.0', port=port, debug=False)
