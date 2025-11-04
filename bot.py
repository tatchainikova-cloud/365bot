import os, re, sqlite3, asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from vkbottle.bot import Bot, Message
from vkbottle import API

# === Конфиг ===
load_dotenv()
TOKEN = os.getenv("VK_GROUP_TOKEN")                           # токен сообщества
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS","").replace(" ","").split(",") if x}  # твой числовой id
MSK = ZoneInfo("Europe/Moscow")

bot = Bot(token=TOKEN)
api = API(token=TOKEN)

# === БД ===
DB = sqlite3.connect("bot.db", check_same_thread=False)
DB.execute("""
CREATE TABLE IF NOT EXISTS participants(
  chat_id INTEGER,
  user_id INTEGER,      -- может быть NULL до сопоставления
  name   TEXT,          -- "Фамилия Имя" как в /start
  active INTEGER DEFAULT 1,     -- участвует (не выбывал)
  PRIMARY KEY(chat_id, name)    -- ключ по имени в этом чате
);""")
DB.execute("""
CREATE TABLE IF NOT EXISTS reports(
  chat_id INTEGER,
  user_id INTEGER,
  gdate  TEXT,          -- игровой день (срез 06:00 МСК)
  photos INTEGER DEFAULT 0,
  PRIMARY KEY(chat_id, user_id, gdate)
);""")
DB.commit()

# === Вспомогательные ===
def is_admin(uid:int)->bool: return uid in ADMIN_IDS
def now_msk()->datetime: return datetime.now(MSK)
def game_date(ts:datetime|None=None)->str:
    ts = ts or now_msk()
    if ts.hour < 6: ts -= timedelta(days=1)
    return ts.date().isoformat()
def yday_game_date()->str: return game_date(now_msk()-timedelta(days=1))

async def send(chat_id:int, text:str):
    try:
        await bot.api.messages.send(peer_id=chat_id, message=text, random_id=0)
    except Exception:
        pass

def norm_name(s:str)->str:
    # упрощённая нормализация "Фамилия Имя"
    s = re.sub(r"\s+", " ", s.strip())
    return s

async def fetch_chat_members(chat_id:int)->dict[int,str]:
    """Возвращает {user_id: 'Фамилия Имя'} для текущей беседы."""
    res = {}
    try:
        data = await bot.api.request("messages.getConversationMembers", {"peer_id": chat_id})
        profiles = data.get("profiles", []) if isinstance(data, dict) else []
        for p in profiles:
            uid = int(p["id"])
            name = f"{p.get('last_name','')} {p.get('first_name','')}".strip()
            res[uid] = name
    except Exception:
        pass
    return res

def add_photos(chat_id:int, user_id:int, add:int, gdate:str):
    row = DB.execute("SELECT photos FROM reports WHERE chat_id=? AND user_id=? AND gdate=?",
                     (chat_id, user_id, gdate)).fetchone()
    cur = row[0] if row else 0
    newv = min(2, cur + add)
    if newv != cur:
        DB.execute("INSERT OR REPLACE INTO reports(chat_id,user_id,gdate,photos) VALUES (?,?,?,?)",
                   (chat_id, user_id, gdate, newv))
        DB.commit()

def remaining_today_names(chat_id:int)->list[str]:
    g = game_date()
    rows = DB.execute("SELECT name, user_id FROM participants WHERE chat_id=? AND active=1", (chat_id,)).fetchall()
    left = []
    for name, uid in rows:
        if not uid:
            left.append(name)  # ещё не сопоставлён — считаем как «нет отчёта»
            continue
        r = DB.execute("SELECT photos FROM reports WHERE chat_id=? AND user_id=? AND gdate=?",
                       (chat_id, uid, g)).fetchone()
        if (r[0] if r else 0) < 2:
            left.append(name)
    return left

def mark_failed(chat_id:int, gdate:str)->list[str]:
    rows = DB.execute("SELECT name, user_id FROM participants WHERE chat_id=? AND active=1", (chat_id,)).fetchall()
    failed = []
    for name, uid in rows:
        photos = 0
        if uid:
            r = DB.execute("SELECT photos FROM reports WHERE chat_id=? AND user_id=? AND gdate=?",
                           (chat_id, uid, gdate)).fetchone()
            photos = r[0] if r else 0
        if photos < 2:
            DB.execute("UPDATE participants SET active=0 WHERE chat_id=? AND name=?", (chat_id, name))
            failed.append(name)
    DB.commit()
    return failed

async def scan_today_history(chat_id:int):
    """Разовый просмотр истории с 06:00 МСК за СЕГОДНЯ. Учитываем только имена из списка."""
    # сопоставим имена с айди по текущему составу беседы
    members = await fetch_chat_members(chat_id)               # {uid: 'Фамилия Имя'}
    rev_map = {norm_name(v).lower(): k for k, v in members.items()}

    # заполним user_id там, где удаётся по имени
    rows = DB.execute("SELECT name FROM participants WHERE chat_id=?", (chat_id,)).fetchall()
    for (name,) in rows:
        uid = rev_map.get(norm_name(name).lower())
        if uid:
            DB.execute("UPDATE participants SET user_id=? WHERE chat_id=? AND name=?", (uid, chat_id, name))
    DB.commit()

    # время начала дня
    start = now_msk().replace(hour=6, minute=0, second=0, microsecond=0)
    if now_msk().hour < 6:
        start -= timedelta(days=1)
    start_ts = int(start.timestamp())

    # прокрутка истории вверх
    offset, count = 0, 200
    while True:
        resp = await bot.api.request("messages.getHistory", {"peer_id": chat_id, "count": count, "offset": offset, "rev": 1})
        items = resp.get("items", []) if isinstance(resp, dict) else []
        if not items: break
        for it in items:
            if it.get("date", 0) < start_ts:
                continue
            uid = it.get("from_id")
            if not uid:
                continue
            # учитываем только тех, кто в participants активный и с таким uid
            row = DB.execute("SELECT 1 FROM participants WHERE chat_id=? AND user_id=? AND active=1",
                             (chat_id, uid)).fetchone()
            if not row:
                continue
            atts = it.get("attachments", []) or []
            photos = sum(1 for a in atts if a.get("type") == "photo")
            if photos:
                add_photos(chat_id, uid, photos, game_date())
        if len(items) < count:
            break
        offset += count

# === Расписание: 22:00 напоминалка, 06:01 выбыли, 1-е число 06:02 «финалисты» ===
async def scheduler():
    await asyncio.sleep(2)
    while True:
        now = now_msk()
        at_22   = (now.replace(hour=22, minute=0, second=0, microsecond=0) + (timedelta(days=1) if now >= now.replace(hour=22,minute=0,second=0,microsecond=0) else timedelta(0)))
        at_0601 = (now.replace(hour=6,  minute=1, second=0, microsecond=0) + (timedelta(days=1) if now >= now.replace(hour=6,minute=1,second=0,microsecond=0) else timedelta(0)))
        # событие на «финалистов»: 1-е число 06:02
        at_final = now.replace(day=1, hour=6, minute=2, second=0, microsecond=0)
        if now >= at_final:
            # следующее 1-е число
            month = (now.month % 12) + 1
            year  = now.year + (1 if now.month==12 else 0)
            at_final = at_final.replace(year=year, month=month)

        wake = min(at_22, at_0601, at_final)
        await asyncio.sleep((wake - now).total_seconds() + 0.5)

        chats = [r[0] for r in DB.execute("SELECT DISTINCT chat_id FROM participants").fetchall()]

        if wake == at_22:
            for chat in chats:
                names = remaining_today_names(chat)
                if names:
                    await send(chat, "🕙 @all Напоминалка: ещё не отчитались (2 фото):\n• " + "\n• ".join(names))
        elif wake == at_0601:
            g = yday_game_date()
            for chat in chats:
                failed = mark_failed(chat, g)
                if failed:
                    await send(chat, f"⛳️ Итоги за игровой день {g} (срез 06:00 МСК)\n"
                                     "Выбывают (нет 2 фото):\n• " + "\n• ".join(failed) +
                                     "\nОни могут писать в чат, но бот их больше не учитывает.")
        else:
            # список финалистов: все, кто до сих пор active=1
            for chat in chats:
                cur = DB.execute("SELECT name FROM participants WHERE chat_id=? AND active=1 ORDER BY name", (chat,)).fetchall()
                names = [n[0] for n in cur]
                if names:
                    await send(chat, "🏁 Список финалистов (не пропускали весь месяц):\n• " + "\n• ".join(names))

bot.loop_wrapper.add_task(scheduler())

# === Команда /start (только админ): список «Фамилия Имя» через запятую или с новой строки ===
@bot.on.message(text="/start <tail>")
async def start_cmd(m: Message, tail: str):
    if not is_admin(m.from_id):
        return  # без ответов
    chat = m.peer_id
    raw = [norm_name(x) for x in re.split(r"[,\n;]+", tail) if norm_name(x)]
    if not raw:
        return
    # сохранить список как активных (перезаписать состав)
    DB.execute("DELETE FROM participants WHERE chat_id=?", (chat,))
    for name in raw:
        DB.execute("INSERT OR REPLACE INTO participants(chat_id,name,active) VALUES (?,?,1)", (chat, name))
    DB.commit()
    # разово сканировать историю сегодняшнего дня и сопоставить айди
    await scan_today_history(chat)

# === Невидимый сборщик новых фото (молча) ===
@bot.on.message()
async def collector(m: Message):
    if m.peer_id <= 2000000000:
        return
    # узнаем, есть ли этот пользователь среди активных по имени (уже сопоставленных)
    row = DB.execute("SELECT name FROM participants WHERE chat_id=? AND user_id=? AND active=1",
                     (m.peer_id, m.from_id)).fetchone()
    if not row:
        return
    add = sum(1 for a in (m.attachments or []) if getattr(a, "photo", None))
    if add:
        add_photos(m.peer_id, m.from_id, add, game_date())

bot.run_forever()
