import os
import qrcode
import signal
import asyncio
import clickhouse_connect
from datetime import datetime, timezone
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError

def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise SystemExit(f"Missing required env var: {name}")
    return v

TG_API_ID = int(env("TG_API_ID"))
TG_API_HASH = env("TG_API_HASH")
TG_SESSION_DIR = env("TG_SESSION_DIR", "./session")
TG_SESSION_PATH = os.path.join(TG_SESSION_DIR, "telethon.session")

CH_HOST = env("CH_HOST", "localhost")
CH_PORT = int(env("CH_PORT", "8123"))
CH_USER = env("CH_USER", "default")
CH_PASSWORD = env("CH_PASSWORD", "")
CH_DATABASE = env("CH_DATABASE", "tg")
CH_TABLE = env("CH_TABLE", "channel_messages")

CHANNELS_RAW = env("TG_CHANNELS", "")
CHANNELS = [c.strip() for c in CHANNELS_RAW.split(",") if c.strip()]

STORE_EMPTY = os.getenv("STORE_EMPTY", "0") == "1"

CREATE_DB_SQL = f"CREATE DATABASE IF NOT EXISTS {CH_DATABASE}"
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {CH_DATABASE}.{CH_TABLE} (
  ts DateTime64(3, 'UTC'),
  channel String,
  msg String
)
ENGINE = MergeTree
ORDER BY (channel, ts)
"""


def print_qr_ascii(data: str):
    qr = qrcode.QRCode(border=1)
    qr.add_data(data)
    qr.make(fit=True)
    m = qr.get_matrix()  # list[list[bool]]
    black = "██"
    white = "  "
    for row in m:
        print("".join(black if cell else white for cell in row))


def ch_client():
    # HTTP interface (no auth)
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
    )

async def ensure_clickhouse():
    client = ch_client()
    client.command(CREATE_DB_SQL)
    client.command(CREATE_TABLE_SQL)

def normalize_channel_name(event) -> str:
    # prefer username if exists, fallback to title/id
    ch = getattr(event, "chat", None)
    if ch is None:
        return "unknown"
    if getattr(ch, "username", None):
        return f"@{ch.username}"
    if getattr(ch, "title", None):
        return ch.title
    if getattr(ch, "id", None):
        return str(ch.id)
    return "unknown"


async def main():

    await ensure_clickhouse()
    client_ch = ch_client()

    os.makedirs(TG_SESSION_DIR, exist_ok=True)
    tg = TelegramClient(TG_SESSION_PATH, TG_API_ID, TG_API_HASH)

    await tg.connect()
    print("Connected")

    if await tg.is_user_authorized():
        print("Logged in")
    else:

        print("Generating QR code…")
        qr_login = await tg.qr_login()

        print_qr_ascii(qr_login.url)

        print("Open Telegram app → Settings → Devices → Link Desktop Device")
        print("Scan the QR code")

        try:
            await qr_login.wait()
        except SessionPasswordNeededError:
            # Telegram account has 2FA enabled
            pw = env("TG_2FA_PASSWORD")
            await tg.sign_in(password=pw)

        print("Logged in successfully")


    # Resolve channels upfront (ensures you’re subscribed and name is valid)
    entities = []
    for c in CHANNELS:
        ent = await tg.get_entity(c)
        entities.append(ent)

    @tg.on(events.NewMessage(chats=entities))
    async def handler(event):
        text = event.raw_text or ""
        if (not STORE_EMPTY) and (not text.strip()):
            return

        ts = event.date
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        ts_utc = ts.astimezone(timezone.utc)

        print(f'{event}')

        row = [[ts_utc, normalize_channel_name(event), text]]

        # Insert
        client_ch.insert(
            table=f"{CH_DATABASE}.{CH_TABLE}",
            data=row,
            column_names=["ts", "channel", "msg"],
        )

    print(f"Listening to {len(entities)} channels: {', '.join(CHANNELS)}")
    await tg.run_until_disconnected()
    print('Disconnected')

if __name__ == "__main__":
    asyncio.run(main())
