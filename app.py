import os
import qrcode
import signal
import asyncio
import clickhouse_connect
import logging
from datetime import datetime, timedelta, timezone
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

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

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

def normalize_channel_name(chat) -> str:
    # prefer username if exists, fallback to title/id
    if chat is None:
        return "unknown"
    if getattr(chat, "username", None):
        return f"@{chat.username}"
    if getattr(chat, "title", None):
        return chat.title
    if getattr(chat, "id", None):
        return str(chat.id)
    return "unknown"

def get_last_timestamp(client, channel_name: str):
    query = (
        f"SELECT max(ts) AS max_ts "
        f"FROM {CH_DATABASE}.{CH_TABLE} "
        "WHERE channel = %(channel)s"
    )
    result = client.query(query, parameters={"channel": channel_name})
    if not result.result_rows:
        return None
    last_ts = result.result_rows[0][0]
    if last_ts is None:
        return None
    if last_ts.tzinfo is None:
        last_ts = last_ts.replace(tzinfo=timezone.utc)
    return last_ts.astimezone(timezone.utc)

async def backfill_channel(tg, client_ch, entity):
    channel_name = normalize_channel_name(entity)
    last_ts = get_last_timestamp(client_ch, channel_name)
    if last_ts is None:
        start_time = datetime.now(timezone.utc) - timedelta(days=7)
        logger.info("Backfilling %s for last 7 days", channel_name)
    else:
        start_time = last_ts
        logger.info("Backfilling %s since %s", channel_name, start_time.isoformat())

    rows = []
    async for message in tg.iter_messages(entity, reverse=True, offset_date=start_time):
        text = message.raw_text or ""
        if (not STORE_EMPTY) and (not text.strip()):
            continue

        ts = message.date
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        ts_utc = ts.astimezone(timezone.utc)

        if last_ts is not None and ts_utc <= last_ts:
            continue

        rows.append([ts_utc, channel_name, text])
        if len(rows) >= 1000:
            client_ch.insert(
                table=f"{CH_DATABASE}.{CH_TABLE}",
                data=rows,
                column_names=["ts", "channel", "msg"],
            )
            rows.clear()

    if rows:
        client_ch.insert(
            table=f"{CH_DATABASE}.{CH_TABLE}",
            data=rows,
            column_names=["ts", "channel", "msg"],
        )


async def main():

    await ensure_clickhouse()
    client_ch = ch_client()

    os.makedirs(TG_SESSION_DIR, exist_ok=True)
    tg = TelegramClient(TG_SESSION_PATH, TG_API_ID, TG_API_HASH)

    await tg.connect()
    logger.info("Connected")

    if await tg.is_user_authorized():
        logger.info("Logged in")
    else:

        logger.info("Generating QR code…")
        qr_login = await tg.qr_login()

        print_qr_ascii(qr_login.url)

        logger.info("Open Telegram app → Settings → Devices → Link Desktop Device")
        logger.info("Scan the QR code")

        try:
            await qr_login.wait()
        except SessionPasswordNeededError:
            # Telegram account has 2FA enabled
            pw = env("TG_2FA_PASSWORD")
            await tg.sign_in(password=pw)

        logger.info("Logged in successfully")


    # Resolve channels upfront (ensures you’re subscribed and name is valid)
    entities = []
    for c in CHANNELS:
        ent = await tg.get_entity(c)
        entities.append(ent)

    for ent in entities:
        await backfill_channel(tg, client_ch, ent)

    @tg.on(events.NewMessage(chats=entities))
    async def handler(event):
        text = event.raw_text or ""
        if (not STORE_EMPTY) and (not text.strip()):
            return

        ts = event.date
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        ts_utc = ts.astimezone(timezone.utc)

        logger.info("%s", event)

        row = [[ts_utc, normalize_channel_name(event.chat), text]]

        # Insert
        client_ch.insert(
            table=f"{CH_DATABASE}.{CH_TABLE}",
            data=row,
            column_names=["ts", "channel", "msg"],
        )

    logger.info("Listening to %s channels: %s", len(entities), ", ".join(CHANNELS))
    await tg.run_until_disconnected()
    logger.info("Disconnected")

if __name__ == "__main__":
    asyncio.run(main())
