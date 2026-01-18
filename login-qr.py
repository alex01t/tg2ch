#!/usr/bin/env python3

import os
import qrcode
import getpass
import asyncio
from telethon import TelegramClient
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


def print_qr_ascii(data: str):
    qr = qrcode.QRCode(border=1)
    qr.add_data(data)
    qr.make(fit=True)

    m = qr.get_matrix()  # list[list[bool]]
    black = "██"
    white = "  "

    # top quiet zone already included by border=1
    for row in m:
        print("".join(black if cell else white for cell in row))


async def main():
    os.makedirs(TG_SESSION_DIR, exist_ok=True)
    client = TelegramClient(TG_SESSION_PATH, TG_API_ID, TG_API_HASH)
    await client.connect()

    if await client.is_user_authorized():
        print("Already logged in")
        return

    print("Generating QR code…")
    qr_login = await client.qr_login()

    # This prints a terminal QR code (works in most terminals)
    print_qr_ascii(qr_login.url)

    print("Open Telegram app → Settings → Devices → Link Desktop Device")
    print("Scan the QR code")


    try:
        await qr_login.wait()
    except SessionPasswordNeededError:
        # Telegram account has 2FA enabled
        pw = os.getenv("TG_2FA_PASSWORD") or getpass.getpass("Telegram 2FA password: ")
        await client.sign_in(password=pw)

    print("Logged in successfully")

    await client.disconnect()

asyncio.run(main())
