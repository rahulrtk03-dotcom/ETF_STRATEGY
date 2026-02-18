import os
import pandas as pd
import requests
import schedule
import time
from datetime import datetime

# ================== CONFIG ==================

SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1KPbQBEdz5mJlB9prdFNn7_6_5F8gA4uN3DvAXUvAeII"
    "/gviz/tq?tqx=out:csv&sheet=ETF_Sip_Strategy"
)

# 🔐 Telegram (Render Environment Variables)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

TOP_N = 10

# ⏰ Auto run times (24-hour format, IST)
RUN_TIMES = ["10:00", "14:55"]

# 🗓 Weekday skip
# Monday=0 ... Sunday=6
# Default: Saturday & Sunday skip
SKIP_WEEKDAYS = [5, 6]

# ================== TELEGRAM ==================

def telegram_enabled():
    return TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID


def send_telegram(msg):
    if not telegram_enabled():
        print("⚠️ Telegram condition fail: Token / Chat ID missing")
        return

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=10
        )
        print("✅ Telegram message sent")
    except Exception as e:
        print("❌ Telegram error:", e)

# ================== SHEET ==================

def get_last_update():
    raw = pd.read_csv(SHEET_URL, header=None)
    for _, row in raw.iterrows():
        if str(row[0]).strip().lower() == "last update":
            return str(row[1])
    return "Unknown"


def load_data():
    raw = pd.read_csv(SHEET_URL, header=None)

    start = None
    for i, row in raw.iterrows():
        if str(row[0]).startswith("NSE:"):
            start = i
            break

    if start is None:
        raise Exception("ETF data not found")

    df = raw.iloc[start:].reset_index(drop=True)
    df = df.iloc[:, 0:7]
    df.columns = ["ETF", "ASSET", "CMP", "LOW", "DIFF", "PCT", "RANK"]
    return df


def fetch_and_rank():
    df = load_data()
    df["CMP"] = pd.to_numeric(df["CMP"], errors="coerce")
    df["PCT"] = pd.to_numeric(df["PCT"], errors="coerce")
    df = df.dropna(subset=["CMP", "PCT"])
    df = df.sort_values("PCT")
    return df.head(TOP_N)

# ================== OUTPUT ==================

def print_terminal(df, last_update):
    print("\n" + "=" * 65)
    print("📊 ETF BUY LIST (Rank 1–10) | 52W Low Strategy")
    print(f"🕒 Last Update: {last_update}")
    print(f"⏱ Run Time: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")
    print("=" * 65)

    for i, r in enumerate(df.itertuples(), 1):
        print(
            f"{i:>2}. {r.ETF:<18} "
            f"CMP: ₹{r.CMP:>8.2f} "
            f"| From 52W Low: {r.PCT:>6.2f}%"
        )

    print("=" * 65)


def build_message(df, last_update):
    msg = f"📊 ETF BUY LIST (Rank 1–10)\n🕒 Last Update: {last_update}\n\n"
    for i, r in enumerate(df.itertuples(), 1):
        msg += (
            f"{i}. {r.ETF}\n"
            f"CMP: ₹{r.CMP:.2f}\n"
            f"From 52W Low: {r.PCT:.2f}%\n\n"
        )
    return msg

# ================== JOB ==================

def run_job():
    today = datetime.now().weekday()

    if today in SKIP_WEEKDAYS:
        print(f"⛔ Skipped Today (weekday={today})")
        return

    try:
        df = fetch_and_rank()
        last_update = get_last_update()
        print_terminal(df, last_update)
        send_telegram(build_message(df, last_update))
    except Exception as e:
        print("❌ Job failed:", e)

# ================== SCHEDULER ==================

print("🚀 ETF Auto Runner Started")
print("⏰ Scheduled Times:", RUN_TIMES)
print("🚫 Skip Weekdays:", SKIP_WEEKDAYS)

for t in RUN_TIMES:
    schedule.every().day.at(t).do(run_job)

# ❌ No first run – sirf scheduled time pe hi chalega

while True:
    schedule.run_pending()
    time.sleep(1)
