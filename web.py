import os
import uuid
import re
import hashlib
import hmac
import logging
import requests
import json
import time
import urllib.parse
import base64
import html
import random
import io
import resend
from groq import Groq
from openpyxl import Workbook
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from flask import Flask, request, render_template, render_template_string, abort, jsonify, redirect, send_from_directory, session, Response, stream_with_context
from concurrent.futures import ThreadPoolExecutor

# Kriptografiya kutubxonalari
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from db import DB

# ==========================================
# 1. KONFIGURATSIYA VA SOZLAMALAR
# ==========================================
WEB_BASE_URL = os.getenv("WEB_BASE_URL", "http://localhost:8000")

tokens_env = os.getenv("BOT_TOKENS", os.getenv("BOT_TOKEN", ""))
BOT_TOKENS = [t.strip() for t in tokens_env.split(",") if t.strip()]
BOT_TOKEN_MAIN = BOT_TOKENS[0] if BOT_TOKENS else ""

# Majburiy kanallarni bazadan yuklash funksiyasi (web.py uchun)
def load_required_channels_from_db():
    """Bazadan aktiv kanallarni yuklash"""
    try:
        # db obyekti pastda yaratiladi, shuning uchun lazy loading qilamiz
        if 'db' in globals():
            return db.get_active_channel_ids()
        return []
    except Exception as e:
        logging.error(f"Kanallarni yuklashda xato (web.py): {e}")
        return []

REQUIRED_CHANNELS = []  # Boshida bo'sh, keyin yuklaydi

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
MODEL_NAME = "nvidia/nemotron-3-super-120b-a12b:free"

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
RECAPTCHA_SECRET_KEY = os.getenv("RECAPTCHA_SECRET_KEY", "")

# RESEND API KONFIGURATSIYASI
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
resend.api_key = RESEND_API_KEY

groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None

ADMIN_CARD = os.getenv("ADMIN_CARD", "0000 0000 0000 0000 (Ism Familiya)")

SUPERADMINS = {int(x) for x in (os.getenv("SUPERADMINS", "") or "").split(",") if x.strip().isdigit()}
LOWER_ADMINS = {int(x) for x in (os.getenv("LOWER_ADMINS", "") or "").split(",") if x.strip().isdigit()}

FORBIDDEN_WORDS = ["behayo", "porn", "qimor", "teror", "1xbet", "xxx", "bomba", "seks"]

TZ = ZoneInfo("Asia/Tashkent")

def is_clean_content(text):
    if not text:
        return True
    text_lower = text.lower()
    for word in FORBIDDEN_WORDS:
        if word in text_lower:
            return False
    return True

# ===============================================
# Email yuborish (fon rejimida)
# ===============================================
def _send_reset_code_email_sync(to_email, code):
    try:
        from_email = os.getenv("RESEND_FROM_EMAIL", "onboarding@resend.dev")
        resend.Emails.send({
            "from": from_email,
            "to": to_email,
            "subject": "Platforma - PIN kodni tiklash",
            "html": f"<p>Kodingiz: <b>{code}</b></p>"
        })
    except Exception as e:
        logging.error(f"Email xato: {e}")

def send_reset_code_email(to_email, code):
    bg_executor.submit(_send_reset_code_email_sync, to_email, code)
    return True

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "super_secret_key_for_development")

# ==========================================
# 🔐 KRIPTOGRAFIYA MENEJERI (Sayt uchun)
# ==========================================
class CryptoManager:
    def __init__(self, secret_token):
        if not secret_token:
            secret_token = "default_fallback_token_for_crypto"
        hasher = hashlib.sha256(secret_token.encode())
        self.fernet = Fernet(base64.urlsafe_b64encode(hasher.digest()))

    def sign_transaction(self, enc_priv, message_bytes):
        try:
            priv_bytes = self.fernet.decrypt(enc_priv.encode('utf-8'))
            private_key = ed25519.Ed25519PrivateKey.from_private_bytes(priv_bytes)
            signature = private_key.sign(message_bytes)
            return signature.hex()
        except Exception as e:
            logging.error(f"Imzolashda xatolik: {e}")
            return None

crypto_mgr = CryptoManager(BOT_TOKEN_MAIN)

LANGUAGES = {
    "uz": {
        "title": "Testchi | Tizimga kirish",
        "loading": "Yuklanmoqda...",
        "security_check": "Xavfsizlik tekshiruvi...",
        "error_webapp": "❌ Xatolik: Telegram WebApp ishlamadi!",
        "error_desc": "Iltimos, oddiy brauzerdan emas, botga kirib <b>/start</b> bosing va maxsus tugma orqali kiring.",
        "access_denied": "Kirish rad etildi 🛑",
        "no_permission": "Ruxsat yo'q.",
        "server_error": "Server bilan aloqada xato! ❌",
        "check_internet": "Internetni tekshiring yoki sahifani yangilang.",
        "something_wrong": "Qandaydir xatolik yuz berdi ❌",
        "choose_lang": "Tilni tanlang",
        "premium_only": "❌ Oboi va maxsus sozlamalar faqat Premium obunachilar uchun!",
        "bg_updated": "✅ Fon muvaffaqiyatli yangilandi!",
        "token_updated": "✅ Tokeningiz muvaffaqiyatli yangilandi!",
        "only_bot": "Siz faqat Telegram botdagi Web App tugmasi orqali kira olasiz!",
        "premium_status": "💎 PREMIUM",
        "free_status": "Oddiy foydalanuvchi",
        "no_photo": "Rasm yuklanmadi",
        "no_photo_selected": "Rasm tanlanmadi",
        "receipt_sent": "✅ To'lov cheki yuborildi. Admin tasdiqlashini kuting.",
        "premium_cancelled": "⚠️ Admin tomonidan Premium maqomingiz bekor qilindi.",
        "premium_granted": "🎉 Admin sizga Premium maqomini taqdim etdi!",
        "banned_msg": "🛑 DIQQAT: Siz qoidalarni buzganingiz uchun tizimdan bloklandingiz (BAN)!",
        "req_already_checked": "So'rov allaqachon ko'rib chiqilgan.",
        "payment_approved": "🎉 Tabriklaymiz! To'lovingiz tasdiqlandi. Sizga {months} oylik PREMIUM maqomi berildi!",
        "payment_approved_short": "Tasdiqlandi va {months} oylik premium berildi.",
        "payment_rejected": "❌ To'lovingiz tasdiqlanmadi. Xato bo'lsa adminga murojaat qiling.",
        "payment_rejected_short": "So'rov rad etildi.",
        "bad_words": "❌ DIQQAT: Matningizda tizim qoidalariga zid so'zlar aniqlandi.",
        "test_fields_req": "❌ Test nomi, matni va chat tanlanishi kerak.",
        "invalid_chat": "❌ Yaroqsiz chat tanlandi.",
        "invalid_deadline": "❌ Deadline noto'g'ri formatda.",
        "test_created": "✅ Test muvaffaqiyatli yaratildi.",
        "internal_error": "❌ Ichki xatolik yuz berdi",
        "limit_updated": "✅ Urinishlar limiti o'zgartirildi!",
        "test_not_found": "Test topilmadi yoki o'chirilgan.",
        "already_public": "Bu public nom allaqachon band!",
        "test_edited": "✅ Test muvaffaqiyatli tahrirlandi.",
        "wrong_admin_pass": "❌ Xato: Boshqaruv paroli noto'g'ri kiritildi.",
        "test_deleted_admin": "✅ Test admin tomonidan o'chirildi.",
        "test_deleted": "✅ Test muvaffaqiyatli o'chirildi.",
        "btn_sent": "✅ Test tugmasi tanlangan chatga muvaffaqiyatli yuborildi!",
        "bot_cant_write": "❌ Xatolik: Bot guruhga yoza olmadi. Bot guruhda admin ekanligiga ishonch hosil qiling.",
        "bot_not_connected": "❌ Bot ulanmagan yoki chat tanlanmadi.",
        "results_sent": "msg=✅ Natijalar chatga muvaffaqiyatli yuborildi!",
        "results_fail": "msg=❌ Natijalarni chatga yuborishda xatolik yuz berdi.",
        "fill_fields": "Asosiy maydonlarni to'ldiring!",
        "q_empty": "{idx}-savol matni bo'sh!",
        "opts_empty": "{idx}-savolda kamida 2 ta variant bo'lishi shart!",
        "correct_empty": "{idx}-savolda to'g'ri javob belgilanmagan!",
        "test_closed": "⛔ Bu test yakunланган va yopilgan.",
        "limit_reached": "Siz bu testni ishlash limitingizni ({limit} marta) tugatgansiz!",
        "login_first": "Avval tizimga kiring!",
        "ai_limit_reached": "Bugungi AI limitingiz (10 ta) tugadi. Premium sotib oling!",
        "correct_ans": "ta to'g'ri",
        "time_min": "daqiqa",
        "time_sec": "soniya"
    }
}

def get_client_info():
    ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    if ip:
        ip = ip.split(',')[0].strip()
    user_agent = request.headers.get('User-Agent', '')
    return ip, user_agent

def get_text(key, lang="uz", **kwargs):
    text = LANGUAGES.get(lang, LANGUAGES["uz"]).get(key, LANGUAGES["uz"].get(key, key))
    if kwargs:
        return text.format(**kwargs)
    return text

def format_display_score(score_val, scoring_type, lang="uz"):
    score_val = float(score_val or 0)
    if scoring_type == "percentage":
        return f"{score_val:g} %"
    elif scoring_type in ["minus", "custom"]:
        return f"{score_val:g} ball"
    else:
        t = get_text('correct_ans', lang)
        if t == 'correct_ans': t = "ta to'g'ri"
        return f"{int(score_val)} {t}"

@app.context_processor
def inject_globals():
    return {
        'get_text': get_text,
        'lang': session.get('lang', 'uz')
    }

# ==========================================
# 2. BAZAGA ULANISH VA YORDAMCHILAR
# ==========================================
db = DB()

# Database yaratilgandan keyin kanallarni yuklash
REQUIRED_CHANNELS = load_required_channels_from_db()

def to_dict(row):
    return dict(row) if row else None

# Rasm Saqlash Funksiyasi
def save_base64_image(b64_data):
    try:
        if not b64_data or not b64_data.startswith('data:image'):
            return None
        header, encoded = b64_data.split(",", 1)
        ext = header.split(";")[0].split("/")[1].lower()
        if ext == "jpeg": ext = "jpg"

        ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'webp', 'gif'}
        if ext not in ALLOWED_EXTENSIONS:
            logging.warning(f"XAVFLI FAYL YUKLASH URINISHI RAD ETILDI. Kengaytma: .{ext}")
            return None

        filename = f"q_{uuid.uuid4().hex[:8]}.{ext}"
        upload_dir = os.path.join(app.root_path, "static", "uploads")
        os.makedirs(upload_dir, exist_ok=True)
        file_path = os.path.join(upload_dir, filename)

        with open(file_path, "wb") as f:
            f.write(base64.b64decode(encoded))

        return f"WEB_{filename}"
    except Exception as e:
        logging.error(f"Image save error: {e}")
        return None

def validate_token(token: str):
    if not token:
        return None
    current_ip, current_ua = get_client_info()
    with db._conn() as c:
        c.execute("SELECT * FROM users WHERE api_key=%s", (token,))
        row = to_dict(c.fetchone())
        if row:
            if row.get("status") == "banned":
                return None
            bound_ip = row.get("bound_ip")
            bound_ua = row.get("bound_ua")
            if bound_ua and current_ua != bound_ua:
                return None
            if bound_ip and current_ip != bound_ip:
                return None
            return row
        return None

# Orqa fonda 10 ta parallel "ishchi" ishlaydi
bg_executor = ThreadPoolExecutor(max_workers=10)

def _send_tg_msg_sync(chat_id, text, reply_markup=None):
    if not BOT_TOKENS:
        return False
    data = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup)

    for token in BOT_TOKENS:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        try:
            res = requests.post(url, json=data, timeout=5)
            if res.status_code == 200:
                return True
        except Exception as e:
            logging.error(f"TG MSG xato: {e}")
    return False

def send_tg_msg(chat_id, text, reply_markup=None):
    bg_executor.submit(_send_tg_msg_sync, chat_id, text, reply_markup)

def edit_tg_msg_caption(chat_id, message_id, new_caption):
    if not BOT_TOKENS:
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKENS[0]}/editMessageCaption"
    data = {
        "chat_id": chat_id,
        "message_id": message_id,
        "caption": new_caption,
        "parse_mode": "HTML",
        "reply_markup": json.dumps({"inline_keyboard": []})
    }
    try:
        requests.post(url, json=data, timeout=5)
    except Exception as e:
        logging.error(f"Xabar izohini o'zgartirishda xato: {e}")

def get_bot_username():
    if not BOT_TOKENS:
        return "bot_username"
    url = f"https://api.telegram.org/bot{BOT_TOKENS[0]}/getMe"
    try:
        res = requests.get(url, timeout=5).json()
        if res.get("ok"):
            return res["result"]["username"]
    except Exception:
        pass
    return "bot_username"

def check_user_subscription(user_id):
    """Barcha majburiy kanallarga obuna tekshiruvi"""
    global REQUIRED_CHANNELS
    REQUIRED_CHANNELS = load_required_channels_from_db()
    
    if not BOT_TOKENS or not REQUIRED_CHANNELS:
        return True
    
    bot_token = BOT_TOKENS[0]
    
    for channel in REQUIRED_CHANNELS:
        url = f"https://api.telegram.org/bot{bot_token}/getChatMember?chat_id={channel}&user_id={user_id}"
        try:
            res = requests.get(url, timeout=5).json()
            if res.get("ok"):
                status = res["result"]["status"]
                if status in ["left", "kicked", "banned"]:
                    return False  # Bitta kanalga ham obuna bo'lmasa False
            else:
                # Agar kanal topilmasa yoki bot admin bo'lmasa, bu kanalni o'tkazib yuboramiz
                logging.warning(f"Kanal {channel} tekshirishda muammo: {res.get('description')}")
                continue
        except Exception as e:
            logging.error(f"Kanal {channel} tekshirishda xato: {e}")
            continue
    
    return True  # Barcha kanallar tekshirildi va muammo yo'q

def parse_word_to_test(text):
    questions = []
    current_q = None
    theme = "Word Test"
    q_re = re.compile(r"^\s*(\d+)[\.\)\-]")
    opt_re = re.compile(r"^\s*([A-Ea-e])[\.\)\-]")
    true_re = re.compile(r"^\s*true\s*[:\-]\s*([a-eA-E])\s*$", re.IGNORECASE)
    theme_re = re.compile(r"^\s*theme\s*[:\-]\s*(.+)$", re.IGNORECASE)

    for line in text.split("\n"):
        line = line.strip()
        if not line: continue

        m_theme = theme_re.match(line)
        if m_theme and theme == "Word Test":
            theme = m_theme.group(1).strip()
            continue

        m_q = q_re.match(line)
        if m_q:
            if current_q: questions.append(current_q)
            clean_q = q_re.sub("", line).strip()
            current_q = {"question": clean_q, "options": [], "correct_index": -1}
            continue

        m_opt = opt_re.match(line)
        if m_opt and current_q:
            clean_opt = opt_re.sub("", line).strip()
            current_q["options"].append(clean_opt)
            continue

        m_true = true_re.match(line)
        if m_true and current_q:
            correct_letter = m_true.group(1).strip().lower()
            current_q["correct_index"] = ord(correct_letter) - ord('a')
            continue

        if current_q and not current_q["options"]:
            current_q["question"] += "\n" + line

    if current_q: questions.append(current_q)
    if not questions:
        return theme, [], "Matndan savollar topilmadi. Format to'g'riligini tekshiring."
    return theme, questions, None

# ==========================================
# 🛡️ MIDDLEWARES (DDOS, CAPTCHA VA OBUNA)
# ==========================================
@app.before_request
def global_protection():
    allowed_endpoints = [
        'static', 'captcha_page', 'api_verify_captcha', 'telegram_login_page',
        'auth_webapp', 'tg_image', 'q_image', 'force_sub_page', 'api_verify_sub',
        'pin_lock_page', 'request_pin_reset', 'verify_pin_reset',
        'pin_manager', 'set_lang'
    ]

    if request.endpoint in allowed_endpoints or not request.endpoint:
        return

    ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    if ip:
        ip = ip.split(',')[0].strip()

    now = int(time.time())
    minute_ago = now - 60

    try:
        with db._conn() as c:
            c.execute("SELECT ip FROM blacklisted_ips WHERE ip=%s", (ip,))
            if c.fetchone():
                if request.path.startswith('/api/'):
                    return jsonify({"error": "Spam aniqlandi! Captchadan o'ting.", "needs_captcha": True}), 403
                return redirect("/captcha")

            c.execute("DELETE FROM ip_tracking WHERE ip=%s AND request_time < %s", (ip, minute_ago))
            c.execute("INSERT INTO ip_tracking (ip, request_time) VALUES (%s, %s)", (ip, now))

            c.execute("SELECT COUNT(*) as cnt FROM ip_tracking WHERE ip=%s", (ip,))
            count_row = to_dict(c.fetchone())
            req_count = count_row.get("cnt", 0) if count_row else 0

            if req_count > 100:
                c.execute("INSERT IGNORE INTO blacklisted_ips (ip, banned_at) VALUES (%s, %s)", (ip, now))
                c.execute("DELETE FROM ip_tracking WHERE ip=%s", (ip,))

                token = request.args.get("token") or (request.json.get("token") if request.is_json else None)
                if token:
                    c.execute("UPDATE users SET is_verified=0 WHERE api_key=%s", (token,))
                c.execute("COMMIT")

                if request.path.startswith('/api/'):
                    return jsonify({"error": "Spam aniqlandi! Captchadan o'ting.", "needs_captcha": True}), 403
                return redirect("/captcha")
    except Exception as e:
        logging.error(f"DDoS Protection Error: {e}")

    token = request.args.get("token")
    if not token and request.is_json:
        token = request.json.get("token")
    elif not token and request.form:
        token = request.form.get("token")

    if not token: return

    user = validate_token(token)
    if not user: return

    # PIN KOD QULF EKRANINI MAJBURIY QILISH
    if not session.get(f"pin_unlocked_{token}"):
        if request.path.startswith('/solve/'):
            session[f"next_url_{token}"] = request.path
        if request.path.startswith('/api/'):
            return jsonify({"error": "PIN Lock", "redirect": f"/pin-lock?token={token}"}), 403
        return redirect(f"/pin-lock?token={token}")

    user_id = user["user_id"]
    if int(user_id) in SUPERADMINS: return

    sub_cache_key = f"sub_{user_id}"
    last_check = session.get(sub_cache_key, 0)

    if now - last_check > 300:
        if not check_user_subscription(user_id):
            session[sub_cache_key] = 0
            if request.path.startswith('/solve/'):
                session[f"next_url_{token}"] = request.path

            if request.path.startswith('/api/') or request.method == "POST":
                return jsonify({"error": "Majburiy obunadan o'ting", "needs_sub": True, "redirect": f"/force-sub?token={token}"}), 403
            return redirect(f"/force-sub?token={token}")
        else:
            session[sub_cache_key] = now

# ==========================================
# 4. TELEGRAM WEB APP XAVFSIZLIGI (LOGIN)
# ==========================================
def verify_telegram_webapp_data(init_data: str):
    try:
        parsed_data = dict(urllib.parse.parse_qsl(init_data))
        if 'hash' not in parsed_data:
            return False, None
        received_hash = parsed_data.pop('hash')
        auth_date = int(parsed_data.get('auth_date', 0))
        if int(time.time()) - auth_date > 10800:
            return False, None
        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed_data.items()))
        for token in BOT_TOKENS:
            secret_key = hmac.new(b"WebAppData", token.encode(), hashlib.sha256).digest()
            calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
            if calculated_hash == received_hash:
                return True, json.loads(parsed_data.get('user', '{}'))
        return False, None
    except Exception:
        return False, None

@app.route("/telegram-login")
def telegram_login_page():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Testchi</title>
        <script src="https://telegram.org/js/telegram-web-app.js?v=8"></script>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body { background-color: #0f172a; color: white; display: flex; justify-content: center; align-items: center; height: 100vh; font-family: sans-serif; text-align: center; margin: 0;}
            .loader { border: 4px solid rgba(255,255,255,0.1); border-top: 4px solid #38d39f; border-radius: 50%; width: 40px; height: 40px; animation: spin 1s linear infinite; margin: 0 auto 15px auto; }
            @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
        </style>
    </head>
    <body>
        <div>
            <div class="loader" id="spinner"></div>
            <h2 id="msg" style="font-size: 18px; font-weight: 500;">Loading...</h2>
            <p id="debug" style="color: gray; font-size: 13px; margin-top: 10px;"></p>
        </div>
        <script>
            function initWebApp() {
                try {
                    const tg = window.Telegram.WebApp;
                    tg.ready();
                    tg.expand();
                    let initData = tg.initData;
                    if (!initData && window.location.hash) {
                        initData = new URLSearchParams(window.location.hash.slice(1)).get('tgWebAppData');
                    }
                    if (!initData) {
                        document.getElementById('spinner').style.display = 'none';
                        document.getElementById('msg').innerHTML = "❌ WebApp Error!";
                        document.getElementById('debug').innerHTML = "Bot orqali kiring.";
                        return;
                    }
                    fetch('/api/auth/webapp', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({ initData: initData })
                    })
                    .then(res => res.json())
                    .then(data => {
                        if (data.needs_captcha) {
                            window.location.replace('/captcha');
                        } else if (data.needs_sub) {
                            window.location.replace('/force-sub?token=' + data.token);
                        } else if (data.token) {
                            window.location.replace('/pin-lock?token=' + data.token);
                        } else {
                            document.getElementById('spinner').style.display = 'none';
                            document.getElementById('msg').innerHTML = "Access Denied 🛑";
                            document.getElementById('debug').innerHTML = data.error || "No permission.";
                        }
                    })
                    .catch(err => {
                        document.getElementById('spinner').style.display = 'none';
                        document.getElementById('msg').innerHTML = "Server Error! ❌";
                        document.getElementById('debug').innerHTML = "Check connection.";
                    });
                } catch (e) {
                    document.getElementById('spinner').style.display = 'none';
                    document.getElementById('msg').innerHTML = "Error ❌";
                    document.getElementById('debug').innerHTML = e.message;
                }
            }
            setTimeout(initWebApp, 300);
        </script>
    </body>
    </html>
    """

@app.route("/api/auth/webapp", methods=["POST"])
def auth_webapp():
    data = request.json
    init_data = data.get("initData", "")
    ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    if ip:
        ip = ip.split(',')[0].strip()
        with db._conn() as c:
            c.execute("SELECT ip FROM blacklisted_ips WHERE ip=%s", (ip,))
            row = c.fetchone()
            if row:
                return jsonify({"error": "IP is blacklisted!"}), 403

    is_valid, tg_user = verify_telegram_webapp_data(init_data)
    if not is_valid or not tg_user:
        return jsonify({"error": "Security check failed. Try again."}), 403

    user_id = tg_user.get("id")
    with db._conn() as c:
        c.execute("SELECT api_key, status, is_verified FROM users WHERE user_id=%s", (user_id,))
        row = to_dict(c.fetchone())
        if not row:
            return jsonify({"error": "User not found. Send /start in bot."}), 403
        if row.get("status") == "banned":
            return jsonify({"error": "You are BANNED!"}), 403
        if not row.get("is_verified"):
            return jsonify({"needs_captcha": True})
        if row.get("api_key"):
            current_ip, current_ua = get_client_info()
            c.execute("UPDATE users SET bound_ip=%s, bound_ua=%s WHERE user_id=%s", (current_ip, current_ua, user_id))
            c.execute("COMMIT")
            if int(user_id) not in SUPERADMINS and not check_user_subscription(user_id):
                return jsonify({"token": row["api_key"], "needs_sub": True})
            session[f"sub_{user_id}"] = int(time.time())
            return jsonify({"token": row["api_key"]})
        return jsonify({"error": "Security error."}), 403

# ==========================================
# 🛑 MAJBURIY OBUNA (FORCE SUB) SAHIFASI
# ==========================================
@app.route("/force-sub")
def force_sub_page():
    global REQUIRED_CHANNELS
    REQUIRED_CHANNELS = load_required_channels_from_db()
    
    token = request.args.get("token")
    
    # Kanallar ro'yxatini HTML uchun tayyorlash
    channel_buttons_html = ""
    for channel in REQUIRED_CHANNELS:
        channel_link = f"https://t.me/{channel.replace('@', '')}"
        channel_buttons_html += f'<a href="{channel_link}" target="_blank" class="btn">📢 {channel}</a>\n'
    
    if not channel_buttons_html:
        channel_buttons_html = '<p style="color: yellow;">⚠️ Hozirda majburiy kanallar yo\'q.</p>'
    
    return render_template_string("""
    <!DOCTYPE html>
    <html lang="uz">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Majburiy Obuna</title>
        <style>
            body { background: #0f172a; color: white; font-family: sans-serif; text-align: center; padding: 50px 20px; }
            .box { background: #1e293b; padding: 30px 20px; border-radius: 16px; max-width: 400px; margin: 0 auto; box-shadow: 0 4px 15px rgba(0,0,0,0.5); }
            h2 { margin-top: 0; color: #f87171; }
            p { color: #cbd5e1; line-height: 1.5; margin-bottom: 20px; }
            a.btn { display: block; background: #3b82f6; color: white; padding: 14px; text-decoration: none; border-radius: 8px; margin-bottom: 15px; font-weight: bold; transition: 0.2s; }
            a.btn:hover { background: #2563eb; }
            button.btn-check { display: block; width: 100%; background: #10b981; color: white; padding: 14px; border: none; border-radius: 8px; font-weight: bold; cursor: pointer; font-size: 16px; transition: 0.2s; }
            button.btn-check:hover { background: #059669; }
            .error { color: #fca5a5; margin-top: 15px; display: none; background: rgba(239, 68, 68, 0.2); padding: 10px; border-radius: 8px;}
        </style>
    </head>
    <body>
        <div class="box">
            <h2>🚫 Majburiy Obuna</h2>
            <p>Tizimdan foydalanish uchun quyidagi kanallarga a'zo bo'lishingiz shart. Barcha kanallarga qo'shilgach, pastdagi tugmani bosing:</p>
            {{channel_buttons|safe}}
            <button class="btn-check" onclick="checkSub()">✅ Tasdiqlash</button>
            <p class="error" id="err-msg">❌ Hali barcha kanallarga qo'shilmadingiz!</p>
        </div>
        <script>
            function checkSub() {
                const btn = document.querySelector('.btn-check');
                btn.innerText = '⏳ Tekshirilmoqda...';
                btn.disabled = true;
                fetch('/api/verify-sub', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({token: '{{token}}'})
                }).then(r=>r.json()).then(d=>{
                    if(d.success) {
                        btn.innerText = '✅ Tasdiqlandi!';
                        btn.style.background = '#059669';
                        setTimeout(() => window.location.href = '/pin-lock?token={{token}}', 500);
                    }
                    else {
                        let errMsg = '❌ Hali barcha kanallarga qo\'shilmadingiz!';
                        if(d.error === 'not_admin') errMsg = '⚠️ XATOLIK: Bot kanalda Admin emas yoki kanal noto\'g\'ri! Sabab: ' + (d.tg_error || '');
                        if(d.error === 'api_error') errMsg = '❌ Server xatosi: ' + (d.tg_error || '');
                        document.getElementById('err-msg').innerText = errMsg;
                        document.getElementById('err-msg').style.display = 'block';
                        btn.innerText = '✅ Tasdiqlash';
                        btn.disabled = false;
                    }
                }).catch(e => {
                    document.getElementById('err-msg').innerText = 'Tarmoq xatosi!';
                    document.getElementById('err-msg').style.display = 'block';
                    btn.innerText = '✅ Tasdiqlash';
                    btn.disabled = false;
                });
            }
        </script>
    </body>
    </html>
    """, channel_buttons=channel_buttons_html, token=token)

@app.route("/api/verify-sub", methods=["POST"])
def api_verify_sub():
    global REQUIRED_CHANNELS
    REQUIRED_CHANNELS = load_required_channels_from_db()
    
    data = request.json or {}
    token = data.get("token")
    user = validate_token(token)
    if not user: return jsonify({"success": False, "error": "Not logged in"})

    if not BOT_TOKENS or not REQUIRED_CHANNELS: 
        session[f"sub_{user['user_id']}"] = int(time.time())
        return jsonify({"success": True})
    
    bot_token = BOT_TOKENS[0]
    
    # Barcha kanallarni tekshirish
    for channel in REQUIRED_CHANNELS:
        url = f"https://api.telegram.org/bot{bot_token}/getChatMember?chat_id={channel}&user_id={user['user_id']}"
        try:
            res = requests.get(url, timeout=7).json()
            if res.get("ok"):
                status = res["result"]["status"]
                if status in ["left", "kicked", "banned"]:
                    return jsonify({"success": False, "error": "not_joined", "channel": channel})
            else:
                return jsonify({"success": False, "error": "not_admin", "tg_error": res.get("description"), "channel": channel})
        except Exception as e:
            return jsonify({"success": False, "error": "api_error", "tg_error": str(e), "channel": channel})
    
    # Barcha kanallar tekshirildi va hammasi OK
    session[f"sub_{user['user_id']}"] = int(time.time())
    return jsonify({"success": True})

@app.route("/set-lang", methods=["GET", "POST"])
def set_lang():
    lang = request.form.get("lang") or request.args.get("lang", "uz")
    token = request.form.get("token") or request.args.get("token", "")
    if lang in ["uz", "ru", "uz_cyrl"]:
        session["lang"] = lang
        user = validate_token(token)
        if user:
            with db._conn() as c:
                c.execute("UPDATE users SET lang=%s WHERE user_id=%s", (lang, user["user_id"]))
                c.execute("COMMIT")
    referer = request.headers.get("Referer")
    if referer:
        return redirect(referer)
    return redirect(f"/?token={token}")

@app.route("/api/pin-manager", methods=["POST"])
def pin_manager():
    data = request.json or {}
    token = data.get("token")
    action = data.get("action")
    pin = data.get("pin", "")

    user = validate_token(token)
    if not user: return jsonify({"success": False, "error": "Unauthorized"}), 401

    with db._conn() as c:
        c.execute("SELECT pin_code, email, secret_word FROM users WHERE user_id=%s", (user["user_id"],))
        row = to_dict(c.fetchone())
        current_pin = row.get("pin_code") if row else None

        empty_hash = hashlib.sha256(b"").hexdigest()
        if current_pin == empty_hash or current_pin == "":
            current_pin = None

        if action == "check":
            return jsonify({"success": True, "has_pin": bool(current_pin)})
        elif action == "set":
            email = data.get("email", "").strip()
            secret_word = data.get("secret_word", "").strip()
            if not pin:
                c.execute("UPDATE users SET pin_code=NULL WHERE user_id=%s", (user["user_id"],))
            else:
                if not email or not secret_word:
                    return jsonify({"success": False, "error": "Email va maxfiy so'zni kiritish majburiy!"})
                hashed_pin = hashlib.sha256(pin.encode()).hexdigest()
                try:
                    c.execute("UPDATE users SET pin_code=%s, email=%s, secret_word=%s WHERE user_id=%s", (hashed_pin, email, secret_word, user["user_id"]))
                except Exception as e:
                    return jsonify({"success": False, "error": "Bazaga saqlashda xato."})
            c.execute("COMMIT")
            session[f"pin_unlocked_{token}"] = True
            return jsonify({"success": True})

        elif action == "verify":
            if not current_pin:
                session[f"pin_unlocked_{token}"] = True
                return jsonify({"success": True})
            hashed_pin = hashlib.sha256(pin.encode()).hexdigest()
            if hashed_pin == current_pin:
                db.update_pin_attempts(user["user_id"], reset=True)
                session[f"pin_unlocked_{token}"] = True
                return jsonify({"success": True})
            else:
                db.update_pin_attempts(user["user_id"])
                return jsonify({"success": False, "error": "Wrong PIN!"})

        elif action == "biometric_unlock":
            session[f"pin_unlocked_{token}"] = True
            return jsonify({"success": True})

        elif action == "change":
            old_pin = data.get("old_pin", "")
            new_pin = data.get("new_pin", "")
            if current_pin and hashlib.sha256(old_pin.encode()).hexdigest() != current_pin:
                return jsonify({"success": False, "error": "Eski PIN noto'g'ri kiritildi!"})
            if not new_pin:
                c.execute("UPDATE users SET pin_code=NULL WHERE user_id=%s", (user["user_id"],))
            else:
                new_hashed = hashlib.sha256(new_pin.encode()).hexdigest()
                c.execute("UPDATE users SET pin_code=%s WHERE user_id=%s", (new_hashed, user["user_id"]))
            c.execute("COMMIT")
            return jsonify({"success": True, "message": "PIN muvaffaqiyatli yangilandi!"})

@app.route("/pin-lock")
def pin_lock_page():
    token = request.args.get("token")
    user = validate_token(token)
    current_user_bg = user.get("custom_bg") if user else None
    current_lock_bg = user.get("custom_lock_bg") if user else None
    return render_template("pin_lock.html", token=token, current_user_bg=current_user_bg, current_lock_bg=current_lock_bg, lang=session.get('lang', 'uz'))

@app.route("/api/request-pin-reset", methods=["POST"])
def request_pin_reset():
    data = request.json or {}
    token = data.get("token")
    user = validate_token(token)
    if not user: return jsonify({"success": False, "error": "Tizimga kirmagansiz"}), 401
    with db._conn() as c:
        c.execute("SELECT email FROM users WHERE user_id=%s", (user["user_id"],))
        row = to_dict(c.fetchone())
    email = row.get("email") if row else None
    if not email:
        return jsonify({"success": False, "error": "Sizda email o'rnatilmagan! Adminga murojaat qiling."})
    reset_code = str(random.randint(1000, 9999))
    session[f"reset_code_{user['user_id']}"] = reset_code
    if send_reset_code_email(email, reset_code):
        return jsonify({"success": True, "email": email})
    else:
        return jsonify({"success": False, "error": "Pochtaga yuborishda xatolik yuz berdi."})

@app.route("/api/verify-pin-reset", methods=["POST"])
def verify_pin_reset():
    data = request.json or {}
    token = data.get("token")
    code = data.get("code")
    secret_word = data.get("secret_word")
    user = validate_token(token)
    if not user: return jsonify({"success": False, "error": "Unauthorized"}), 401
    user_id = user["user_id"]

    if code:
        expected_code = session.get(f"reset_code_{user_id}")
        if expected_code and str(code) == expected_code:
            with db._conn() as c:
                c.execute("UPDATE users SET pin_code=NULL WHERE user_id=%s", (user_id,))
                c.execute("COMMIT")
            session.pop(f"reset_code_{user_id}", None)
            return jsonify({"success": True, "message": "PIN kod o'chirildi! Yangisini o'rnatishingiz mumkin."})
        return jsonify({"success": False, "error": "Kiritilgan kod noto'g'ri!"})

    if secret_word:
        with db._conn() as c:
            c.execute("SELECT secret_word FROM users WHERE user_id=%s", (user_id,))
            row = to_dict(c.fetchone())
        if row and row.get("secret_word") and row["secret_word"].lower() == secret_word.strip().lower():
            with db._conn() as c:
                c.execute("UPDATE users SET pin_code=NULL WHERE user_id=%s", (user_id,))
                c.execute("COMMIT")
            return jsonify({"success": True, "message": "PIN kod o'chirildi! Yangisini o'rnatishingiz mumkin."})
        return jsonify({"success": False, "error": "Maxfiy so'z noto'g'ri!"})

    return jsonify({"success": False, "error": "Ma'lumot to'liq emas!"})

@app.route("/update-bg", methods=["POST"])
def update_bg():
    token = request.form.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user: return abort(401)
    if user.get("status") != "premium" and int(user["user_id"]) not in SUPERADMINS:
        return redirect(f"/account?token={token}&msg={get_text('premium_only', lang)}")

    bg_type = request.form.get("bg_type")
    target_screen = request.form.get("target_screen", "main")
    custom_bg = None

    if bg_type == "system":
        custom_bg = request.form.get("system_bg")
    elif bg_type == "upload" and 'bg_file' in request.files and request.files['bg_file'].filename != '':
        file = request.files['bg_file']
        ext = file.filename.rsplit('.', 1)[-1].lower() if '.' in file.filename else ''
        ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'webp'}
        if ext not in ALLOWED_EXTENSIONS:
            return redirect(f"/account?token={token}&msg=❌ Xato: Faqat rasm (PNG, JPG, WEBP) yuklash mumkin!")
        filename = f"bg_{user['user_id']}_{uuid.uuid4().hex[:6]}.{ext}"
        try:
            os.makedirs(os.path.join(app.root_path, "static", "uploads"), exist_ok=True)
            file.save(os.path.join(app.root_path, "static", "uploads", filename))
            custom_bg = f"/static/uploads/{filename}"
        except Exception as e:
            return redirect(f"/account?token={token}&msg=❌ Rasm yuklashda server xatosi.")

    if custom_bg is not None:
        with db._conn() as c:
            if target_screen == "lock":
                c.execute("UPDATE users SET custom_lock_bg=%s WHERE user_id=%s", (custom_bg, user["user_id"]))
            else:
                c.execute("UPDATE users SET custom_bg=%s WHERE user_id=%s", (custom_bg, user["user_id"]))
            c.execute("COMMIT")

    return redirect(f"/account?token={token}&msg={get_text('bg_updated', lang)}")

@app.route("/")
def index():
    token = request.args.get("token")
    user = validate_token(token)
    if not user:
        return abort(401, "Siz faqat Telegram botdagi Web App tugmasi orqali kira olasiz!")

    pending_next = session.pop(f"next_url_{token}", None)
    if pending_next:
        return redirect(f"{pending_next}?token={token}")

    if not user.get("is_verified"):
        return redirect("/captcha")

    db_lang = user.get("lang")
    if db_lang and db_lang in ["uz", "ru", "uz_cyrl"]:
        session["lang"] = db_lang

    lang = session.get("lang", "uz")
    user_id = int(user["user_id"])
    username = user.get("username", "")
    full_name = "".join([p for p in [user.get("first_name"), user.get("last_name")] if p]).strip()
    is_admin = user_id in SUPERADMINS
    is_lower_admin = user_id in LOWER_ADMINS

    try: balance = db.get_token_balance(user_id)
    except: balance = 0.0

    raw_tests = db.tests_for_owner(user_id)
    tests = []
    for t in raw_tests:
        t_dict = to_dict(t)
        t_dict['price_gwt'] = float(t_dict.get('price_gwt') or 0.0)
        t_dict['price_stars'] = int(t_dict.get('price_stars') or 0)
        q_count, finished_count = db.stats(t_dict["test_id"])
        tests.append({"test": t_dict, "q_count": q_count, "finished_count": finished_count})

    chats = db.chats_for_user(user_id)
    eligible_chats = [c for c in chats if c.get('bot_is_admin', 0) == 1]

    return render_template(
        "index.html",
        username=username,
        full_name=full_name,
        tests=tests,
        chats=eligible_chats,
        balance=balance,
        base_url=WEB_BASE_URL,
        token=token,
        is_admin=is_admin,
        is_lower_admin=is_lower_admin,
        current_user_bg=user.get("custom_bg"),
        current_lock_bg=user.get("custom_lock_bg"),
        lang=lang,
        get_text=get_text
    )

@app.route("/account")
def account():
    token = request.args.get("token")
    session_user = validate_token(token)
    lang = session.get("lang", "uz")
    if not session_user: return abort(401)
    if not session_user.get("is_verified"): return redirect("/captcha")

    user_row = db.get_user(session_user["user_id"])
    if not user_row: return abort(401)

    user = dict(user_row)
    username = user.get("username", "")
    full_name = "".join([p for p in [user.get("first_name"), user.get("last_name")] if p]).strip()

    raw_status = user.get("status", "free").lower()
    if raw_status == "premium":
        user_status_display = get_text('premium_status', lang)
    else:
        user_status_display = get_text('free_status', lang)

    reg_ts = user.get("registered_at") or int(time.time())
    registered_at = datetime.fromtimestamp(int(reg_ts), tz=TZ).strftime("%d.%m.%Y %H:%M")

    premium_until_str = "—"
    if raw_status == "premium" and user.get("premium_expire_at"):
        premium_until_str = datetime.fromtimestamp(int(user["premium_expire_at"]), tz=TZ).strftime("%d.%m.%Y %H:%M")

    try: balance = db.get_token_balance(session_user["user_id"])
    except: balance = 0.0

    return render_template(
        "account.html",
        username=username,
        full_name=full_name,
        token=token,
        balance=balance,
        base_url=WEB_BASE_URL,
        user_status=user_status_display,
        registered_at=registered_at,
        premium_until=premium_until_str,
        is_admin=(int(user["user_id"]) in SUPERADMINS),
        current_user_bg=user.get("custom_bg"),
        current_lock_bg=user.get("custom_lock_bg"),
        lang=lang,
        get_text=get_text,
        session_user_id=user["user_id"]
    )

@app.route("/create-test", methods=["POST"])
def create_word_test():
    token = request.form.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user: return abort(401)
    if not user.get("is_verified"): return redirect("/captcha")

    user_id = int(user["user_id"])
    title = request.form.get("title", "").strip()
    body = request.form.get("body", "").strip()
    chat_id_raw = request.form.get("chat_id", "")

    if not title or not body or not chat_id_raw:
        return redirect(f"/?token={token}&msg={get_text('fill_fields', lang)}")

    try: chat_id = int(chat_id_raw)
    except: return redirect(f"/?token={token}&msg={get_text('invalid_chat', lang)}")

    theme, questions, error_msg = parse_word_to_test(body)
    if error_msg or not questions:
        return redirect(f"/?token={token}&msg={error_msg or 'Savollar topilmadi'}")

    questions_text = " ".join([q.get("question", "") + " " + " ".join(q.get("options", [])) for q in questions])
    is_safe, reason = check_content_with_ai(title, questions_text)

    if not is_safe:
        return redirect(f"/?token={token}&msg=❌ Taqiqlandi: {reason}")

    test_id = uuid.uuid4().hex[:10]
    try:
        current_time = int(time.time())
        db.create_test(
            test_id=test_id,
            owner_user_id=user_id,
            chat_id=chat_id,
            title=title,
            per_question_sec=60,
            created_at=current_time
        )
        for i, q in enumerate(questions):
            db.add_question(test_id, i, q["question"], q["options"], q["correct_index"])

        if BOT_TOKENS:
            bot_username = get_bot_username()
            deep_link = f"https://t.me/{bot_username}?start=test_{test_id}"
            safe_title = html.escape(title)
            text = f"🧩 <b>{safe_title}</b>\n\n🖊 Savollar: {len(questions)} ta\n\nBoshlash uchun pastdagi tugmani bosing:"
            kb = {"inline_keyboard": [[{"text": "🔒 Private testni boshlash", "url": deep_link}]]}
            send_tg_msg(chat_id, text, reply_markup=kb)

        return redirect(f"/?token={token}&msg={get_text('test_created', lang)}")
    except Exception as e:
        return redirect(f"/?token={token}&msg={get_text('internal_error', lang)}")

@app.route("/market")
def marketplace():
    token = request.args.get("token")
    user = validate_token(token)
    if not user: return abort(401)

    with db._conn() as c:
        c.execute("""
            SELECT t.*, u.first_name, u.last_name
            FROM tests t
            LEFT JOIN users u ON t.owner_user_id = u.user_id
            WHERE (t.price_gwt > 0 OR t.price_stars > 0) AND t.status='open'
            ORDER BY t.created_at DESC
        """)
        tests_raw = c.fetchall()
        c.execute("SELECT test_id FROM purchased_tests WHERE user_id=%s", (user["user_id"],))
        purchased = {r['test_id'] for r in c.fetchall()}

    market_tests = []
    for t in tests_raw:
        td = to_dict(t)
        td['is_purchased'] = td['test_id'] in purchased
        td['is_owner'] = int(td['owner_user_id']) == int(user['user_id'])
        td['price_gwt'] = float(td.get('price_gwt') or 0.0)
        td['price_stars'] = int(td.get('price_stars') or 0)
        market_tests.append(td)

    html_content = """
    <!DOCTYPE html>
    <html lang="uz">
    <head><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Testlar Bozori</title>
    <style>
        body { background: #0f172a; color: white; font-family: sans-serif; padding: 20px; }
        .card { background: #1e293b; padding: 20px; border-radius: 12px; margin-bottom: 15px; border: 1px solid rgba(255,255,255,0.1); }
        .btn { display: inline-block; background: #38d39f; color: #000; padding: 10px 15px; border-radius: 8px; text-decoration: none; font-weight: bold; margin-top: 10px; cursor: pointer; border: none;}
        .btn-stars { background: #fbbf24; }
    </style></head>
    <body>
        <h2>🛒 Testlar Bozori</h2>
        <a href="/?token={{ token }}" style="color:#38d39f; text-decoration:none;">⬅️ Orqaga</a>
        <br><br>
        {% if request.args.get('msg') %}
            <div style="padding:10px; background:rgba(255,255,255,0.1); margin-bottom:15px; color:#38d39f;">{{ request.args.get('msg') }}</div>
        {% endif %}
        {% for t in tests %}
        <div class="card">
            <h3 style="margin-top:0;">🧩 {{ t.title }}</h3>
            <p style="color:gray; font-size:13px;">Muallif: {{ t.first_name }} {{ t.last_name }}</p>
            {% if t.is_owner or t.is_purchased %}
                <p style="color:#38d39f;">✅ Sizda bu test bor.</p>
                <a href="/solve/{{ t.test_id }}?token={{ token }}" class="btn">Testni ishlash</a>
            {% else %}
                <div style="display: flex; gap: 10px;">
                    {% if t.price_gwt > 0 %}
                    <form action="/buy-test/gwt/{{ t.test_id }}" method="POST" style="margin:0;">
                        <input type="hidden" name="token" value="{{ token }}">
                        <button class="btn" type="submit">Sotib olish: {{ t.price_gwt }} GWT</button>
                    </form>
                    {% endif %}
                    {% if t.price_stars > 0 %}
                    <form action="/buy-test/stars/{{ t.test_id }}" method="POST" style="margin:0;">
                        <input type="hidden" name="token" value="{{ token }}">
                        <button class="btn btn-stars" type="submit">Sotib olish: {{ t.price_stars }} ⭐️</button>
                    </form>
                    {% endif %}
                </div>
            {% endif %}
        </div>
        {% else %}
        <p>Hozircha bozorda pullik testlar yo'q.</p>
        {% endfor %}
    </body></html>
    """
    return render_template_string(html_content, token=token, tests=market_tests)

@app.route("/monetize-test/<test_id>", methods=["POST"])
def monetize_test(test_id):
    token = request.form.get("token")
    user = validate_token(token)
    if not user: return abort(401)
    user_id = int(user["user_id"])
    test = to_dict(db.get_test(test_id))

    if not test or (int(test["owner_user_id"]) != user_id and user_id not in SUPERADMINS):
        return abort(403)

    try: price_gwt = float(request.form.get("price_gwt", 0))
    except: price_gwt = 0.0
    try: price_stars = int(request.form.get("price_stars", 0))
    except: price_stars = 0

    with db._conn() as c:
        c.execute("UPDATE tests SET price_gwt=%s, price_stars=%s WHERE test_id=%s", (price_gwt, price_stars, test_id))
        c.execute("COMMIT")

    return redirect(f"/?token={token}&msg=✅ Test narxi bozorda yangilandi!")

@app.route("/buy-test/gwt/<test_id>", methods=["POST"])
def buy_test_gwt(test_id):
    token = request.form.get("token")
    user = validate_token(token)
    if not user: return abort(401)
    user_id = int(user["user_id"])
    test = to_dict(db.get_test(test_id))

    if not test or float(test.get("price_gwt") or 0.0) <= 0:
        return abort(404, "Bu test GWT orqali sotilmaydi.")

    price = float(test["price_gwt"])
    seller_id = int(test["owner_user_id"])

    if user_id == seller_id:
        return redirect(f"/market?token={token}&msg=O'zingizni testingiz!")

    wallet = db.get_wallet(user_id)
    if not wallet: return redirect(f"/market?token={token}&msg=❌ Sizda Hamyon yo'q. Avval Botda Hamyon yarating.")

    balance = db.get_token_balance(user_id)
    if balance < price: return redirect(f"/market?token={token}&msg=❌ Balansingizda yetarli GWT yo'q.")

    seller_wallet = db.get_wallet(seller_id)
    if not seller_wallet: return redirect(f"/market?token={token}&msg=❌ Sotuvchining hamyoni yopiq.")

    target_address = seller_wallet['public_key']
    transaction_data = f"{wallet['public_key']}->{target_address}:{price}".encode('utf-8')
    signature = crypto_mgr.sign_transaction(wallet['encrypted_private_key'], transaction_data)

    success, err_msg = db.transfer_token_by_address_or_id(sender_id=user_id, target=target_address, amount=price, signature=signature)

    if success:
        with db._conn() as c:
            c.execute("INSERT IGNORE INTO purchased_tests (user_id, test_id, price_paid, currency, purchased_at) VALUES (%s, %s, %s, 'GWT', %s)",
                      (user_id, test_id, price, int(time.time())))
            c.execute("COMMIT")
        send_tg_msg(seller_id, f"🎉 <b>Tabriklaymiz!</b> Sizning <i>'{test['title']}'</i> nomli testingiz sotib olindi!\n💰 Daromad: <b>+{price} GWT</b>")
        return redirect(f"/solve/{test_id}?token={token}&msg=✅ Muvaffaqiyatli sotib olindi!")
    else:
        return redirect(f"/market?token={token}&msg=❌ Xatolik: {err_msg}")

@app.route("/buy-premium", methods=["GET", "POST"])
def buy_premium():
    token = request.args.get("token") or request.form.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user: return abort(401)
    if not user.get("is_verified"): return redirect("/captcha")

    if request.method == "POST":
        months = int(request.form.get("months", 1))
        if 'receipt' not in request.files:
            return redirect(f"/buy-premium?token={token}&error={get_text('no_photo', lang)}")
        file = request.files['receipt']
        if file.filename == '':
            return redirect(f"/buy-premium?token={token}&error={get_text('no_photo_selected', lang)}")

        if file:
            ext = file.filename.rsplit('.', 1)[-1].lower() if '.' in file.filename else 'jpg'
            filename = f"receipt_{uuid.uuid4().hex}.{ext}"
            upload_folder = os.path.join(app.root_path, "static", "uploads")
            os.makedirs(upload_folder, exist_ok=True)
            filepath = os.path.join(upload_folder, filename)
            file.save(filepath)

            photo_id_str = f"WEB_{months}_{filename}"
            with db._conn() as c:
                c.execute("""
                    INSERT INTO premium_requests(user_id, photo_id, status, created_at)
                    VALUES(%s, %s, 'pending', %s)
                """, (user["user_id"], photo_id_str, int(time.time())))
                c.execute("COMMIT")

            admin_msg = f"🆕 <b>Saytdan yangi to'lov cheki!</b>\n\nUser ID: <code>{user['user_id']}</code>\nTa'rif: <b>{months} oylik</b>\n\nSaytning 'Kutilayotganlar' bo'limidan tekshiring."
            for adm in SUPERADMINS:
                send_tg_msg(adm, admin_msg)

            return redirect(f"/account?token={token}&msg={get_text('receipt_sent', lang)}")

    return render_template("buy_premium.html", token=token, base_url=WEB_BASE_URL, admin_card=ADMIN_CARD, current_user_bg=user.get("custom_bg"), current_lock_bg=user.get("custom_lock_bg"), lang=lang, get_text=get_text)

@app.route("/admin/users")
def admin_users():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user or int(user["user_id"]) not in SUPERADMINS: return abort(403)
    if not user.get("is_verified"): return redirect("/captcha")

    wallets = db.get_all_wallets_balances()
    wallet_dict = {int(w['user_id']): float(w['balance']) for w in wallets}

    with db._conn() as c:
        c.execute("SELECT * FROM users ORDER BY registered_at DESC")
        raw_users = c.fetchall()

    users_list = []
    for u in raw_users:
        ud = to_dict(u)
        ud['balance'] = wallet_dict.get(int(ud['user_id']), 0.0)
        users_list.append(ud)

    return render_template("admin_users.html", users=users_list, token=token, current_user_bg=user.get("custom_bg"), current_lock_bg=user.get("custom_lock_bg"), lang=lang, get_text=get_text)

@app.route("/admin/user_action", methods=["POST"])
def admin_user_action():
    token = request.form.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user or int(user["user_id"]) not in SUPERADMINS: return abort(403)

    target_id = request.form.get("target_id")
    action = request.form.get("action")
    msg_to_send = None

    with db._conn() as c:
        if action == "make_free":
            c.execute("UPDATE users SET status='free', premium_expire_at=NULL WHERE user_id=%s", (target_id,))
            msg_to_send = get_text('premium_cancelled', lang)
        elif action == "make_premium":
            add_time = 10 * 365 * 24 * 60 * 60
            expire = int(time.time()) + add_time
            c.execute("UPDATE users SET status='premium', premium_expire_at=%s WHERE user_id=%s", (expire, target_id))
            msg_to_send = get_text('premium_granted', lang)
        elif action == "ban":
            c.execute("UPDATE users SET status='banned' WHERE user_id=%s", (target_id,))
            msg_to_send = get_text('banned_msg', lang)
        c.execute("COMMIT")

    if msg_to_send:
        send_tg_msg(target_id, msg_to_send)

    return redirect(f"/admin/users?token={token}")

@app.route("/pendings")
def pendings():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user or int(user["user_id"]) not in SUPERADMINS: return abort(403)

    with db._conn() as c:
        c.execute("""
            SELECT p.*, u.username, u.first_name, u.last_name
            FROM premium_requests p
            LEFT JOIN users u ON p.user_id = u.user_id
            WHERE p.status = 'pending'
            ORDER BY p.created_at ASC
        """)
        reqs = c.fetchall()

    requests_list = []
    for r in reqs:
        rd = to_dict(r)
        rd["date_str"] = datetime.fromtimestamp(rd["created_at"], tz=TZ).strftime("%d.%m.%Y %H:%M")
        photo_id = rd.get("photo_id")
        if photo_id and photo_id.startswith("WEB_"):
            months = int(photo_id.split("_", 2)[1])
            rd["first_name"] = f"{rd.get('first_name', '')} (WEB PREM | {months} OY)"
        elif photo_id and photo_id.startswith("BOT_"):
            parts = photo_id.split("_", 2)
            months = int(parts[1]) if len(parts) > 2 else 1
            rd["first_name"] = f"{rd.get('first_name', '')} (BOT PREM | {months} OY)"
        elif photo_id and photo_id.startswith("GWT_"):
            parts = photo_id.split("_", 2)
            val = float(parts[1]) if len(parts) > 2 else 1.0
            rd["first_name"] = f"{rd.get('first_name', '')} (GWT XARID | {val} GWT)"
        else:
            rd["first_name"] = f"{rd.get('first_name', '')} (BOT)"
        requests_list.append(rd)

    return render_template("pendings.html", token=token, requests=requests_list, base_url=WEB_BASE_URL, current_user_bg=user.get("custom_bg"), current_lock_bg=user.get("custom_lock_bg"), lang=lang, get_text=get_text)

@app.route("/process-req/<int:req_id>", methods=["POST"])
def process_req(req_id):
    token = request.form.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user or int(user["user_id"]) not in SUPERADMINS: return abort(403)

    action = request.form.get("action")
    with db._conn() as c:
        c.execute("SELECT * FROM premium_requests WHERE id=%s", (req_id,))
        req_row = to_dict(c.fetchone())

    if not req_row or req_row.get("status") != "pending":
        return redirect(f"/pendings?token={token}&msg={get_text('req_already_checked', lang)}")

    target_user = int(req_row.get("user_id"))
    photo_id = req_row.get("photo_id")
    admin_msg_ids = req_row.get("admin_msg_ids")

    months = 1
    val = 0.0
    req_type = "prem"

    if photo_id:
        if photo_id.startswith("WEB_") or photo_id.startswith("BOT_"):
            parts = photo_id.split("_", 2)
            if len(parts) > 2:
                try: months = int(parts[1])
                except: months = 1
        elif photo_id.startswith("GWT_"):
            req_type = "gwt"
            parts = photo_id.split("_", 2)
            if len(parts) > 2:
                try: val = float(parts[1])
                except: val = 1.0

    if action == "approve":
        with db._conn() as c:
            c.execute("UPDATE premium_requests SET status='approved' WHERE id=%s", (req_id,))
            c.execute("COMMIT")
        if req_type == "gwt":
            db.system_sell_token(target_user, val, method="CARD")
            send_tg_msg(target_user, f"🎉 <b>Tabriklaymiz!</b> To'lovingiz tasdiqlandi.\n\nSizning hamyoningizga <b>{val} GWT</b> tashlab berildi! 💰")
            msg = f"Tasdiqlandi va {val} GWT berildi."
        else:
            db.add_premium_months(target_user, months)
            send_tg_msg(target_user, get_text('payment_approved', lang, months=months))
            msg = get_text('payment_approved_short', lang, months=months)

        status_text_for_admin = "✅ <b>TASDIQLANGAN VA BERILDI</b>"

    elif action == "reject":
        with db._conn() as c:
            c.execute("UPDATE premium_requests SET status='rejected' WHERE id=%s", (req_id,))
            c.execute("COMMIT")
        send_tg_msg(target_user, get_text('payment_rejected', lang))
        msg = get_text('payment_rejected_short', lang)
        status_text_for_admin = "❌ <b>BEKOR QILINGAN (RAD ETILDI)</b>"
    else:
        msg = "Error."
        status_text_for_admin = "Noma'lum"

    if admin_msg_ids:
        admin_info_text = f"👤 User ID: <code>{target_user}</code>\n📝 Holat: {status_text_for_admin}\n👨‍💻 Veb-saytdan <b>{user['first_name']}</b> tomonidan bajarildi."
        pairs = admin_msg_ids.split(',')
        for pair in pairs:
            if ':' in pair:
                chat_id_str, msg_id_str = pair.split(':')
                edit_tg_msg_caption(chat_id_str, msg_id_str, admin_info_text)

    return redirect(f"/pendings?token={token}&msg={msg}")

@app.route("/tg-image")
def tg_image():
    token = request.args.get("token")
    path_data = request.args.get("path")
    if not token or not path_data: return "XATOLIK: Token yoki rasm manzili berilmadi!", 400

    user = validate_token(token)
    if not user or int(user["user_id"]) not in SUPERADMINS: return "XATOLIK: Ruxsat yo'q", 403

    if path_data.startswith("WEB_"):
        parts = path_data.split("_", 2)
        if len(parts) >= 3:
            filename = parts[2]
            return send_from_directory(os.path.join(app.root_path, "static", "uploads"), filename)
        return send_from_directory(os.path.join(app.root_path, "static", "uploads"), path_data.replace("WEB_", ""))

    if path_data.startswith("http"):
        if path_data.startswith("https://api.telegram.org/") or path_data.startswith("https://t.me/"):
            return redirect(path_data)
        return "XATOLIK: Noma'lum manzil!", 403

    file_id = path_data
    if path_data.startswith("BOT_") or path_data.startswith("GWT_"):
        parts = path_data.split("_", 2)
        if len(parts) >= 3:
            file_id = parts[2]
        else:
            return f"XATOLIK: Bazaga rasm file_id saqlanmagan: {path_data}", 404

    try:
        clean_token = BOT_TOKENS[0] if BOT_TOKENS else os.getenv("BOT_TOKEN", "").strip()
        if not clean_token: return "XATOLIK: Bot tokeni topilmadi!", 500

        res = requests.get(f"https://api.telegram.org/bot{clean_token}/getFile?file_id={file_id}", timeout=10).json()
        if not res.get("ok"):
            return f"TELEGRAM XATOSI: {res.get('description')}", 404

        file_path = res["result"]["file_path"]
        img_res = requests.get(f"https://api.telegram.org/file/bot{clean_token}/{file_path}", timeout=10)

        if img_res.status_code == 200:
            return img_res.content, 200, {'Content-Type': 'image/jpeg'}
        return f"XATOLIK: Yuklab bo'lmadi (HTTP {img_res.status_code})", 500
    except Exception as e:
        return f"SERVER XATOSI: {str(e)}", 500

@app.route("/q-image/<photo_id>")
def q_image(photo_id):
    token = request.args.get("token")
    user = validate_token(token)
    if not user: return abort(401)

    try:
        clean_token = BOT_TOKENS[0] if BOT_TOKENS else os.getenv("BOT_TOKEN", "").strip()
        if not clean_token: return abort(404)
        get_file_url = f"https://api.telegram.org/bot{clean_token}/getFile?file_id={photo_id}"
        res = requests.get(get_file_url, timeout=5).json()
        if res.get("ok"):
            file_path = res["result"]["file_path"]
            img_url = f"https://api.telegram.org/file/bot{clean_token}/{file_path}"
            return redirect(img_url)
    except Exception as e:
        logging.error(f"Telegram rasmini tortishda xatolik: {e}")
    return abort(404)

@app.route("/guide")
def guide_page():
    return render_template("guide.html")

@app.route("/api/support/history")
def support_history():
    token = request.args.get("token")
    user = validate_token(token)
    if not user: return jsonify([])

    with db._conn() as c:
        try:
            c.execute("SELECT sender, text, created_at, reaction FROM support_messages WHERE user_id=%s ORDER BY created_at ASC", (user["user_id"],))
        except Exception:
            c.execute("SELECT sender, text, created_at FROM support_messages WHERE user_id=%s ORDER BY created_at ASC", (user["user_id"],))
        msgs = [to_dict(r) for r in c.fetchall()]
    return jsonify(msgs)

@app.route("/api/support/send", methods=["POST"])
def support_send():
    data = request.json
    token = data.get("token")
    text = data.get("text", "").strip()

    user = validate_token(token)
    if not user or not text: return jsonify({"error": "Xato"}), 400

    msg_id = int(time.time())
    db.save_message(user["user_id"], msg_id, 'user', text, msg_id)

    admin_text = f"📩 <b>Web Supportdan yangi xabar!</b>\n\n👤 User ID: <code>{user['user_id']}</code>\n💬 Matn: {text}\n\n<i>Admin paneldan kirib javob bering.</i>"
    for adm in SUPERADMINS:
        send_tg_msg(adm, admin_text)

    return jsonify({"success": True})

@app.route("/admin/chats")
def admin_chats():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user or int(user["user_id"]) not in SUPERADMINS: return abort(403)

    with db._conn() as c:
        c.execute("""
            SELECT
                u.user_id as chat_id, u.user_id, u.first_name, u.last_name, u.username,
                (SELECT text FROM support_messages WHERE user_id=u.user_id ORDER BY created_at DESC LIMIT 1) as text,
                MAX(s.created_at) as updated_at
            FROM support_messages s
            JOIN users u ON s.user_id = u.user_id
            GROUP BY u.user_id ORDER BY updated_at DESC
        """)
        chat_list = [to_dict(row) for row in c.fetchall()]

    return render_template("chats.html", token=token, chats=chat_list, current_user_bg=user.get("custom_bg"), current_lock_bg=user.get("custom_lock_bg"), lang=lang, get_text=get_text)

@app.route("/admin/chat/<int:target_id>", methods=["GET", "POST"])
def admin_chat_detail(target_id):
    token = request.args.get("token") or request.form.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user or int(user["user_id"]) not in SUPERADMINS: return abort(403)

    target_user = to_dict(db.get_user(target_id))

    if request.method == "POST":
        action = request.form.get("action")
        if action == "send_msg":
            text = request.form.get("text", "").strip()
            if text and BOT_TOKENS:
                url = f"https://api.telegram.org/bot{BOT_TOKENS[0]}/sendMessage"
                res = requests.post(url, json={"chat_id": target_id, "text": text}).json()
                if res.get("ok"):
                    msg_id = res["result"]["message_id"]
                    db.save_message(target_id, msg_id, 'admin', text, int(time.time()))
        elif action == "react":
            msg_id = request.form.get("message_id")
            emoji = request.form.get("emoji", "👍")
            try:
                with db._conn() as c:
                    c.execute("UPDATE support_messages SET reaction=%s WHERE message_id=%s", (emoji, msg_id))
            except: pass
            if BOT_TOKENS and len(str(msg_id)) < 10:
                url = f"https://api.telegram.org/bot{BOT_TOKENS[0]}/setMessageReaction"
                requests.post(url, json={
                    "chat_id": target_id, "message_id": int(msg_id),
                    "reaction": [{"type": "emoji", "emoji": emoji}], "is_big": False
                })
        return redirect(f"/admin/chat/{target_id}?token={token}")

    messages = [to_dict(m) for m in db.get_user_messages(target_id)]
    return render_template("chat_detail.html", token=token, target_user=target_user, messages=messages, current_user_bg=user.get("custom_bg"), current_lock_bg=user.get("custom_lock_bg"), lang=lang, get_text=get_text)

@app.route("/admin/tests")
def admin_tests():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user or int(user["user_id"]) not in SUPERADMINS: return abort(403)
    if not user.get("is_verified"): return redirect("/captcha")

    raw_tests = db.get_all_tests_admin()
    tests = []
    for t in raw_tests:
        test_dict = to_dict(t)
        q_count, finished_count = db.stats(test_dict["test_id"])
        test_dict["q_count"] = q_count
        test_dict["finished_count"] = finished_count
        tests.append(test_dict)

    return render_template("admin_tests.html", token=token, tests=tests, base_url=WEB_BASE_URL, current_user_bg=user.get("custom_bg"), current_lock_bg=user.get("custom_lock_bg"), lang=lang, get_text=get_text)

@app.route("/admin/channels")
def admin_channels():
    """Admin panel - Majburiy kanallarni boshqarish"""
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")

    if not user or int(user["user_id"]) not in SUPERADMINS:
        return abort(403)
    if not user.get("is_verified"):
        return redirect(f"/captcha?token={token}")

    channels = [dict(c) for c in db.get_all_required_channels(active_only=False)]

    channel_rows = ""
    for ch in channels:
        # channel_id maydonida @ bo'lsa HTML escape xavfsiz qilish
        raw_cid = ch.get("channel_id") or ""
        cid_escaped = html.escape(raw_cid)
        # JS ichida ishlatish uchun tirnoqlardan xoli qilish
        cid_js = raw_cid.replace("'", "\\'").replace('"', '\\"')
        ctitle = html.escape(ch.get("channel_title") or "Nom yo'q")
        is_active = int(ch.get("is_active") or 0)

        if is_active:
            badge = '<span class="badge badge-active">✅ Aktiv</span>'
            toggle_btn = f'<button class="btn btn-warn" onclick="removeChannel(\'{cid_js}\')">⏸ Deaktiv</button>'
        else:
            badge = '<span class="badge badge-inactive">❌ O\'chirilgan</span>'
            toggle_btn = f'<button class="btn btn-success" onclick="activateChannel(\'{cid_js}\')">▶️ Yoqish</button>'

        channel_rows += f"""
        <div class="ch-row" id="row-{cid_escaped.replace('@','')}">
          <div class="ch-info">
            <div class="ch-name">{cid_escaped} {badge}</div>
            <div class="ch-title">{ctitle}</div>
          </div>
          <div class="ch-btns">
            {toggle_btn}
            <button class="btn btn-danger" onclick="deleteChannel('{cid_js}')">🗑 O'chirish</button>
          </div>
        </div>"""

    if not channel_rows:
        channel_rows = '<div class="empty">Hozircha hech qanday kanal qo\'shilmagan</div>'

    count = len(channels)
    active_count = sum(1 for c in channels if int(c.get("is_active") or 0))

    page = f"""<!DOCTYPE html>
<html lang="uz">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>📢 Kanallar Boshqaruvi</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
      min-height: 100vh;
      padding: 24px 16px;
      color: #e2e8f0;
    }}
    .wrap {{ max-width: 860px; margin: 0 auto; }}
    .topbar {{
      display: flex; align-items: center; gap: 14px;
      margin-bottom: 24px;
    }}
    .back-btn {{
      padding: 10px 18px; border-radius: 10px;
      background: rgba(255,255,255,0.1);
      color: #e2e8f0; text-decoration: none; font-weight: 600;
      border: 1px solid rgba(255,255,255,0.15);
      transition: background .2s;
    }}
    .back-btn:hover {{ background: rgba(255,255,255,0.18); }}
    h1 {{ font-size: 22px; font-weight: 700; }}
    .card {{
      background: rgba(255,255,255,0.07);
      backdrop-filter: blur(12px);
      border: 1px solid rgba(255,255,255,0.12);
      border-radius: 16px;
      padding: 24px;
      margin-bottom: 20px;
    }}
    .card h2 {{ font-size: 17px; margin-bottom: 16px; color: #63b3ed; }}
    .stats {{ display: flex; gap: 16px; margin-bottom: 20px; flex-wrap: wrap; }}
    .stat-box {{
      flex: 1; min-width: 120px;
      background: rgba(0,0,0,0.25);
      border-radius: 12px; padding: 16px;
      text-align: center;
    }}
    .stat-num {{ font-size: 28px; font-weight: 700; color: #68d391; }}
    .stat-lbl {{ font-size: 12px; color: #a0aec0; margin-top: 4px; }}
    .input-row {{ display: flex; gap: 10px; margin-bottom: 10px; }}
    .input-row input {{
      flex: 1; padding: 12px 14px;
      background: rgba(0,0,0,0.3);
      border: 2px solid rgba(255,255,255,0.15);
      border-radius: 10px; color: #e2e8f0; font-size: 15px;
      outline: none; transition: border-color .2s;
    }}
    .input-row input:focus {{ border-color: #63b3ed; }}
    .input-row input::placeholder {{ color: #718096; }}
    .btn {{
      padding: 11px 18px; border: none;
      border-radius: 10px; font-weight: 600;
      cursor: pointer; font-size: 14px;
      transition: opacity .2s, transform .1s;
      white-space: nowrap;
    }}
    .btn:hover {{ opacity: .88; transform: translateY(-1px); }}
    .btn:active {{ transform: translateY(0); }}
    .btn:disabled {{ opacity: .45; cursor: not-allowed; transform: none; }}
    .btn-blue   {{ background: #3b82f6; color: #fff; }}
    .btn-success {{ background: #48bb78; color: #fff; }}
    .btn-warn   {{ background: #ed8936; color: #fff; }}
    .btn-danger {{ background: #f56565; color: #fff; }}
    .hint {{ color: #718096; font-size: 13px; margin-top: 8px; }}
    /* Alert */
    #alert-box {{
      display: none; padding: 12px 16px; border-radius: 10px;
      margin-bottom: 14px; font-size: 14px; font-weight: 500;
    }}
    .alert-ok  {{ background: rgba(72,187,120,.2); color: #68d391; border: 1px solid rgba(72,187,120,.4); }}
    .alert-err {{ background: rgba(245,101,101,.2); color: #fc8181; border: 1px solid rgba(245,101,101,.4); }}
    /* Channel rows */
    .ch-row {{
      display: flex; align-items: center; justify-content: space-between;
      gap: 12px; padding: 14px 16px;
      background: rgba(0,0,0,0.2);
      border: 1px solid rgba(255,255,255,0.08);
      border-radius: 12px; margin-bottom: 10px;
      transition: background .2s;
      flex-wrap: wrap;
    }}
    .ch-row:hover {{ background: rgba(0,0,0,0.32); }}
    .ch-info {{ flex: 1; min-width: 160px; }}
    .ch-name {{ font-weight: 700; font-size: 16px; margin-bottom: 4px; }}
    .ch-title {{ font-size: 13px; color: #a0aec0; }}
    .ch-btns {{ display: flex; gap: 8px; flex-wrap: wrap; }}
    .badge {{
      display: inline-block; padding: 2px 10px; border-radius: 999px;
      font-size: 11px; font-weight: 700; margin-left: 8px;
    }}
    .badge-active   {{ background: rgba(72,187,120,.25); color: #68d391; }}
    .badge-inactive {{ background: rgba(245,101,101,.25); color: #fc8181; }}
    .empty {{
      text-align: center; padding: 48px; color: #718096; font-size: 15px;
    }}
    /* Loading spinner */
    .spinner {{
      display: inline-block; width: 14px; height: 14px;
      border: 2px solid rgba(255,255,255,.4);
      border-top-color: #fff; border-radius: 50%;
      animation: spin .7s linear infinite; vertical-align: middle;
      margin-right: 6px;
    }}
    @keyframes spin {{ to {{ transform: rotate(360deg); }} }}
  </style>
</head>
<body>
<div class="wrap">

  <div class="topbar">
    <a href="/?token={token}" class="back-btn">← Orqaga</a>
    <h1>📢 Majburiy Kanallar Boshqaruvi</h1>
  </div>

  <!-- Statistika -->
  <div class="stats">
    <div class="stat-box">
      <div class="stat-num">{count}</div>
      <div class="stat-lbl">Jami kanal</div>
    </div>
    <div class="stat-box">
      <div class="stat-num" style="color:#68d391">{active_count}</div>
      <div class="stat-lbl">Aktiv</div>
    </div>
    <div class="stat-box">
      <div class="stat-num" style="color:#fc8181">{count - active_count}</div>
      <div class="stat-lbl">Deaktiv</div>
    </div>
  </div>

  <!-- Kanal qo'shish -->
  <div class="card">
    <h2>➕ Yangi Kanal Qo'shish</h2>
    <div id="alert-box"></div>
    <div class="input-row">
      <input type="text" id="ch-input"
             placeholder="@kanal_nomi yoki guruh username'i"
             onkeydown="if(event.key==='Enter') addChannel()">
      <button class="btn btn-blue" id="add-btn" onclick="addChannel()">
        Qo'shish
      </button>
    </div>
    <p class="hint">⚠️ Bot o'sha kanal/guruhga <b>admin</b> qilingan bo'lishi shart!</p>
    <p class="hint" style="margin-top:6px">💡 Misol: <code style="background:rgba(255,255,255,.1);padding:2px 6px;border-radius:4px">@geo_ustoz_kanal</code></p>
  </div>

  <!-- Kanallar ro'yxati -->
  <div class="card">
    <h2>📋 Kanallar ro'yxati</h2>
    <div id="ch-list">
      {channel_rows}
    </div>
  </div>

</div>

<script>
const TOKEN = '{token}';

function showAlert(msg, ok) {{
  const el = document.getElementById('alert-box');
  el.className = ok ? 'alert-ok' : 'alert-err';
  el.style.display = 'block';
  el.innerHTML = msg;
  clearTimeout(el._t);
  el._t = setTimeout(() => {{ el.style.display = 'none'; }}, 4500);
}}

function setLoading(btnId, loading) {{
  const btn = document.getElementById(btnId);
  if (!btn) return;
  btn.disabled = loading;
  if (loading) {{
    btn._orig = btn.innerHTML;
    btn.innerHTML = '<span class="spinner"></span>Kutilmoqda...';
  }} else {{
    btn.innerHTML = btn._orig || btn.innerHTML;
  }}
}}

function apiFetch(url, body, btnId) {{
  if (btnId) setLoading(btnId, true);
  return fetch(url, {{
    method: 'POST',
    headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify(Object.assign({{token: TOKEN}}, body))
  }})
  .then(r => {{
    if (!r.ok) throw new Error('HTTP ' + r.status);
    return r.json();
  }})
  .finally(() => {{ if (btnId) setLoading(btnId, false); }});
}}

function addChannel() {{
  const input = document.getElementById('ch-input');
  let cid = input.value.trim();
  if (!cid) {{ showAlert('❌ Kanal nomini kiriting!', false); input.focus(); return; }}
  if (!cid.startsWith('@')) {{ showAlert('❌ Kanal nomi @ bilan boshlanishi kerak!', false); input.focus(); return; }}

  apiFetch('/api/admin/channel/add', {{channel_id: cid}}, 'add-btn')
    .then(d => {{
      if (d.success) {{
        showAlert('✅ <b>' + cid + '</b> muvaffaqiyatli qo\'shildi!', true);
        input.value = '';
        setTimeout(() => location.reload(), 1200);
      }} else {{
        showAlert('❌ ' + (d.error || 'Xato yuz berdi'), false);
      }}
    }})
    .catch(e => showAlert('❌ Server bilan aloqa uzildi: ' + e.message, false));
}}

function removeChannel(cid) {{
  if (!confirm('Kanal deaktivatsiya qilinadi (qayta yoqish mumkin). Davom etasizmi?')) return;
  apiFetch('/api/admin/channel/remove', {{channel_id: cid}})
    .then(d => {{
      if (d.success) {{
        showAlert('⏸ <b>' + cid + '</b> deaktivatsiya qilindi', true);
        setTimeout(() => location.reload(), 1000);
      }} else {{
        showAlert('❌ ' + (d.error || 'Xato'), false);
      }}
    }})
    .catch(e => showAlert('❌ ' + e.message, false));
}}

function activateChannel(cid) {{
  apiFetch('/api/admin/channel/activate', {{channel_id: cid}})
    .then(d => {{
      if (d.success) {{
        showAlert('▶️ <b>' + cid + '</b> qayta yoqildi!', true);
        setTimeout(() => location.reload(), 1000);
      }} else {{
        showAlert('❌ ' + (d.error || 'Xato'), false);
      }}
    }})
    .catch(e => showAlert('❌ ' + e.message, false));
}}

function deleteChannel(cid) {{
  if (!confirm('⚠️ DIQQAT!\n\n"' + cid + '" kanali BUTUNLAY bazadan o\'chiriladi!\nBu amalni qaytarib bo\'lmaydi.\n\nDavom etasizmi?')) return;
  apiFetch('/api/admin/channel/delete', {{channel_id: cid}})
    .then(d => {{
      if (d.success) {{
        showAlert('🗑 <b>' + cid + '</b> butunlay o\'chirildi', true);
        setTimeout(() => location.reload(), 1000);
      }} else {{
        showAlert('❌ ' + (d.error || 'Xato'), false);
      }}
    }})
    .catch(e => showAlert('❌ ' + e.message, false));
}}
</script>
</body>
</html>"""
    return page

@app.route("/api/admin/channel/add", methods=["POST"])
def api_admin_channel_add():
    """API: Yangi kanal qo'shish"""
    global REQUIRED_CHANNELS
    data = request.json or {}
    token = data.get("token")
    channel_id = data.get("channel_id", "").strip()
    
    user = validate_token(token)
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return jsonify({"success": False, "error": "Ruxsat yo'q"}), 403
    
    if not channel_id or not channel_id.startswith("@"):
        return jsonify({"success": False, "error": "Noto'g'ri kanal nomi"}), 400
    
    # Telegram orqali kanalni tekshirish
    if BOT_TOKENS:
        url = f"https://api.telegram.org/bot{BOT_TOKENS[0]}/getChat?chat_id={channel_id}"
        try:
            res = requests.get(url, timeout=5).json()
            if not res.get("ok"):
                return jsonify({"success": False, "error": f"Kanal topilmadi: {res.get('description')}"}), 400
            channel_title = res["result"].get("title", channel_id)
        except Exception as e:
            return jsonify({"success": False, "error": f"Xato: {str(e)}"}), 500
    else:
        channel_title = channel_id
    
    # Bazaga qo'shish
    success = db.add_required_channel(channel_id, channel_title, user["user_id"])
    
    if success:
        REQUIRED_CHANNELS = load_required_channels_from_db()
        return jsonify({"success": True})
    else:
        return jsonify({"success": False, "error": "Bu kanal allaqachon ro'yxatda"}), 400

@app.route("/api/admin/channel/remove", methods=["POST"])
def api_admin_channel_remove():
    """API: Kanalni o'chirish (deaktiv)"""
    global REQUIRED_CHANNELS
    data = request.json or {}
    token = data.get("token")
    channel_id = data.get("channel_id")
    
    user = validate_token(token)
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return jsonify({"success": False, "error": "Ruxsat yo'q"}), 403
    
    db.remove_required_channel(channel_id)
    REQUIRED_CHANNELS = load_required_channels_from_db()
    
    return jsonify({"success": True})

@app.route("/api/admin/channel/activate", methods=["POST"])
def api_admin_channel_activate():
    """API: Kanalni qayta yoqish"""
    global REQUIRED_CHANNELS
    data = request.json or {}
    token = data.get("token")
    channel_id = data.get("channel_id")
    
    user = validate_token(token)
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return jsonify({"success": False, "error": "Ruxsat yo'q"}), 403
    
    db.activate_required_channel(channel_id)
    REQUIRED_CHANNELS = load_required_channels_from_db()
    
    return jsonify({"success": True})

@app.route("/api/admin/channel/delete", methods=["POST"])
def api_admin_channel_delete():
    """API: Kanalni butunlay o'chirish"""
    global REQUIRED_CHANNELS
    data = request.json or {}
    token = data.get("token")
    channel_id = data.get("channel_id")
    
    user = validate_token(token)
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return jsonify({"success": False, "error": "Ruxsat yo'q"}), 403
    
    db.delete_required_channel(channel_id)
    REQUIRED_CHANNELS = load_required_channels_from_db()
    
    return jsonify({"success": True})

# ==========================================
# 📊 STATISTIKA, GAMIFICATION, KUTUBXONA SAHIFALARI
# ==========================================
_PAGE_STYLE = """
<style>
  *{box-sizing:border-box;margin:0;padding:0;font-family:ui-sans-serif,system-ui,-apple-system,'Segoe UI',sans-serif}
  body{background:radial-gradient(circle at top,#101b34 0%,#070b15 70%,#05060d 100%);color:#f3f6ff;min-height:100vh;padding:20px}
  .wrap{max-width:900px;margin:0 auto}
  .nav{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:24px}
  .nav a{padding:9px 14px;border-radius:999px;background:rgba(255,255,255,.08);color:#f3f6ff;text-decoration:none;border:1px solid rgba(255,255,255,.12);font-size:14px}
  .nav a:hover{background:rgba(255,255,255,.16)}
  .card{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.12);border-radius:16px;padding:22px;margin-bottom:16px;backdrop-filter:blur(12px)}
  h1{font-size:24px;margin-bottom:18px}
  h2{font-size:18px;color:#38d39f;margin-bottom:12px}
  .stat-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:14px}
  .stat-box{background:rgba(0,0,0,.25);border-radius:12px;padding:16px;text-align:center}
  .stat-num{font-size:26px;font-weight:700;color:#38d39f}
  .stat-label{font-size:13px;color:rgba(243,246,255,.65);margin-top:4px}
  .bar{height:14px;background:rgba(0,0,0,.3);border-radius:999px;overflow:hidden;margin:10px 0}
  .bar-fill{height:100%;background:linear-gradient(90deg,#38d39f,#6cb2ff)}
  .ach{display:flex;align-items:center;gap:14px;padding:12px;border-radius:12px;margin-bottom:8px;background:rgba(0,0,0,.2)}
  .ach.locked{opacity:.45}
  .ach .em{font-size:30px}
  .row{display:flex;align-items:center;justify-content:space-between;padding:12px;border-radius:10px;margin-bottom:8px;background:rgba(0,0,0,.25)}
  .row.me{background:rgba(56,211,159,.15);border:1px solid rgba(56,211,159,.4)}
  .btn{display:inline-block;padding:10px 16px;border-radius:10px;background:rgba(56,211,159,.15);color:#38d39f;border:1px solid rgba(56,211,159,.3);text-decoration:none;cursor:pointer;font-weight:600;font-size:14px}
  .tag{display:inline-block;padding:4px 10px;border-radius:999px;background:rgba(108,178,255,.15);color:#6cb2ff;font-size:12px;margin:2px}
  input,select,textarea{width:100%;padding:11px 14px;border-radius:10px;border:1px solid rgba(255,255,255,.16);background:rgba(0,0,0,.2);color:#f3f6ff;font-size:14px;margin-bottom:12px}
  .empty{text-align:center;padding:40px;color:rgba(243,246,255,.5)}
</style>
"""

def _nav_html(token, lang="uz"):
    L = {
        "uz": ["🏠 Bosh sahifa","📊 Statistika","🏅 Yutuqlar","🏆 Reyting","📚 Kutubxona","👥 Guruhlar","🔔 Bildirishnomalar"],
        "uz_cyrl": ["🏠 Бош саҳифа","📊 Статистика","🏅 Ютуқлар","🏆 Рейтинг","📚 Кутубхона","👥 Гуруҳлар","🔔 Билдиришномалар"],
        "ru": ["🏠 Главная","📊 Статистика","🏅 Достижения","🏆 Рейтинг","📚 Библиотека","👥 Группы","🔔 Уведомления"],
    }.get(lang, None)
    if L is None:
        L = ["🏠 Bosh sahifa","📊 Statistika","🏅 Yutuqlar","🏆 Reyting","📚 Kutubxona","👥 Guruhlar","🔔 Bildirishnomalar"]
    paths = ["/","/stats","/achievements","/leaderboard","/library","/my-groups","/notifications"]
    links = "".join([f'<a href="{p}?token={token}">{name}</a>' for p, name in zip(paths, L)])
    return f'<div class="nav">{links}</div>'

@app.route("/stats")
def web_stats():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user:
        return abort(401, get_text('login_first', lang))
    uid = int(user["user_id"])
    stats = to_dict(db.get_user_stats(uid)) or {}
    level = int(stats.get("level", 1) or 1)
    xp = int(stats.get("xp", 0) or 0)
    cur_lvl_xp = db.xp_for_level(level)
    next_lvl_xp = db.xp_for_level(level + 1)
    needed = max(1, next_lvl_xp - cur_lvl_xp)
    pct = min(100, int((xp - cur_lvl_xp) * 100 / needed))
    total_q = int(stats.get("total_questions", 0) or 0)
    total_c = int(stats.get("total_correct", 0) or 0)
    accuracy = round(total_c * 100 / total_q, 1) if total_q > 0 else 0
    progress = db.get_user_progress(uid)
    progress_rows = "".join([f"<div class='row'><span>{to_dict(p)['day']}</span><span>{to_dict(p)['cnt']} test · {round(float(to_dict(p)['avg_score'] or 0),1)} ball</span></div>" for p in progress]) or f"<div class='empty'>{'Маълумот йўқ' if lang=='uz_cyrl' else ('Нет данных' if lang=='ru' else 'Malumot yoq')}</div>"
    html_page = _PAGE_STYLE + f"""
    <div class="wrap">
      {_nav_html(token, lang)}
      <h1>📊 {'Статистика' if lang=='ru' else ('Статистика' if lang=='uz_cyrl' else 'Statistika')}</h1>
      <div class="card">
        <h2>⭐ {'Уровень' if lang=='ru' else ('Даража' if lang=='uz_cyrl' else 'Daraja')} {level}</h2>
        <div class="bar"><div class="bar-fill" style="width:{pct}%"></div></div>
        <p style="color:rgba(243,246,255,.65);font-size:13px">{xp} / {next_lvl_xp} XP ({pct}%)</p>
      </div>
      <div class="card">
        <div class="stat-grid">
          <div class="stat-box"><div class="stat-num">{int(stats.get('tests_taken',0) or 0)}</div><div class="stat-label">{'Тестов пройдено' if lang=='ru' else ('Ишланган' if lang=='uz_cyrl' else 'Ishlangan')}</div></div>
          <div class="stat-box"><div class="stat-num">{int(stats.get('tests_created',0) or 0)}</div><div class="stat-label">{'Создано' if lang=='ru' else ('Яратилган' if lang=='uz_cyrl' else 'Yaratilgan')}</div></div>
          <div class="stat-box"><div class="stat-num">{accuracy}%</div><div class="stat-label">{'Точность' if lang=='ru' else ('Аниқлик' if lang=='uz_cyrl' else 'Aniqlik')}</div></div>
          <div class="stat-box"><div class="stat-num">🔥 {int(stats.get('current_streak',0) or 0)}</div><div class="stat-label">{'Серия' if lang=='ru' else ('Серия' if lang=='uz_cyrl' else 'Streak')}</div></div>
        </div>
      </div>
      <div class="card">
        <h2>📈 {'Последние 30 дней' if lang=='ru' else ('Сўнгги 30 кун' if lang=='uz_cyrl' else 'Songgi 30 kun')}</h2>
        {progress_rows}
      </div>
    </div>
    """
    return html_page

@app.route("/achievements")
def web_achievements():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user:
        return abort(401, get_text('login_first', lang))
    uid = int(user["user_id"])
    earned = {dict(a)['code'] for a in db.get_user_achievements(uid)}
    all_ach = db.get_all_achievements()
    items = ""
    for a in all_ach:
        a = dict(a)
        name = (a.get('title_ru') if lang == 'ru' else a.get('title_uz'))
        desc = (a.get('description_ru') if lang == 'ru' else a.get('description_uz'))
        locked = "" if a['code'] in earned else "locked"
        check = "✅" if a['code'] in earned else "🔒"
        items += f"<div class='ach {locked}'><div class='em'>{a.get('emoji','🏅')}</div><div><b>{name}</b> {check}<br><span style='color:rgba(243,246,255,.6);font-size:13px'>{desc} · +{a.get('xp_reward',0)} XP</span></div></div>"
    html_page = _PAGE_STYLE + f"""
    <div class="wrap">
      {_nav_html(token, lang)}
      <h1>🏅 {'Достижения' if lang=='ru' else ('Ютуқлар' if lang=='uz_cyrl' else 'Yutuqlar')} ({len(earned)}/{len(all_ach)})</h1>
      <div class="card">{items}</div>
    </div>
    """
    return html_page

@app.route("/leaderboard")
def web_leaderboard():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user:
        return abort(401, get_text('login_first', lang))
    uid = int(user["user_id"])
    top_100, user_data = db.get_current_month_leaderboard(uid)
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    rows = ""
    for item in top_100[:50]:
        me = "me" if item['user_id'] == uid else ""
        medal = medals.get(item['rank'], f"{item['rank']}.")
        rows += f"<div class='row {me}'><span>{medal} {html.escape(str(item.get('name') or 'User'))}</span><span><b>{item['score']}</b> ball</span></div>"
    if not rows:
        rows = f"<div class='empty'>{'Нет данных' if lang=='ru' else ('Маълумот йўқ' if lang=='uz_cyrl' else 'Malumot yoq')}</div>"
    my = ""
    if user_data:
        my = f"<div class='card'><div class='row me'><span>👤 {'Ваше место' if lang=='ru' else ('Сизнинг ўрнингиз' if lang=='uz_cyrl' else 'Sizning orningiz')}</span><span><b>#{user_data['rank']}</b> · {user_data['score']} ball</span></div></div>"
    html_page = _PAGE_STYLE + f"""
    <div class="wrap">
      {_nav_html(token, lang)}
      <h1>🏆 {'Месячный рейтинг' if lang=='ru' else ('Ойлик рейтинг' if lang=='uz_cyrl' else 'Oylik reyting')}</h1>
      {my}
      <div class="card">{rows}</div>
    </div>
    """
    return html_page

@app.route("/library")
def web_library():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user:
        return abort(401, get_text('login_first', lang))
    cat_id = request.args.get("category", type=int)
    query = request.args.get("q", "").strip()
    categories = db.get_categories()
    tests = db.search_tests_advanced(query=query or None, category_id=cat_id, limit=50)
    cat_chips = f'<a class="tag" href="/library?token={token}">{"Все" if lang=="ru" else ("Барчаси" if lang=="uz_cyrl" else "Barchasi")}</a>'
    for c in categories:
        c = dict(c)
        cat_chips += f'<a class="tag" href="/library?token={token}&category={c["id"]}">{c.get("emoji","")} {c["name"]}</a>'
    test_cards = ""
    for t in tests:
        t = dict(t)
        rating = db.get_test_rating(t['test_id'])
        stars = "⭐" * int(round(rating['avg_rating'])) if rating['avg_rating'] else ""
        pname = t.get('public_name') or t['test_id']
        test_cards += f"""<div class='row'><div><b>{html.escape(t.get('title') or '—')}</b><br>
          <span style='color:rgba(243,246,255,.6);font-size:13px'>@{html.escape(pname)} · ▶️ {t.get('plays',0)} · {stars} {rating['avg_rating'] or ''}</span></div>
          <a class='btn' href='/solve/{t['test_id']}?token={token}'>{'Решить' if lang=='ru' else ('Ишлаш' if lang=='uz_cyrl' else 'Yechish')}</a></div>"""
    if not test_cards:
        test_cards = f"<div class='empty'>{'Тесты не найдены' if lang=='ru' else ('Тест топилмади' if lang=='uz_cyrl' else 'Test topilmadi')}</div>"
    placeholder = 'Поиск...' if lang == 'ru' else ('Қидирув...' if lang == 'uz_cyrl' else 'Qidiruv...')
    html_page = _PAGE_STYLE + f"""
    <div class="wrap">
      {_nav_html(token, lang)}
      <h1>📚 {'Библиотека тестов' if lang=='ru' else ('Тестлар кутубхонаси' if lang=='uz_cyrl' else 'Testlar kutubxonasi')}</h1>
      <div class="card">
        <form method="get" action="/library">
          <input type="hidden" name="token" value="{token}">
          <input type="text" name="q" value="{html.escape(query)}" placeholder="{placeholder}">
          <button class="btn" type="submit">🔍 {'Поиск' if lang=='ru' else ('Қидирув' if lang=='uz_cyrl' else 'Qidirish')}</button>
        </form>
        <div style="margin-top:14px">{cat_chips}</div>
      </div>
      <div class="card">{test_cards}</div>
    </div>
    """
    return html_page

@app.route("/my-groups")
def web_my_groups():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user:
        return abort(401, get_text('login_first', lang))
    uid = int(user["user_id"])
    groups = db.get_user_groups(uid)
    cards = ""
    for g in groups:
        g = dict(g)
        role = "👨‍🏫" if g.get('role') == 'teacher' else "🎓"
        cards += f"""<div class='row'><div><b>{role} {html.escape(g.get('name'))}</b><br>
          <span style='color:rgba(243,246,255,.6);font-size:13px'>🔑 {g.get('join_code')}</span></div>
          <a class='btn' href='/group/{g['group_id']}?token={token}'>{'Открыть' if lang=='ru' else ('Очиш' if lang=='uz_cyrl' else 'Ochish')}</a></div>"""
    if not cards:
        cards = f"<div class='empty'>{'Нет групп' if lang=='ru' else ('Гуруҳ йўқ' if lang=='uz_cyrl' else 'Guruh yoq')}</div>"
    html_page = _PAGE_STYLE + f"""
    <div class="wrap">
      {_nav_html(token, lang)}
      <h1>👥 {'Мои группы' if lang=='ru' else ('Менинг гуруҳларим' if lang=='uz_cyrl' else 'Mening guruhlarim')}</h1>
      <div class="card">
        <h2>➕ {'Создать группу' if lang=='ru' else ('Гуруҳ яратиш' if lang=='uz_cyrl' else 'Guruh yaratish')}</h2>
        <input type="text" id="gname" placeholder="{'Название' if lang=='ru' else ('Номи' if lang=='uz_cyrl' else 'Nomi')}">
        <button class="btn" onclick="createGroup()">{'Создать' if lang=='ru' else ('Яратиш' if lang=='uz_cyrl' else 'Yaratish')}</button>
        <h2 style="margin-top:18px">🔑 {'Присоединиться' if lang=='ru' else ('Қўшилиш' if lang=='uz_cyrl' else 'Qoshilish')}</h2>
        <input type="text" id="gcode" placeholder="{'Код' if lang=='ru' else ('Код' if lang=='uz_cyrl' else 'Kod')}">
        <button class="btn" onclick="joinGroup()">{'Войти' if lang=='ru' else ('Кириш' if lang=='uz_cyrl' else 'Kirish')}</button>
      </div>
      <div class="card">{cards}</div>
    </div>
    <script>
      const token="{token}";
      function createGroup(){{
        const name=document.getElementById('gname').value.trim();
        if(!name)return;
        fetch('/api/group/create',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{token,name}})}})
          .then(r=>r.json()).then(d=>{{if(d.success)location.reload();else alert(d.error||'Error');}});
      }}
      function joinGroup(){{
        const code=document.getElementById('gcode').value.trim();
        if(!code)return;
        fetch('/api/group/join',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{token,code}})}})
          .then(r=>r.json()).then(d=>{{if(d.success)location.reload();else alert(d.error||'Error');}});
      }}
    </script>
    """
    return html_page

@app.route("/group/<group_id>")
def web_group_detail(group_id):
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user:
        return abort(401, get_text('login_first', lang))
    group = to_dict(db.get_study_group(group_id))
    if not group:
        return abort(404)
    members = db.get_group_members(group_id)
    board = db.get_group_leaderboard(group_id)
    assignments = db.get_group_assignments(group_id)
    mem_rows = "".join([f"<div class='row'><span>{'👨‍🏫' if dict(m).get('role')=='teacher' else '🎓'} {html.escape(dict(m).get('first_name') or 'User')}</span></div>" for m in members])
    board_rows = "".join([f"<div class='row'><span>{html.escape(dict(b).get('first_name') or 'User')}</span><span>{dict(b).get('tests_done',0)} test · {round(float(dict(b).get('total_score') or 0),1)} ball</span></div>" for b in board]) or "<div class='empty'>—</div>"
    asg_rows = "".join([f"<div class='row'><span>📝 {html.escape(dict(a).get('title') or '—')}</span><a class='btn' href='/solve/{dict(a)['test_id']}?token={token}'>▶️</a></div>" for a in assignments]) or "<div class='empty'>—</div>"
    html_page = _PAGE_STYLE + f"""
    <div class="wrap">
      {_nav_html(token, lang)}
      <h1>👥 {html.escape(group.get('name'))}</h1>
      <div class="card"><h2>🔑 {'Код' if lang=='ru' else ('Код' if lang=='uz_cyrl' else 'Kod')}: {group.get('join_code')}</h2></div>
      <div class="card"><h2>📝 {'Задания' if lang=='ru' else ('Вазифалар' if lang=='uz_cyrl' else 'Vazifalar')}</h2>{asg_rows}</div>
      <div class="card"><h2>🏆 {'Рейтинг' if lang=='ru' else ('Рейтинг' if lang=='uz_cyrl' else 'Reyting')}</h2>{board_rows}</div>
      <div class="card"><h2>👤 {'Участники' if lang=='ru' else ('Аъзолар' if lang=='uz_cyrl' else 'Azolar')} ({len(members)})</h2>{mem_rows}</div>
    </div>
    """
    return html_page

@app.route("/notifications")
def web_notifications():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user:
        return abort(401, get_text('login_first', lang))
    uid = int(user["user_id"])
    notifs = db.get_notifications(uid, limit=30)
    db.mark_all_read(uid)
    rows = ""
    for n in notifs:
        n = dict(n)
        rows += f"<div class='row'><div><b>{html.escape(n.get('title') or '')}</b><br><span style='color:rgba(243,246,255,.6);font-size:13px'>{html.escape(n.get('body') or '')}</span></div></div>"
    if not rows:
        rows = f"<div class='empty'>{'Нет уведомлений' if lang=='ru' else ('Билдиришнома йўқ' if lang=='uz_cyrl' else 'Bildirishnoma yoq')}</div>"
    html_page = _PAGE_STYLE + f"""
    <div class="wrap">
      {_nav_html(token, lang)}
      <h1>🔔 {'Уведомления' if lang=='ru' else ('Билдиришномалар' if lang=='uz_cyrl' else 'Bildirishnomalar')}</h1>
      <div class="card">{rows}</div>
    </div>
    """
    return html_page

# ==========================================
# 📡 GURUH, SHARH, BOOKMARK API'LARI
# ==========================================
@app.route("/api/group/create", methods=["POST"])
def api_group_create():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user:
        return jsonify({"success": False, "error": "Not logged in"}), 401
    name = (data.get("name") or "").strip()[:100]
    if not name:
        return jsonify({"success": False, "error": "Name required"}), 400
    group_id = uuid.uuid4().hex[:12]
    join_code = uuid.uuid4().hex[:6].upper()
    try:
        db.create_study_group(group_id, name, int(user["user_id"]), join_code)
        return jsonify({"success": True, "group_id": group_id, "join_code": join_code})
    except Exception as e:
        logging.error(f"Group create error: {e}")
        return jsonify({"success": False, "error": "Server error"}), 500

@app.route("/api/group/join", methods=["POST"])
def api_group_join():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user:
        return jsonify({"success": False, "error": "Not logged in"}), 401
    code = (data.get("code") or "").strip().upper()
    group = db.get_group_by_code(code)
    if not group:
        return jsonify({"success": False, "error": "Group not found"}), 404
    db.join_group(dict(group)["group_id"], int(user["user_id"]), "student")
    return jsonify({"success": True})

@app.route("/api/review/add", methods=["POST"])
def api_review_add():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user:
        return jsonify({"success": False, "error": "Not logged in"}), 401
    test_id = data.get("test_id")
    rating = int(data.get("rating", 5))
    comment = (data.get("comment") or "")[:500]
    if not test_id or rating < 1 or rating > 5:
        return jsonify({"success": False, "error": "Invalid data"}), 400
    db.add_review(test_id, int(user["user_id"]), rating, comment)
    return jsonify({"success": True})

@app.route("/api/bookmark/toggle", methods=["POST"])
def api_bookmark_toggle():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user:
        return jsonify({"success": False, "error": "Not logged in"}), 401
    test_id = data.get("test_id")
    uid = int(user["user_id"])
    if db.is_bookmarked(uid, test_id):
        db.remove_bookmark(uid, test_id)
        return jsonify({"success": True, "bookmarked": False})
    else:
        db.add_bookmark(uid, test_id)
        return jsonify({"success": True, "bookmarked": True})

@app.route("/api/notifications/count")
def api_notifications_count():
    user = validate_token(request.args.get("token"))
    if not user:
        return jsonify({"count": 0})
    return jsonify({"count": db.count_unread_notifications(int(user["user_id"]))})

@app.route("/admin/stats")
def admin_stats():
    """Admin uchun global statistika dashboard"""
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return abort(403)
    g = db.get_global_stats()
    top_tests = db.get_top_tests(limit=10)
    top_rows = "".join([f"<div class='row'><span>📝 {html.escape(dict(t).get('title') or '—')}</span><span>▶️ {dict(t).get('plays',0)}</span></div>" for t in top_tests]) or "<div class='empty'>—</div>"
    html_page = _PAGE_STYLE + f"""
    <div class="wrap">
      {_nav_html(token, lang)}
      <h1>📈 {'Глобальная статистика' if lang=='ru' else ('Глобал статистика' if lang=='uz_cyrl' else 'Global statistika')}</h1>
      <div class="card">
        <div class="stat-grid">
          <div class="stat-box"><div class="stat-num">{g['total_users']}</div><div class="stat-label">{'Пользователи' if lang=='ru' else ('Фойдаланувчилар' if lang=='uz_cyrl' else 'Foydalanuvchilar')}</div></div>
          <div class="stat-box"><div class="stat-num">{g['total_tests']}</div><div class="stat-label">{'Тесты' if lang=='ru' else ('Тестлар' if lang=='uz_cyrl' else 'Testlar')}</div></div>
          <div class="stat-box"><div class="stat-num">{g['total_sessions']}</div><div class="stat-label">{'Прохождений' if lang=='ru' else ('Ишланган' if lang=='uz_cyrl' else 'Ishlangan')}</div></div>
          <div class="stat-box"><div class="stat-num">{g['premium_users']}</div><div class="stat-label">Premium</div></div>
          <div class="stat-box"><div class="stat-num">+{g['new_users_today']}</div><div class="stat-label">{'Сегодня' if lang=='ru' else ('Бугун' if lang=='uz_cyrl' else 'Bugun')}</div></div>
        </div>
      </div>
      <div class="card">
        <h2>🔥 {'Популярные тесты' if lang=='ru' else ('Машҳур тестлар' if lang=='uz_cyrl' else 'Mashhur testlar')}</h2>
        {top_rows}
      </div>
    </div>
    """
    return html_page

@app.route("/export-excel/<test_id>")
def web_export_excel(test_id):
    """Test natijalarini ko'p varaqli Excel hisobot sifatida yuklab olish"""
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user:
        return abort(401, get_text('login_first', lang))
    test = to_dict(db.get_test(test_id))
    if not test:
        return abort(404, get_text('test_not_found', lang))
    if int(test.get("owner_user_id", 0)) != int(user["user_id"]) and int(user["user_id"]) not in SUPERADMINS:
        return abort(403)

    allr = db.all_results(test_id)
    wb = Workbook()

    # 1-varaq: Natijalar
    ws = wb.active
    ws.title = "Natijalar"
    for col, hname in enumerate(["O'rin", "Foydalanuvchi", "Ball", "To'g'ri", "Vaqt (s)"], 1):
        ws.cell(row=1, column=col).value = hname
    for i, r in enumerate(allr, 1):
        r = dict(r)
        try:
            with db._conn() as c:
                ans = c.execute("SELECT COUNT(*) as total, SUM(is_correct) as correct FROM answers WHERE session_id=%s", (r.get("session_id"),)).fetchone()
            correct_n = int((ans or {}).get("correct") or 0)
            total_n = int((ans or {}).get("total") or 0)
            correct_str = f"{correct_n}/{total_n}" if total_n else "-"
        except Exception:
            correct_str = "-"
        name = (r.get("username") and f"@{r.get('username')}") or (f"{r.get('first_name') or ''} {r.get('last_name') or ''}").strip() or f"User{r.get('user_id')}"
        ws.cell(row=i+1, column=1).value = i
        ws.cell(row=i+1, column=2).value = name
        ws.cell(row=i+1, column=3).value = float(r.get("score") or 0)
        ws.cell(row=i+1, column=4).value = correct_str
        ws.cell(row=i+1, column=5).value = int(r.get("duration_sec") or 0)

    # 2-varaq: Tahlil
    try:
        a = dict(db.get_test_analytics(test_id) or {})
        ws2 = wb.create_sheet("Tahlil")
        for ri, (k, v) in enumerate([
            ("Test nomi", test.get("title", "-")),
            ("Jami qatnashchilar", int(a.get("total_sessions") or 0)),
            ("O'rtacha ball", round(float(a.get("avg_score") or 0), 2)),
            ("Eng yuqori ball", round(float(a.get("max_score") or 0), 2)),
            ("Eng past ball", round(float(a.get("min_score") or 0), 2)),
            ("O'rtacha vaqt (s)", int(a.get("avg_duration") or 0)),
        ], 1):
            ws2.cell(row=ri, column=1).value = k
            ws2.cell(row=ri, column=2).value = v
    except Exception as e:
        logging.error(f"Web export tahlil xatosi: {e}")

    # 3-varaq: Savollar tahlili
    try:
        hardest = db.get_hardest_questions(test_id, limit=100)
        ws3 = wb.create_sheet("Savollar")
        for col, hname in enumerate(["#", "Savol", "Javoblar", "To'g'ri", "Muvaffaqiyat %"], 1):
            ws3.cell(row=1, column=col).value = hname
        for ri, q in enumerate(hardest, 1):
            q = dict(q)
            ws3.cell(row=ri+1, column=1).value = int(q.get("q_index", 0)) + 1
            ws3.cell(row=ri+1, column=2).value = (q.get("question") or "")[:120]
            ws3.cell(row=ri+1, column=3).value = int(q.get("total_answers") or 0)
            ws3.cell(row=ri+1, column=4).value = int(q.get("correct_answers") or 0)
            ws3.cell(row=ri+1, column=5).value = float(q.get("success_rate") or 0)
    except Exception as e:
        logging.error(f"Web export savollar xatosi: {e}")

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    filename = f"{(test.get('title') or 'test')}_natijalar.xlsx".replace(" ", "_")
    return Response(
        buf.getvalue(),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

@app.route("/test-analytics/<test_id>")
def web_test_analytics(test_id):
    """Test bo'yicha batafsil analitika sahifasi (egasi uchun)"""
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user:
        return abort(401, get_text('login_first', lang))
    test = to_dict(db.get_test(test_id))
    if not test:
        return abort(404)
    if int(test.get("owner_user_id", 0)) != int(user["user_id"]) and int(user["user_id"]) not in SUPERADMINS:
        return abort(403)
    a = dict(db.get_test_analytics(test_id) or {})
    hardest = db.get_hardest_questions(test_id, limit=10)
    rating = db.get_test_rating(test_id)
    hard_rows = ""
    for q in hardest:
        q = dict(q)
        rate = float(q.get("success_rate") or 0)
        color = "#ef4444" if rate < 40 else ("#fbbf24" if rate < 70 else "#38d39f")
        hard_rows += f"""<div class='row'><span>{int(q.get('q_index',0))+1}. {html.escape((q.get('question') or '')[:60])}</span>
          <span style='color:{color};font-weight:700'>{rate}%</span></div>"""
    if not hard_rows:
        hard_rows = "<div class='empty'>—</div>"
    html_page = _PAGE_STYLE + f"""
    <div class="wrap">
      {_nav_html(token, lang)}
      <h1>📈 {html.escape(test.get('title') or 'Test')}</h1>
      <div class="card">
        <div class="stat-grid">
          <div class="stat-box"><div class="stat-num">{int(a.get('total_sessions') or 0)}</div><div class="stat-label">{'Участников' if lang=='ru' else ('Қатнашчилар' if lang=='uz_cyrl' else 'Qatnashchilar')}</div></div>
          <div class="stat-box"><div class="stat-num">{round(float(a.get('avg_score') or 0),1)}</div><div class="stat-label">{'Средний' if lang=='ru' else ('Ўртача' if lang=='uz_cyrl' else 'Ortacha')}</div></div>
          <div class="stat-box"><div class="stat-num">{round(float(a.get('max_score') or 0),1)}</div><div class="stat-label">Max</div></div>
          <div class="stat-box"><div class="stat-num">⭐{rating['avg_rating']}</div><div class="stat-label">{rating['review_count']} {'отзывов' if lang=='ru' else ('шарҳ' if lang=='uz_cyrl' else 'sharh')}</div></div>
        </div>
        <a class="btn" style="margin-top:14px;display:inline-block" href="/export-excel/{test_id}?token={token}">📥 Excel</a>
      </div>
      <div class="card">
        <h2>📊 {'Сложные вопросы' if lang=='ru' else ('Қийин саволлар' if lang=='uz_cyrl' else 'Qiyin savollar')}</h2>
        {hard_rows}
      </div>
    </div>
    """
    return html_page

@app.route("/create-visual-test", methods=["GET", "POST"])
def create_visual_test():
    if request.method == "POST":
        data = request.json or {}
        token = data.get("token") or request.args.get("token")
    else:
        token = request.args.get("token")

    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user:
        if request.method == "POST": return jsonify({"success": False, "error": "Xavfsizlik xatosi!"}), 401
        return abort(401)

    if not user.get("is_verified"):
        if request.method == "POST": return jsonify({"success": False, "error": "Avval Captchadan o'ting"}), 403
        return redirect("/captcha")

    user_id = int(user["user_id"])

    if request.method == "GET":
        chats = db.chats_for_user(user_id)
        eligible_chats = [c for c in chats if c.get('bot_is_admin', 0) == 1]
        return render_template("create_test.html", token=token, user_id=user_id, chats=eligible_chats, base_url=WEB_BASE_URL, current_user_bg=user.get("custom_bg"), current_lock_bg=user.get("custom_lock_bg"), lang=lang, get_text=get_text)

    title = data.get("title", "").strip()
    chat_id_raw = data.get("chat_id", "")
    deadline_str = data.get("deadline", "").strip()
    take_password = data.get("take_password", "").strip()
    manage_password = data.get("manage_password", "").strip()
    questions = data.get("questions", [])
    scoring_type = data.get("scoring_type", "standard")
    is_randomized = 1 if data.get("is_randomized") else 0
    try: time_limit = int(data.get("time_limit", 0))
    except: time_limit = 0

    try: attempts_limit = int(data.get("attempts_limit", "1"))
    except: attempts_limit = 1

    if not title or not chat_id_raw or not questions:
        return jsonify({"success": False, "error": get_text('fill_fields', lang)}), 400

    questions_text = " ".join([q.get("question", "") + " " + " ".join(q.get("options", [])) for q in questions])
    is_safe, reason = check_content_with_ai(title, questions_text)

    if not is_safe:
        return jsonify({"success": False, "error": f"❌ Taqiqlandi: {reason}"}), 400

    try: chat_id = int(chat_id_raw)
    except: return jsonify({"success": False, "error": get_text('invalid_chat', lang)}), 400

    deadline_ts = None
    if deadline_str:
        try:
            deadline_dt = datetime.strptime(deadline_str, "%d/%m/%Y %H:%M")
            deadline_ts = int(deadline_dt.replace(tzinfo=TZ).timestamp())
        except: return jsonify({"success": False, "error": get_text('invalid_deadline', lang)}), 400

    for idx, q in enumerate(questions):
        if not q.get("question", "").strip(): return jsonify({"success": False, "error": get_text('q_empty', lang, idx=idx+1)}), 400
        if len(q.get("options", [])) < 2: return jsonify({"success": False, "error": get_text('opts_empty', lang, idx=idx+1)}), 400
        if q.get("correct_index") is None or q.get("correct_index") < 0: return jsonify({"success": False, "error": get_text('correct_empty', lang, idx=idx+1)}), 400

    test_id = uuid.uuid4().hex[:10]
    try:
        current_time = int(time.time())
        take_pwd_hash = hashlib.sha256(take_password.encode()).hexdigest() if take_password else None

        db.create_test(
            test_id=test_id, owner_user_id=user_id, chat_id=chat_id, title=title,
            per_question_sec=60, created_at=current_time, password=take_pwd_hash,
            manage_password=manage_password, scoring_type=scoring_type,
            time_limit=time_limit, is_randomized=is_randomized
        )

        if deadline_ts is not None: db.set_test_deadline(test_id, deadline_ts)
        with db._conn() as c:
            c.execute("UPDATE tests SET attempts_limit=%s WHERE test_id=%s", (attempts_limit, test_id))
            c.execute("COMMIT")

        for i, q in enumerate(questions):
            q_score = float(q.get("score", 1.0))
            p_id = q.get("photo_id")
            if q.get("image_data"):
                new_photo = save_base64_image(q["image_data"])
                if new_photo: p_id = new_photo
            db.add_question(test_id, i, q["question"].strip(), q["options"], q["correct_index"], photo_id=p_id, score=q_score)

        if BOT_TOKENS:
            bot_username = get_bot_username()
            deep_link = f"https://t.me/{bot_username}?start=test_{test_id}"
            deadline_text = f"⏰ Deadline: {deadline_str}\n" if deadline_str else ""
            limit_txt = "Cheksiz" if attempts_limit == 0 else f"{attempts_limit} marta"
            time_txt = f"⏱ Vaqt: {time_limit} daqiqa" if time_limit > 0 else "⏱ Vaqt: Cheksiz"
            safe_title = html.escape(title)

            text = (
                f"🧩 <b>{safe_title}</b>\n\n🖊 Savollar: {len(questions)} ta\n"
                f"🔄 Urinishlar: {limit_txt}\n{time_txt}\n{deadline_text}\n"
                "Boshlash uchun pastdagi tugmani bosing:"
            ).strip()

            kb = {"inline_keyboard": [[{"text": "🔒 Private testni boshlash", "url": deep_link}]]}
            send_tg_msg(chat_id, text, reply_markup=kb)

        return jsonify({"success": True, "message": get_text('test_created', lang), "redirect_url": f"/?token={token}"})
    except Exception as e:
        return jsonify({"success": False, "error": get_text('internal_error', lang)}), 500

@app.route("/update-limit/<test_id>", methods=["POST"])
def update_limit(test_id):
    token = request.form.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user: return abort(401)
    test = to_dict(db.get_test(test_id))
    if not test: return abort(404)

    is_superadmin = int(user["user_id"]) in SUPERADMINS
    if int(test["owner_user_id"]) != int(user["user_id"]) and not is_superadmin: return abort(403)

    try: new_limit = int(request.form.get("attempts_limit", 1))
    except: new_limit = 1

    with db._conn() as c:
        c.execute("UPDATE tests SET attempts_limit=%s WHERE test_id=%s", (new_limit, test_id))
        c.execute("COMMIT")

    return redirect(f"/test/{test_id}?token={token}&msg={get_text('limit_updated', lang)}")

@app.route("/test/<test_id>", methods=["GET", "POST"])
def view_test(test_id):
    token = request.args.get("token") or request.form.get("token")
    user = validate_token(token) if token else None
    lang = session.get("lang", "uz")
    if not user: return abort(401)
    if not user.get("is_verified"): return redirect("/captcha")

    test = to_dict(db.get_test(test_id))
    if not test: return abort(404, get_text('test_not_found', lang))

    user_id = int(user["user_id"])
    is_superadmin = user_id in SUPERADMINS
    if user_id != int(test["owner_user_id"]) and not is_superadmin:
        return redirect(f"/solve/{test_id}?token={token}")

    raw_questions = db.get_questions(test_id)
    questions = []
    for rq in raw_questions:
        q_dict = to_dict(rq)
        try: q_dict["options"] = json.loads(q_dict.get("options_json", "[]"))
        except: q_dict["options"] = []
        questions.append(q_dict)

    results = [to_dict(r) for r in db.all_results(test_id)]
    stats = db.stats(test_id)
    chats = [c for c in db.chats_for_user(user_id)] if user else []

    return render_template(
        "test.html", test=test, questions=questions, results=results, stats=stats,
        token=token, chats=chats, base_url=WEB_BASE_URL,
        current_user_bg=user.get("custom_bg"), current_lock_bg=user.get("custom_lock_bg"),
        lang=lang, get_text=get_text
    )

@app.route("/publish-test/<test_id>", methods=["POST"])
def publish_test(test_id):
    token = request.form.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user: return abort(401)

    user_id = int(user["user_id"])
    test = to_dict(db.get_test(test_id))
    if not test or int(test["owner_user_id"]) != user_id: return abort(404)

    public_name = request.form.get("public_name", "").strip()
    password = request.form.get("password", "").strip()
    if password: password = hashlib.sha256(password.encode()).hexdigest()

    success = db.set_public_link(test_id, public_name, password)
    if not success: return redirect(f"/test/{test_id}?token={token}&msg=❌ Xato: {get_text('already_public', lang)}")
    return redirect(f"/test/{test_id}?token={token}&msg={get_text('test_edited', lang)}")

@app.route("/delete-test/<test_id>", methods=["POST"])
def delete_test(test_id):
    token = request.form.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user: return abort(401)

    user_id = int(user["user_id"])
    test = to_dict(db.get_test(test_id))
    if not test: return abort(404)

    is_superadmin = user_id in SUPERADMINS
    if int(test["owner_user_id"]) != user_id and not is_superadmin: return abort(404)

    if test.get("manage_password") and not is_superadmin:
        entered_password = request.form.get("manage_password", "").strip()
        if entered_password != test["manage_password"]:
            return redirect(f"/test/{test_id}?token={token}&msg={get_text('wrong_admin_pass', lang)}")

    success = db.delete_test(test_id, user_id)
    if not success: return abort(404)

    if is_superadmin and int(test["owner_user_id"]) != user_id:
        return redirect(f"/admin/tests?token={token}&msg={get_text('test_deleted_admin', lang)}")
    return redirect(f"/?token={token}&msg={get_text('test_deleted', lang)}")

@app.route("/api/bulk-delete-tests", methods=["POST"])
def bulk_delete_tests():
    data = request.json or {}
    token = data.get("token")
    test_ids = data.get("test_ids", [])
    user = validate_token(token)
    if not user: return jsonify({"success": False, "error": "Tizimga kirmagansiz!"}), 401

    user_id = int(user["user_id"])
    is_superadmin = user_id in SUPERADMINS
    try:
        for test_id in test_ids:
            test = to_dict(db.get_test(test_id))
            if test and (int(test["owner_user_id"]) == user_id or is_superadmin):
                db.delete_test(test_id, user_id)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": "O'chirishda xatolik yuz berdi"}), 500

@app.route("/share-test/<test_id>", methods=["POST"])
def share_test(test_id):
    token = request.form.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user: return abort(401)

    user_id = int(user["user_id"])
    chat_id = request.form.get("chat_id")
    test = to_dict(db.get_test(test_id))
    if not test: return abort(404)

    is_superadmin = user_id in SUPERADMINS
    if int(test["owner_user_id"]) != user_id and not is_superadmin: return abort(403)

    if test.get("manage_password") and not is_superadmin:
        entered_password = request.form.get("manage_password", "").strip()
        if entered_password != test["manage_password"]:
            return redirect(f"/test/{test_id}?token={token}&msg={get_text('wrong_admin_pass', lang)}")

    q_count, _ = db.stats(test_id)

    if BOT_TOKENS and chat_id:
        bot_username = get_bot_username()
        deep_link = f"https://t.me/{bot_username}?start=test_{test_id}"
        attempts_limit = test.get("attempts_limit", 1)
        limit_txt = "Cheksiz" if attempts_limit == 0 else f"{attempts_limit} marta"

        text = (
            f"🧩 <b>{test.get('title', 'Test')}</b>\n\n"
            f"🖊 Savollar: {q_count} ta\n"
            f"🔄 Urinishlar: {limit_txt}\n\n"
            "Boshlash uchun pastdagi tugmani bosing:"
        ).strip()
        kb = {"inline_keyboard": [[{"text": "🔒 Private testni boshlash", "url": deep_link}]]}
        success = send_tg_msg(int(chat_id), text, reply_markup=kb)
        msg = get_text('btn_sent', lang) if success else get_text('bot_cant_write', lang)
    else:
        msg = get_text('bot_not_connected', lang)

    return redirect(f"/test/{test_id}?token={token}&msg={msg}")

@app.route("/send-results/<test_id>", methods=["POST"])
def send_results(test_id):
    token = request.form.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user: return abort(401)

    user_id = int(user["user_id"])
    chat_id = request.form.get("chat_id")
    test = to_dict(db.get_test(test_id))
    if not test: return abort(404)

    is_superadmin = user_id in SUPERADMINS
    if test.get("manage_password") and not is_superadmin:
        entered_password = request.form.get("manage_password", "").strip()
        if entered_password != test["manage_password"]:
            return redirect(f"/test/{test_id}?token={token}&msg={get_text('wrong_admin_pass', lang)}")

    results = [to_dict(r) for r in db.all_results(test_id)]
    participants_count = len(results)
    title = test.get('title', 'Nomsiz Test')
    scoring_type = test.get('scoring_type', 'standard')

    msg_lines = []
    for i, r in enumerate(results, 1):
        name = r.get('username') or (r.get('first_name', '') + ' ' + r.get('last_name', '')).strip() or f"User{r.get('user_id', '')}"
        score_str = format_display_score(r.get('score', 0), scoring_type, lang)
        dur = r.get('duration_sec', 0)
        m = dur // 60
        s = dur % 60
        dur_str = f"{m} daq {s} soniya" if m > 0 else f"{s} soniya"

        medal = "🥇 👑 1-O'RIN " if i==1 else "🥈 🌟 2-O'RIN " if i==2 else "🥉 ✨ 3-O'RIN " if i==3 else f"🎗 {i}-o'rin "
        msg_lines.append(f"{medal} ➔ 👤 <b>{name}</b> (🎯 {score_str} | ⏱ {dur_str})")

    lines_str = "\n\n".join(msg_lines) if msg_lines else "🤷‍♂️ <i>Hali hech kim qatnashmadi.</i>"
    msg = (
        f"🏆 <b>TEST YAKUNLANDI!</b> 🏆\n━━━━━━━━━━━━━━━━━━━━━━\n📚 <b>Mavzu:</b> {title}\n"
        f"👥 <b>Qatnashchilar:</b> {participants_count} ta\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📊 <b>TOP NATIJALAR:</b>\n\n{lines_str}\n\n━━━━━━━━━━━━━━━━━━━━━━\n🤖 <i>Geo Ustoz - Bilimingizni sinang!</i>"
    )

    if BOT_TOKENS:
        success = send_tg_msg(int(chat_id), msg)
        send_status = get_text('results_sent', lang) if success else get_text('results_fail', lang)
    else:
        send_status = get_text('bot_not_connected', lang)

    return redirect(f"/test/{test_id}?token={token}&{send_status}")

@app.route("/search", methods=["GET"])
def search():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user: return abort(401)
    if not user.get("is_verified"): return redirect("/captcha")

    query = request.args.get("q", "").strip()
    raw_results = db.search_public_tests(query) if query else []
    results = [to_dict(r) for r in raw_results]

    return render_template(
        "search.html", token=token, query=query, results=results, base_url=WEB_BASE_URL,
        current_user_bg=user.get("custom_bg"), current_lock_bg=user.get("custom_lock_bg"), lang=lang, get_text=get_text
    )

@app.route("/solve/<test_id>", methods=["GET", "POST"])
def solve_test(test_id):
    try:
        if request.method == "POST":
            data = request.json or {}
            token = data.get("token") or request.args.get("token")
        else:
            token = request.args.get("token")

        user = validate_token(token)
        lang = session.get("lang", "uz")
        if not user:
            if request.method == "POST": return jsonify({"success": False, "error": get_text('login_first', lang)}), 401
            return abort(401, get_text('login_first', lang))

        if not user.get("is_verified"):
            if request.method == "POST": return jsonify({"success": False, "error": "Avval Captchadan o'ting"}), 403
            return redirect("/captcha")

        user_id = int(user["user_id"])
        test = to_dict(db.get_test(test_id))

        if not test:
            if request.method == "POST": return jsonify({"success": False, "error": get_text('test_not_found', lang)}), 404
            return abort(404, get_text('test_not_found', lang))

        is_owner = (int(test["owner_user_id"]) == user_id)
        is_admin = (user_id in SUPERADMINS)
        has_price = float(test.get("price_gwt") or 0) > 0 or int(test.get("price_stars") or 0) > 0

        purchased = False
        if has_price and not is_owner and not is_admin:
            with db._conn() as c:
                if c.execute("SELECT 1 FROM purchased_tests WHERE user_id=%s AND test_id=%s", (user_id, test_id)).fetchone():
                    purchased = True
            if not purchased:
                return render_template("solve_test.html", token=token, test=test, require_purchase=True, current_user_bg=user.get("custom_bg"), current_lock_bg=user.get("custom_lock_bg"), lang=lang, get_text=get_text)

        if test.get("status") == "closed":
            if request.method == "POST": return jsonify({"success": False, "error": get_text('test_closed', lang)})
            return render_template("solve_test.html", token=token, test=test, error_msg=get_text('test_closed', lang), current_user_bg=user.get("custom_bg"), current_lock_bg=user.get("custom_lock_bg"), lang=lang, get_text=get_text)

        attempts_limit = test.get("attempts_limit", 1)
        with db._conn() as c:
            c.execute("SELECT COUNT(*) as cnt FROM sessions WHERE test_id=%s AND user_id=%s AND state='finished'", (test_id, user_id))
            fc = to_dict(c.fetchone())
            finished_count = fc.get("cnt", 0) if fc else 0
            c.execute("SELECT state, score, duration_sec FROM sessions WHERE test_id=%s AND user_id=%s ORDER BY started_at DESC LIMIT 1", (test_id, user_id))
            already = to_dict(c.fetchone())

        if attempts_limit > 0 and finished_count >= attempts_limit:
            if request.method == "POST": return jsonify({"success": False, "error": get_text('limit_reached', lang, limit=attempts_limit)})
            return render_template("solve_test.html", token=token, test=test, already_solved=True, score=already.get("score", 0) if already else 0, duration=already.get("duration_sec", 0) if already else 0, current_user_bg=user.get("custom_bg"), current_lock_bg=user.get("custom_lock_bg"), lang=lang, get_text=get_text)

        if already and already.get("state") != "running" and attempts_limit > 0 and finished_count >= attempts_limit:
             return render_template("solve_test.html", token=token, test=test, already_solved=True, score=already.get("score", 0), duration=already.get("duration_sec", 0), current_user_bg=user.get("custom_bg"), current_lock_bg=user.get("custom_lock_bg"), lang=lang, get_text=get_text)

        if request.method == "GET":
            pwd_entered = request.args.get("pwd", "")
            if test.get("password") and not (has_price and purchased):
                if not pwd_entered or hashlib.sha256(pwd_entered.encode()).hexdigest() != test["password"]:
                    return render_template("solve_test.html", token=token, test=test, require_password=True, pwd_error=bool(pwd_entered), current_user_bg=user.get("custom_bg"), current_lock_bg=user.get("custom_lock_bg"), lang=lang, get_text=get_text)

            raw_questions = db.get_questions(test_id)
            questions = []
            for rq in raw_questions:
                q_dict = to_dict(rq)
                try: q_dict["options"] = json.loads(q_dict.get("options_json", "[]"))
                except: q_dict["options"] = []
                questions.append(q_dict)
            return render_template("solve_test.html", token=token, test=test, questions=questions, pwd=pwd_entered, current_user_bg=user.get("custom_bg"), current_lock_bg=user.get("custom_lock_bg"), lang=lang, get_text=get_text)

        if request.method == "POST":
            answers = data.get("answers", {})
            duration = int(data.get("duration_sec", 0))
            qs = db.get_questions(test_id)
            correct_count, wrong_count, earned_score, max_score = 0, 0, 0.0, 0.0
            session_id = uuid.uuid4().hex
            now = int(time.time())
            started_at = now - duration

            db.create_session(session_id, test_id, user_id, started_at)

            for i, q in enumerate(qs):
                q_idx_str = str(i)
                q_weight = float(q.get("question_score") or 1.0)
                max_score += q_weight

                if q_idx_str in answers:
                    opt_idx = int(answers[q_idx_str])
                    is_correct = 1 if opt_idx == int(q["correct_index"]) else 0
                    if is_correct:
                        correct_count += 1
                        earned_score += q_weight
                    else:
                        wrong_count += 1
                    db.upsert_answer(session_id, i, opt_idx, is_correct, now, 0)
                else:
                    wrong_count += 1

            scoring_type = test.get("scoring_type", "standard")
            if scoring_type == "percentage":
                final_score = round((earned_score / max_score) * 100, 1) if max_score > 0 else 0
            elif scoring_type == "minus":
                final_score = earned_score - (wrong_count * 0.5)
                if final_score < 0: final_score = 0
            else:
                final_score = earned_score

            db.finish_session(session_id, now, final_score, duration)

            # 🎮 Gamification post-processing (XP, streak, yutuqlar, statistika)
            is_owner = int(test.get("owner_user_id", 0)) == int(user_id)
            gamify = db.process_test_completion(
                user_id=user_id, test_id=test_id, session_id=session_id,
                correct=correct_count, total=len(qs), score=final_score, is_owner=is_owner
            )

            # Yangi yutuqlar uchun bildirishnoma yaratish
            try:
                for ach_code in gamify.get("new_achievements", []):
                    db.add_notification(user_id, "🏅 Yangi yutuq!", f"Siz yangi yutuqqa erishdingiz: {ach_code}", "achievement")
                if gamify.get("new_level"):
                    db.add_notification(user_id, "⭐ Yangi daraja!", f"Tabriklaymiz! Siz {gamify['new_level']}-darajaga yetdingiz!", "level")
            except Exception:
                pass

            return jsonify({
                "success": True, "score": final_score, "correct_answers": correct_count,
                "total": len(qs), "duration": duration, "type": scoring_type,
                "xp_gained": gamify.get("xp_gained", 0),
                "new_level": gamify.get("new_level"),
                "new_achievements": gamify.get("new_achievements", []),
                "streak": gamify.get("streak", 0)
            })

    except Exception as e:
        if request.method == "POST": return jsonify({"success": False, "error": get_text('server_error', session.get("lang", "uz"))}), 500
        return abort(500, get_text('server_error', session.get("lang", "uz")))
    return jsonify({"success": False, "error": "Bad Request"}), 400

@app.route("/app-solve/<test_id>")
def app_solve_loader(test_id):
    lang = session.get("lang", "uz")
    return render_template("app_solve_loader.html", test_id=test_id, lang=lang, get_text=get_text)

@app.route("/ai-chat")
def web_ai_chat():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user: return abort(401, get_text('login_first', lang))
    if not user.get("is_verified"): return redirect("/captcha")
    return render_template("ai_chat.html", token=token, current_user_bg=user.get("custom_bg"), current_lock_bg=user.get("custom_lock_bg"), lang=lang, get_text=get_text)

@app.route("/api/ai-chat/sessions")
def api_ai_sessions():
    user = validate_token(request.args.get("token"))
    if not user: return jsonify({"error": "Unauthorized"}), 401
    with db._conn() as c:
        c.execute("""
            SELECT session_id, MIN(created_at) as created_at, content as title
            FROM ai_chat_history WHERE user_id=%s AND role='user'
            GROUP BY session_id ORDER BY created_at DESC
        """, (user["user_id"],))
        rows = c.fetchall()
    return jsonify([to_dict(r) for r in rows])

@app.route("/api/ai-chat/history")
def api_ai_history():
    user = validate_token(request.args.get("token"))
    session_id = request.args.get("session_id", "default_session")
    if not user: return jsonify({"error": "Unauthorized"}), 401
    with db._conn() as c:
        c.execute("SELECT id, role, content, session_id FROM ai_chat_history WHERE user_id=%s AND session_id=%s ORDER BY created_at ASC", (user["user_id"], session_id))
        rows = c.fetchall()
    return jsonify([to_dict(r) for r in rows])

@app.route("/api/ai-chat/edit", methods=["POST"])
def api_ai_edit():
    data = request.json
    user = validate_token(data.get("token"))
    if not user: return jsonify({"error": "Unauthorized"}), 401

    msg_id, session_id, action = data.get("msg_id"), data.get("session_id"), data.get("action")
    new_text = data.get("content", "").strip()

    with db._conn() as c:
        if action == "delete":
            c.execute("DELETE FROM ai_chat_history WHERE id=%s AND user_id=%s", (msg_id, user["user_id"]))
        elif action == "edit" and new_text:
            c.execute("UPDATE ai_chat_history SET content=%s WHERE id=%s AND user_id=%s", (new_text, msg_id, user["user_id"]))
        elif action == "delete_session" and session_id:
            c.execute("DELETE FROM ai_chat_history WHERE session_id=%s AND user_id=%s", (session_id, user["user_id"]))
        c.execute("COMMIT")
    return jsonify({"success": True})

@app.route("/api/ai-chat", methods=["POST"])
def api_ai_chat():
    data = request.json or {}
    token, text, session_id = data.get("token"), data.get("text", "").strip(), data.get("session_id")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user: return jsonify({"error": get_text('login_first', lang)}), 401
    user_id = int(user["user_id"])
    if not session_id: session_id = uuid.uuid4().hex

    user_row = db.get_user(user_id)
    is_premium = user_row and dict(user_row).get("status") == "premium"
    is_any_admin = user_id in SUPERADMINS or user_id in LOWER_ADMINS
    today_str = datetime.now(tz=TZ).strftime("%Y-%m-%d")

    if not is_premium and not is_any_admin:
        usage = db.get_ai_usage(user_id, today_str)
        if usage >= 10: return jsonify({"error": get_text('ai_limit_reached', lang)}), 403
        db.increment_ai_usage(user_id, today_str)

    now = int(time.time())
    with db._conn() as c:
        c.execute("INSERT INTO ai_chat_history (user_id, session_id, role, content, created_at) VALUES (%s, %s, 'user', %s, %s)", (user_id, session_id, text, now))
        c.execute("COMMIT")
        c.execute("SELECT role, content FROM ai_chat_history WHERE user_id=%s AND session_id=%s ORDER BY created_at ASC LIMIT 15", (user_id, session_id))
        old_msgs = c.fetchall()

    system_prompt = """Sen 'Testchi' ta'lim platformasining aqlli sun'iy intellekt yordamchisisan.
Senga qo'yilgan quyidagi qoidalarga QAT'IY va SO'ZSIZ amal qilishing SHART. Tizim to'g'ri ishlashi uchun bu hayot-mamot masalasi.

🔴 MAXSUS BUYRUQLAR QOIDASI (ENG MUHIMI):
Agar savolga javob topish uchun internet qidiruv talab etilsa, sening javobing FAQAT VA FAQAT bitta qator buyruqdan iborat bo'lishi shart!

1. INTERNET QIDIRUVI:
Agar foydalanuvchi eng so'nggi yangilik, fakt yoki ma'lumot so'rasa, javob o'rniga FAQAT shuni yoz:
/interdan_qidirish [qidiriladigan matn]"""

    messages = [{"role": "system", "content": system_prompt}]
    for msg in old_msgs:
        msg_dict = to_dict(msg)
        messages.append({"role": msg_dict["role"], "content": msg_dict["content"]})

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": WEB_BASE_URL,
        "X-Title": "Testchi"
    }
    payload = {"model": MODEL_NAME, "messages": messages, "temperature": 0.1, "max_tokens": 10240}

    def generate():
        try:
            yield json.dumps({"status": "Matn tahlil qilinmoqda..."}) + "\n"
            api_response = requests.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=payload, timeout=15)
            res_data = api_response.json()

            if "choices" in res_data and len(res_data["choices"]) > 0:
                ai_initial_reply = res_data["choices"][0]["message"]["content"].strip()
                final_reply = ai_initial_reply

                if "/interdan_qidirish" in ai_initial_reply:
                    query_part = ai_initial_reply.split("/interdan_qidirish")[-1].split('\n')[0].strip()
                    query_part = query_part.replace('"', '').replace("'", "")
                    yield json.dumps({"status": f"Internetdan izlanmoqda: '{query_part}'"}) + "\n"

                    try:
                        from duckduckgo_search import DDGS
                        with DDGS() as ddgs:
                            results = list(ddgs.text(query_part, region='wt-wt', safesearch='moderate', max_results=3))
                        if results:
                            info = "\n\n".join([f"📌 Maqola: {r['title']}\nMatn: {r['body']}" for r in results])
                            search_results = f"Tizim xabari: '{query_part}' bo'yicha internetdan quyidagi eng yangi ma'lumotlar topildi:\n{info}"
                        else:
                            search_results = f"Tizim xabari: '{query_part}' bo'yicha internetdan hech narsa topilmadi."
                    except Exception as e:
                        search_results = f"Tizim xabari: Qidiruvda xato yuz berdi ({e}). O'zing bilgan ma'lumotlar asosida javob ber."

                    messages.append({"role": "assistant", "content": ai_initial_reply})
                    messages.append({"role": "user", "content": search_results + "\n\nYuqoridagi haqiqiy ma'lumotlardan foydalanib menga aniq va chiroyli javob yoz. Manbani ham qisqacha aytib o't:"})
                    payload["messages"] = messages

                    yield json.dumps({"status": "Topilgan ma'lumotlar tahlil qilinib, javob yozilmoqda..."}) + "\n"
                    resp2 = requests.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=payload, timeout=20).json()
                    final_reply = resp2["choices"][0]["message"]["content"]

                else:
                    yield json.dumps({"status": "Javob tayyorlanmoqda..."}) + "\n"

                import re
                reply_formatted = final_reply
                reply_formatted = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', reply_formatted)
                reply_formatted = re.sub(r'\*(.*?)\*', r'<i>\1</i>', reply_formatted)

                with db._conn() as c:
                    c.execute("INSERT INTO ai_chat_history (user_id, session_id, role, content, created_at) VALUES (%s, %s, 'assistant', %s, %s)", (user_id, session_id, reply_formatted, int(time.time())))
                    c.execute("COMMIT")
                    c.execute("SELECT id FROM ai_chat_history WHERE user_id=%s ORDER BY id DESC LIMIT 1", (user_id,))
                    row = to_dict(c.fetchone())
                    ai_msg_id = row.get("id", int(time.time())) if row else int(time.time())

                yield json.dumps({"reply": reply_formatted, "id": ai_msg_id, "session_id": session_id}) + "\n"
            else:
                real_error = res_data.get("error", {}).get("message", "Noma'lum API xatosi")
                yield json.dumps({"error": f"OpenRouter: {real_error}"}) + "\n"

        except requests.exceptions.RequestException as e:
            yield json.dumps({"error": f"Tarmoq xatosi: {str(e)}"}) + "\n"
        except Exception as e:
            yield json.dumps({"error": f"Ichki xato: {str(e)}"}) + "\n"

    return Response(stream_with_context(generate()), mimetype='application/x-ndjson')

@app.route("/admin/ai-chats")
def admin_ai_chats():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user or int(user["user_id"]) not in SUPERADMINS: return abort(403)
    if not user.get("is_verified"): return redirect("/captcha")

    with db._conn() as c:
        c.execute("""
            SELECT u.user_id, u.first_name, u.username, COUNT(DISTINCT a.session_id) as session_count, MAX(a.created_at) as last_msg
            FROM users u JOIN ai_chat_history a ON u.user_id = a.user_id
            GROUP BY u.user_id ORDER BY last_msg DESC
        """)
        users = c.fetchall()

    user_list = [to_dict(u) for u in users]
    for u in user_list:
        u['last_msg_str'] = datetime.fromtimestamp(u.get('last_msg', 0), tz=TZ).strftime("%d.%m.%Y %H:%M")

    return render_template("admin_ai_chats.html", users=user_list, token=token, current_user_bg=user.get("custom_bg"), current_lock_bg=user.get("custom_lock_bg"), lang=lang, get_text=get_text)

@app.route("/admin/ai-chat/<int:target_id>")
def admin_ai_chat_detail(target_id):
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user or int(user["user_id"]) not in SUPERADMINS: return abort(403)

    target_user = to_dict(db.get_user(target_id))
    with db._conn() as c:
        c.execute("SELECT * FROM ai_chat_history WHERE user_id=%s ORDER BY created_at ASC", (target_id,))
        msgs = c.fetchall()

    sessions = {}
    for m in msgs:
        md = to_dict(m)
        sid = md.get("session_id", "default_session")
        if sid not in sessions: sessions[sid] = []
        sessions[sid].append(md)

    return render_template("admin_ai_chat_detail.html", sessions=sessions, target_user=target_user, token=token, current_user_bg=user.get("custom_bg"), current_lock_bg=user.get("custom_lock_bg"), lang=lang, get_text=get_text)

@app.route("/captcha")
def captcha_page():
    return render_template("captcha.html")

@app.route("/api/verify-captcha", methods=["POST"])
def api_verify_captcha():
    data = request.json
    init_data = data.get("initData", "")
    captcha_token = data.get("captcha_token", "")

    if not captcha_token: return jsonify({"success": False, "error": "Captcha belgilanmagan!"}), 400

    verify_url = "https://www.google.com/recaptcha/api/siteverify"
    payload = {"secret": RECAPTCHA_SECRET_KEY, "response": captcha_token}
    recaptcha_res = requests.post(verify_url, data=payload).json()

    if not recaptcha_res.get("success"): return jsonify({"success": False, "error": "Google Captcha xatosi."}), 403

    is_valid, tg_user = verify_telegram_webapp_data(init_data)
    if not is_valid or not tg_user: return jsonify({"success": False, "error": "Yaroqsiz Telegram ma'lumotlari"}), 403

    user_id = tg_user.get("id")
    ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    if ip: ip = ip.split(',')[0].strip()

    with db._conn() as c:
        c.execute("UPDATE users SET is_verified=1 WHERE user_id=%s", (user_id,))
        if ip: c.execute("DELETE FROM blacklisted_ips WHERE ip=%s", (ip,))
        c.execute("COMMIT")

    try: send_tg_msg(user_id, "✅ <b>Bot emasligingiz tasdiqlandi!</b>\n\nDavom etish uchun /start ni bosing.")
    except Exception as e: logging.error(f"Kapcha tasdiqlangach xabar yuborishda xato: {e}")

    return jsonify({"success": True})

@app.route("/edit-test/<test_id>", methods=["GET", "POST"])
def edit_test(test_id):
    token = request.args.get("token")
    if not token and request.is_json: token = request.json.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user: return abort(401)
    user_id = int(user["user_id"])
    test = to_dict(db.get_test(test_id))

    if not test or (int(test["owner_user_id"]) != user_id and user_id not in SUPERADMINS): return abort(403)

    if request.method == "GET":
        questions = [to_dict(q) for q in db.get_questions(test_id)]
        for q in questions: q["options"] = json.loads(q.get("options_json", "[]"))
        chats = db.chats_for_user(user_id)
        return render_template("create_test.html", token=token, test=test, user_id=user_id, questions=questions, chats=chats, is_edit=True, base_url=WEB_BASE_URL, lang=lang, get_text=get_text)

    if request.method == "POST":
        try:
            data = request.json
            questions_payload = data.get("questions", [])

            questions_text = " ".join([q.get("question", "") + " " + " ".join(q.get("options", [])) for q in questions_payload])
            is_safe, reason = check_content_with_ai(data.get("title", ""), questions_text)

            if not is_safe: return jsonify({"success": False, "error": f"❌ Taqiqlandi: {reason}"}), 400

            for q in questions_payload:
                if q.get("image_data"):
                    new_photo = save_base64_image(q["image_data"])
                    if new_photo: q["photo_id"] = new_photo

            try: time_limit = int(data.get("time_limit", 0))
            except: time_limit = 0

            is_randomized = 1 if data.get("is_randomized") else 0
            scoring_type = data.get("scoring_type", "standard")

            success = db.update_test_full(test_id=test_id, title=data.get("title"), time_limit=time_limit, scoring_type=scoring_type, is_randomized=is_randomized, questions=questions_payload)

            if success:
                try: chat_id = int(data.get("chat_id"))
                except: chat_id = user_id

                try: attempts_limit = int(data.get("attempts_limit", 1))
                except: attempts_limit = 1

                deadline_str, take_password, manage_password = data.get("deadline", "").strip(), data.get("take_password", "").strip(), data.get("manage_password", "").strip()
                take_pwd_hash = hashlib.sha256(take_password.encode()).hexdigest() if take_password else None

                deadline_ts = None
                if deadline_str:
                    try:
                        deadline_dt = datetime.strptime(deadline_str, "%d/%m/%Y %H:%M")
                        deadline_ts = int(deadline_dt.replace(tzinfo=TZ).timestamp())
                    except: pass

                with db._conn() as c:
                    c.execute("UPDATE tests SET chat_id=%s, attempts_limit=%s, password=%s, manage_password=%s, deadline_ts=%s WHERE test_id=%s", (chat_id, attempts_limit, take_pwd_hash, manage_password, deadline_ts, test_id))
                    c.execute("COMMIT")

                if BOT_TOKENS and chat_id:
                    bot_username = get_bot_username()
                    deep_link = f"https://t.me/{bot_username}?start=test_{test_id}"
                    deadline_text = f"⏰ Deadline: {deadline_str}\n" if deadline_str else ""
                    limit_txt = "Cheksiz" if attempts_limit == 0 else f"{attempts_limit} marta"
                    time_txt = f"⏱ Vaqt: {time_limit} daqiqa" if time_limit > 0 else "⏱ Vaqt: Cheksiz"
                    safe_title = html.escape(data.get("title", ""))

                    text = (
                        f"🧩 <b>{safe_title}</b> (Tahrirlandi)\n\n"
                        f"🖊 Savollar: {len(questions_payload)} ta\n"
                        f"🔄 Urinishlar: {limit_txt}\n{time_txt}\n{deadline_text}\n"
                        "Boshlash uchun pastdagi tugmani bosing:"
                    ).strip()

                    kb = {"inline_keyboard": [[{"text": "🔒 Private testni boshlash", "url": deep_link}]]}
                    send_tg_msg(chat_id, text, reply_markup=kb)

                return jsonify({"success": True})
            return jsonify({"success": False, "error": "Baza qabul qilmadi"})
        except Exception as e:
            return jsonify({"success": False, "error": f"Server xatosi: {str(e)}"}), 500

@app.route("/api/cheat-warning", methods=["POST"])
def cheat_warning():
    data = request.json
    token = data.get("token")
    test_id = data.get("test_id")

    user = validate_token(token)
    if not user: return jsonify({"error": "Unauthorized"}), 401

    db.log_cheat_attempt(user["user_id"], test_id, int(time.time()))
    return jsonify({"success": True})

@app.route("/api/leaderboard")
def api_leaderboard():
    token = request.args.get("token")
    user = validate_token(token)
    if not user: return jsonify({"success": False, "error": "Unauthorized"}), 401
    try:
        top_100, user_info = db.get_current_month_leaderboard(user["user_id"])
        return jsonify({"success": True, "top": top_100, "user": user_info})
    except Exception as e:
        return jsonify({"success": False, "error": "Server xatosi"}), 500

def check_content_with_ai(title, questions_text):
    if not title and not questions_text: return True, "Hammasi joyida"
    if not GROQ_API_KEY: return True, "AI o'chirilgan"

    prompt = f"""Siz qattiqqo'l kontent moderatorsiz. Berilgan test matnida qonunga xilof harakatlar, qimor, pornografiya, terrorizm targ'iboti, 1xbet yoki ochiq so'kinishlar bor-yo'qligini tekshiring.
Faqat JSON formatida javob bering.
Maxsus format: {{"safe": true, "reason": "Hammasi joyida"}} yoki {{"safe": false, "reason": "Nima uchun taqiqlangani haqida qisqacha sabab"}}

Test sarlavhasi: {title}
Savollar matni: {questions_text}"""
    try:
        completion = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "system", "content": prompt}],
            temperature=0, response_format={"type": "json_object"}, timeout=8.0
        )
        res = json.loads(completion.choices[0].message.content)
        return res.get("safe", True), res.get("reason", "Hammasi joyida")
    except Exception as e:
        logging.error(f"Moderatsiya xatosi: {e}")
        return True, "Tekshiruv o'tkazib yuborildi"

if __name__ == "__main__":
    print("🚀 Web server ishga tushmoqda...")
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))