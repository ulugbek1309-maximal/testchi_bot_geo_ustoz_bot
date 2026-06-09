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
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, request, render_template, render_template_string, abort, jsonify, redirect, send_from_directory, session, Response, stream_with_context
from concurrent.futures import ThreadPoolExecutor

# Kriptografiya kutubxonalari
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.asymmetric import ed25519

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
MODEL_NAME = "google/gemini-2.5-flash:free"

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
        "time_sec": "soniya",
        "btn_account": "Hisob",
        "btn_guide": "Yo'riqnoma",
        "btn_search": "Qidiruv",
        "btn_premium": "Premium",
        "btn_ai": "AI Yordamchi",
        "btn_market": "Bozor",
        "back_to_dashboard": "Dashboard'ga qaytish",
        "already_solved_title": "✅ Siz bu testni allaqachon yechgansiz!",
        "already_solved_sub": "Bu testni qayta ishlash limiti tugagan.",
        "time_spent": "Sarflangan vaqt",
        "seconds": "soniya",
        "view_results": "Umumiy natijalarni ko'rish",
        "stars_payment_info": "Stars orqali to'lov botdan amalga oshiriladi",
        "test_not_for_stars": "Bu test Stars orqali sotilmaydi",
        "own_test": "O'zingizning testingiz!"
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

def safe_get_setting(key, default=None):
    """db.get_setting xavfsiz wrapper"""
    try:
        if hasattr(db, 'get_setting'):
            return db.get_setting(key, default)
    except Exception:
        pass
    return default

def safe_db(method_name, *args, default=None, **kwargs):
    """
    Istalgan DB metodini xavfsiz chaqirish.
    Metod yo'q bo'lsa yoki xato bo'lsa default qaytaradi.
    Ishlatish: safe_db('create_coupon', code) yoki
               safe_db('get_coupon', code, default=(None,'Topilmadi'))
    """
    try:
        method = getattr(db, method_name, None)
        if method is None:
            return default
        return method(*args, **kwargs)
    except Exception as e:
        logging.error(f"safe_db({method_name}) xato: {e}")
        return default

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

        m_theme = themere.match(line)
        if m_theme and theme == "Word Test":
            theme = m_theme.group(1).strip()
            continue

        m_q = qre.match(line)
        if m_q:
            if current_q: questions.append(current_q)
            clean_q = q_re.sub("", line).strip()
            current_q = {"question": clean_q, "options": [], "correct_index": -1}
            continue

        m_opt = optre.match(line)
        if m_opt and current_q:
            clean_opt = opt_re.sub("", line).strip()
            current_q["options"].append(clean_opt)
            continue

        m_true = truere.match(line)
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
    # pin_lock va captcha sahifalariga redirect loop oldini olish
    skip_pin_paths = ['/', '/pin-lock', '/captcha', '/force-sub', '/telegram-login']
    if request.path in skip_pin_paths:
        return  # Bu sahifalarda PIN tekshirmaymiz

    if not session.get(f"pin_unlocked_{token}"):
        # Barcha sahifalar uchun next_url saqlash (faqat /solve emas)
        if not request.path.startswith('/api/'):
            session[f"next_url_{token}"] = request.path
        if request.path.startswith('/api/'):
            return jsonify({"error": "PIN Lock", "redirect": f"/pin-lock?token={token}"}), 403
        return redirect(f"/pin-lock?token={token}")

    user_id = user["user_id"]
    if int(user_id) in SUPERADMINS: return

    sub_cache_key = f"sub_{user_id}"
    last_check = session.get(sub_cache_key, 0)

    if now - last_check > 300:
        try:
            is_subbed = check_user_subscription(user_id)
        except Exception as e:
            logging.error(f"Obuna tekshirishda xato: {e}")
            is_subbed = True  # Xato bo'lsa o'tkazib yuboramiz

        if not is_subbed:
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
    return render_template("telegram_login.html")

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
                    logging.error(f"PIN/email saqlashda xato: {e}")
                    return jsonify({"success": False, "error": "Bazaga saqlashda xato."})
            c.execute("COMMIT")
            session[f"pin_unlocked_{token}"] = True
            next_url = session.pop(f"next_url_{token}", None)
            return jsonify({"success": True, "next_url": next_url})

        elif action == "verify":
            if not current_pin:
                session[f"pin_unlocked_{token}"] = True
                next_url = session.pop(f"next_url_{token}", None)
                return jsonify({"success": True, "next_url": next_url})
            hashed_pin = hashlib.sha256(pin.encode()).hexdigest()
            if hashed_pin == current_pin:
                db.update_pin_attempts(user["user_id"], reset=True)
                session[f"pin_unlocked_{token}"] = True
                next_url = session.pop(f"next_url_{token}", None)
                return jsonify({"success": True, "next_url": next_url})
            else:
                db.update_pin_attempts(user["user_id"])
                return jsonify({"success": False, "error": "Wrong PIN!"})

        elif action == "biometric_unlock":
            session[f"pin_unlocked_{token}"] = True
            next_url = session.pop(f"next_url_{token}", None)
            return jsonify({"success": True, "next_url": next_url})

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
            logging.error(f"Fon rasm yuklashda xato: {e}")
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
    except Exception as e:
        logging.error(f"Balance xatosi: {e}")
        balance = 0.0

    try:
        raw_tests = db.tests_for_owner(user_id)
        tests = []
        for t in raw_tests:
            t_dict = to_dict(t)
            t_dict['price_gwt'] = float(t_dict.get('price_gwt') or 0.0)
            t_dict['price_stars'] = int(t_dict.get('price_stars') or 0)
            q_count, finished_count = db.stats(t_dict["test_id"])
            tests.append({"test": t_dict, "q_count": q_count, "finished_count": finished_count})
    except Exception as e:
        logging.error(f"Tests yuklashda xato: {e}")
        tests = []

    try:
        chats = db.chats_for_user(user_id)
        eligible_chats = [c for c in chats if c.get('bot_is_admin', 0) == 1]
    except Exception as e:
        logging.error(f"Chats yuklashda xato: {e}")
        eligible_chats = []

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
        get_text=get_text,
        user_status=user.get("status", "free"),
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
        logging.error(f"Test yaratishda xato: {e}")
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
    <head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Testlar Bozori</title>
    
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
            <div style="padding:10px; background:rgba(255,255,255,0.1); margin-bottom:15px; color:#38d39f; border-radius:8px;">{{ request.args.get('msg') }}</div>
        {% endif %}
        {% for t in tests %}
        <div class="card">
            <h3 style="margin-top:0;">🧩 {{ t.title }}</h3>
            <p style="color:gray; font-size:13px;">Muallif: {{ t.first_name }} {{ t.last_name }}</p>
            {% if t.is_owner or t.is_purchased %}
                <p style="color:#38d39f;">✅ Sizda bu test bor.</p>
                <a href="/solve/{{ t.test_id }}?token={{ token }}" class="btn">Testni ishlash</a>
            {% else %}
                <div style="display: flex; gap: 10px; flex-wrap: wrap;">
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
        <p style="text-align:center; color:#64748b; padding: 40px 0;">🛒 Hozircha bozorda pullik testlar yo'q.</p>
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

@app.route("/buy-test/stars/<test_id>", methods=["POST"])
def buy_test_stars(test_id):
    """Telegram Stars orqali test sotib olish (Tez kunda to'liq ulanganda ishga tushadi)"""
    token = request.form.get("token")
    user = validate_token(token)
    if not user: return abort(401)
    lang = session.get("lang", "uz")

    test = to_dict(db.get_test(test_id))
    if not test:
        return redirect(f"/market?token={token}&msg=❌ Test topilmadi.")

    price_stars = int(test.get("price_stars") or 0)
    if price_stars <= 0:
        return redirect(f"/market?token={token}&msg=❌ Bu test Stars orqali sotilmaydi.")

    user_id = int(user["user_id"])
    if int(test["owner_user_id"]) == user_id:
        return redirect(f"/market?token={token}&msg=❌ O'zingizning testingiz!")

    # Telegram Stars to'lov oqimi hali to'liq ulanmagan —
    # Botdagi /wallet bo'limidan to'lov amalga oshiriladi
    bot_username = get_bot_username()
    msg = (
        f"⭐️ Stars orqali to'lov qilish uchun botga o'ting va "
        f"'Hamyon' bo'limidan {price_stars} Stars to'lang."
    )
    return redirect(f"/market?token={token}&msg={msg}")

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


# ══════════════════════════════════════════════════════════════
# ⭐  TELEGRAM STARS — PREMIUM SOTIB OLISH (SAYT ORQALI)
# ══════════════════════════════════════════════════════════════

# Narxlar jadvali: oylar → stars miqdori (botdagi bilan bir xil)
PREMIUM_STARS_PRICES = {1: 80, 3: 220, 6: 440, 12: 800}


@app.route("/api/create-stars-invoice", methods=["POST"])
def api_create_stars_invoice():
    """
    Bot orqali Telegram Stars invoice link yaratadi va uni saytga qaytaradi.
    Frontend Telegram.WebApp.openInvoice(link) bilan ochadi.
    """
    data = request.json or {}
    token = data.get("token")
    user = validate_token(token)
    if not user:
        return jsonify({"ok": False, "error": "Tizimga kiring"}), 401

    months = int(data.get("months", 1))
    if months not in PREMIUM_STARS_PRICES:
        return jsonify({"ok": False, "error": "Noto'g'ri ta'rif"}), 400

    stars = PREMIUM_STARS_PRICES[months]
    lang  = session.get("lang", "uz")

    titles = {
        "uz":      f"💎 Premium — {months} oylik",
        "uz_cyrl": f"💎 Премиум — {months} ойлик",
        "ru":      f"💎 Premium — {months} мес.",
    }
    descs = {
        "uz":      f"Geo Ustoz testchi platformasida {months} oylik Premium maqomini xarid qiling.",
        "uz_cyrl": f"Geo Ustoz testchi platformasida {months} ойлик Премиум мақомини харид қилинг.",
        "ru":      f"Купите {months}-месячный Premium статус на платформе Geo Ustoz.",
    }

    title       = titles.get(lang, titles["uz"])
    description = descs.get(lang, descs["uz"])
    payload     = f"premium_{months}"

    if not BOT_TOKEN_MAIN:
        return jsonify({"ok": False, "error": "Bot token sozlanmagan"}), 500

    # Telegram Bot API → createInvoiceLink
    url = f"https://api.telegram.org/bot{BOT_TOKEN_MAIN}/createInvoiceLink"
    body = {
        "title":          title,
        "description":    description,
        "payload":        payload,
        "provider_token": "",          # Stars uchun bo'sh
        "currency":       "XTR",
        "prices":         [{"label": title, "amount": stars}],
    }
    try:
        resp = requests.post(url, json=body, timeout=10).json()
    except Exception as e:
        logging.error(f"createInvoiceLink xato: {e}")
        return jsonify({"ok": False, "error": "Telegram API bilan aloqa xatosi"}), 502

    if not resp.get("ok"):
        err = resp.get("description", "Noma'lum xato")
        logging.error(f"createInvoiceLink: {err}")
        return jsonify({"ok": False, "error": err}), 502

    invoice_link = resp["result"]
    return jsonify({"ok": True, "invoice_link": invoice_link, "stars": stars, "months": months})


@app.route("/api/stars-premium-verify", methods=["POST"])
def api_stars_premium_verify():
    """
    WebApp to'lov muvaffaqiyatli bo'lgandan so'ng (invoiceClosed status='paid')
    chaqiriladi. Bot already handles successful_payment va premium qo'shadi,
    bu endpoint faqat real-time UI update uchun premium statusni tekshiradi.
    """
    data = request.json or {}
    token = data.get("token")
    user = validate_token(token)
    if not user:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    user_id = user["user_id"]
    # DB dan yangilangan statusni o'qiymiz
    with db._conn() as c:
        c.execute("SELECT status, premium_expire_at FROM users WHERE user_id=%s", (user_id,))
        row = to_dict(c.fetchone())

    if not row:
        return jsonify({"ok": False, "error": "Foydalanuvchi topilmadi"}), 404

    is_premium      = row.get("status") == "premium"
    expire_at       = row.get("premium_expire_at")
    expire_readable = ""
    if expire_at:
        from datetime import datetime
        expire_readable = datetime.fromtimestamp(expire_at, tz=TZ).strftime("%d.%m.%Y")

    return jsonify({
        "ok":             True,
        "is_premium":     is_premium,
        "expire_at":      expire_at,
        "expire_readable": expire_readable,
    })


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

@app.route("/q-image/<path:photo_id>")
def q_image(photo_id):
    """Savol rasmi — WEB_ (lokal), http(s) (to'g'ridan URL), yoki Telegram file_id"""
    token = request.args.get("token")
    user = validate_token(token)
    if not user: return abort(401)

    try:
        # 1. Lokal yuklangan rasm: WEB_filename.jpg
        if photo_id.startswith("WEB_"):
            filename = photo_id[4:]          # "WEB_" ni olib tashlash
            upload_dir = os.path.join(app.root_path, "static", "uploads")
            filepath = os.path.join(upload_dir, filename)
            if os.path.exists(filepath):
                return send_from_directory(upload_dir, filename)
            # Fayl yo'q — 404
            logging.warning(f"q_image: lokal fayl topilmadi: {filepath}")
            return abort(404)

        # 2. To'g'ridan URL (http/https)
        if photo_id.startswith("http://") or photo_id.startswith("https://"):
            return redirect(photo_id)

        # 3. Telegram file_id (BOT_ prefiksi yoki oddiy file_id)
        actual_file_id = photo_id
        if photo_id.startswith("BOT_"):
            parts = photo_id.split("_", 2)
            actual_file_id = parts[2] if len(parts) >= 3 else photo_id

        clean_token = BOT_TOKENS[0] if BOT_TOKENS else os.getenv("BOT_TOKEN", "").strip()
        if not clean_token:
            logging.error("q_image: bot token topilmadi")
            return abort(404)

        res = requests.get(
            f"https://api.telegram.org/bot{clean_token}/getFile?file_id={actual_file_id}",
            timeout=8
        ).json()

        if res.get("ok"):
            file_path = res["result"]["file_path"]
            img_url = f"https://api.telegram.org/file/bot{clean_token}/{file_path}"
            # Rasmni to'g'ridan serverdan brauzerga yetkazamiz (redirect o'rniga proxy)
            img_res = requests.get(img_url, timeout=10)
            if img_res.status_code == 200:
                content_type = img_res.headers.get("Content-Type", "image/jpeg")
                return img_res.content, 200, {
                    "Content-Type": content_type,
                    "Cache-Control": "public, max-age=86400"
                }
        else:
            logging.warning(f"q_image Telegram xato: {res.get('description')} | file_id={actual_file_id}")

    except Exception as e:
        logging.error(f"q_image xatolik: {e} | photo_id={photo_id}")

    return abort(404)

@app.route("/animations.js")
def serve_animations_js():
    """templates/animations.js faylini JavaScript sifatida serve qiladi"""
    from flask import Response
    try:
        tmpl_path = os.path.join(app.root_path, "templates", "animations.js")
        with open(tmpl_path, "r", encoding="utf-8") as f:
            js_content = f.read()
        return Response(js_content, mimetype="application/javascript",
                        headers={"Cache-Control": "public, max-age=3600"})
    except Exception as e:
        logging.error(f"animations.js serve xatosi: {e}")
        return Response("/* animations.js not found */", mimetype="application/javascript"), 404

@app.route("/guide")
def guide_page():
    token = request.args.get("token", "")
    user = validate_token(token) if token else None
    lang = session.get("lang", "uz")
    if user:
        db_lang = user.get("lang")
        if db_lang and db_lang in ["uz", "ru", "uz_cyrl"]:
            lang = db_lang
    current_user_bg = user.get("custom_bg") if user else None
    return render_template("guide.html", token=token, lang=lang, current_user_bg=current_user_bg, get_text=get_text)

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
        "uz": [
            "🏠", "📊", "🏅", "🏆", "📚", "👥", "🔔",
            "🃏", "⚔️", "🤝", "📜"
        ],
        "uz_cyrl": [
            "🏠", "📊", "🏅", "🏆", "📚", "👥", "🔔",
            "🃏", "⚔️", "🤝", "📜"
        ],
        "ru": [
            "🏠", "📊", "🏅", "🏆", "📚", "👥", "🔔",
            "🃏", "⚔️", "🤝", "📜"
        ],
    }.get(lang, ["🏠","📊","🏅","🏆","📚","👥","🔔","🃏","⚔️","🤝","📜"])

    titles = {
        "uz":     ["Bosh sahifa","Statistika","Yutuqlar","Reyting","Kutubxona","Guruhlar","Bildirishnomalar","Flashcards","Challenge","Hamkor","Sertifikatlar"],
        "uz_cyrl":["Бош саҳифа","Статистика","Ютуқлар","Рейтинг","Кутубхона","Гуруҳлар","Билдиришномалар","Флэшкардлар","Челлендж","Ҳамкор","Сертификатлар"],
        "ru":     ["Главная","Статистика","Достижения","Рейтинг","Библиотека","Группы","Уведомления","Флэшкарты","Челлендж","Партнёр","Сертификаты"],
    }.get(lang, ["Bosh sahifa","Statistika","Yutuqlar","Reyting","Kutubxona","Guruhlar","Bildirishnomalar","Flashcards","Challenge","Hamkor","Sertifikatlar"])

    paths = [
        "/", "/stats", "/achievements", "/leaderboard",
        "/library", "/my-groups", "/notifications",
        "/flashcards", "/challenges", "/affiliate", "/my-certs"
    ]

    links = "".join([
        f'<a href="{p}?token={token}" title="{t}">{icon}</a>'
        for p, icon, t in zip(paths, L, titles)
    ])
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
    level   = int(stats.get("level", 1) or 1)
    xp      = int(stats.get("xp", 0) or 0)
    cur_lvl_xp  = db.xp_for_level(level)
    next_lvl_xp = db.xp_for_level(level + 1)
    needed  = max(1, next_lvl_xp - cur_lvl_xp)
    pct     = min(100, int((xp - cur_lvl_xp) * 100 / needed))
    total_q = int(stats.get("total_questions", 0) or 0)
    total_c = int(stats.get("total_correct", 0) or 0)
    accuracy = round(total_c * 100 / total_q, 1) if total_q > 0 else 0
    streak  = int(stats.get("current_streak", 0) or 0)
    longest = int(stats.get("longest_streak", 0) or 0)

    # Chart uchun ma'lumotlar
    progress = db.get_user_progress(uid)
    chart_labels = json.dumps([str(to_dict(p)['day']) for p in progress])
    chart_tests  = json.dumps([int(to_dict(p)['cnt']) for p in progress])
    chart_scores = json.dumps([round(float(to_dict(p)['avg_score'] or 0), 1) for p in progress])

    # Heatmap (24 soat faollik)
    heatmap = db.get_user_activity_heatmap(uid)
    heatmap_data = json.dumps([heatmap.get(i, 0) for i in range(24)])
    heatmap_labels = json.dumps([f"{i:02d}:00" for i in range(24)])
    # Chart 2 backgroundColor — f-string + backtick muammoidan himoya
    heatmap_bg_js = "hmData.map(v=>v===0?'rgba(255,255,255,0.05)':'rgba(56,211,159,'+(0.2+0.8*(v/maxHm)).toFixed(2)+')')"

    lbl_stats    = {"ru": "Статистика",    "uz_cyrl": "Статистика"   }.get(lang, "Statistika")
    lbl_level    = {"ru": "Уровень",       "uz_cyrl": "Даража"       }.get(lang, "Daraja")
    lbl_taken    = {"ru": "Пройдено",      "uz_cyrl": "Ишланган"     }.get(lang, "Ishlangan")
    lbl_created  = {"ru": "Создано",       "uz_cyrl": "Яратилган"    }.get(lang, "Yaratilgan")
    lbl_accuracy = {"ru": "Точность",      "uz_cyrl": "Аниқлик"      }.get(lang, "Aniqlik")
    lbl_streak   = {"ru": "Серия",         "uz_cyrl": "Серия"        }.get(lang, "Streak")
    lbl_longest  = {"ru": "Рекорд серии",  "uz_cyrl": "Рекорд серия" }.get(lang, "Rekord streak")
    lbl_30       = {"ru": "Активность (последние 30 дней)", "uz_cyrl": "Фаоллик (30 кун)"}.get(lang, "Faollik (30 kun)")
    lbl_tests    = {"ru": "Тестов",        "uz_cyrl": "Тестлар"      }.get(lang, "Testlar")
    lbl_avg      = {"ru": "Средний балл",  "uz_cyrl": "Ўртача балл"  }.get(lang, "Ortacha ball")
    lbl_hour     = {"ru": "По часам",      "uz_cyrl": "Соатлар бўйича"}.get(lang, "Soatlar bo'yicha")

    lbl_no_data  = {"ru": "Данных нет", "uz_cyrl": "Маълумот йўқ"}.get(lang, "Ma'lumot yo'q")

    html_page = _PAGE_STYLE + f"""
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <div class="wrap">
      {_nav_html(token, lang)}
      <h1>📊 {lbl_stats}</h1>

      <!-- XP Progress -->
      <div class="card">
        <h2>⭐ {lbl_level} {level}</h2>
        <div class="bar"><div class="bar-fill" style="width:{pct}%"></div></div>
        <p style="color:rgba(243,246,255,.65);font-size:13px">{xp} / {next_lvl_xp} XP ({pct}%)</p>
      </div>

      <!-- Stat boxes -->
      <div class="card">
        <div class="stat-grid">
          <div class="stat-box"><div class="stat-num">{int(stats.get('tests_taken',0) or 0)}</div><div class="stat-label">{lbl_taken}</div></div>
          <div class="stat-box"><div class="stat-num">{int(stats.get('tests_created',0) or 0)}</div><div class="stat-label">{lbl_created}</div></div>
          <div class="stat-box"><div class="stat-num">{accuracy}%</div><div class="stat-label">{lbl_accuracy}</div></div>
          <div class="stat-box"><div class="stat-num">🔥 {streak}</div><div class="stat-label">{lbl_streak}</div></div>
          <div class="stat-box"><div class="stat-num">🏆 {longest}</div><div class="stat-label">{lbl_longest}</div></div>
          <div class="stat-box"><div class="stat-num">{total_c}</div><div class="stat-label">✅ {lbl_taken}</div></div>
        </div>
      </div>

      <!-- Chart 1: 30 kunlik faollik -->
      <div class="card">
        <h2>📈 {lbl_30}</h2>
        {"<canvas id='activityChart' style='max-height:220px'></canvas>" if progress else "<div class='empty'>" + lbl_no_data + "</div>"}
      </div>

      <!-- Chart 2: Soatlik faollik -->
      <div class="card">
        <h2>🕐 {lbl_hour}</h2>
        <canvas id="heatmapChart" style="max-height:260px"></canvas>
      </div>
    </div>

    <script>
    Chart.defaults.color = 'rgba(243,246,255,0.7)';
    Chart.defaults.font  = {{family: "-apple-system, sans-serif", size: 11}};

    // Chart 1 — Faollik
    const labels = {chart_labels};
    if(labels.length > 0) {{
      const ctx1 = document.getElementById('activityChart').getContext('2d');
      new Chart(ctx1, {{
        data: {{
          labels: labels,
          datasets: [
            {{
              type: 'bar',
              label: '{lbl_tests}',
              data: {chart_tests},
              backgroundColor: 'rgba(56,211,159,0.35)',
              borderColor: '#38d39f',
              borderWidth: 1.5,
              borderRadius: 5,
              yAxisID: 'y'
            }},
            {{
              type: 'line',
              label: '{lbl_avg}',
              data: {chart_scores},
              borderColor: '#6cb2ff',
              backgroundColor: 'rgba(108,178,255,0.1)',
              borderWidth: 2,
              pointRadius: 3,
              tension: 0.4,
              fill: true,
              yAxisID: 'y2'
            }}
          ]
        }},
        options: {{
          responsive: true, maintainAspectRatio: true,
          interaction: {{mode: 'index', intersect: false}},
          plugins: {{legend: {{labels: {{color: 'rgba(243,246,255,0.7)'}}}}}},
          scales: {{
            x: {{grid: {{color: 'rgba(255,255,255,0.05)'}}, ticks: {{maxTicksLimit: 8}}}},
            y:  {{position: 'left',  grid: {{color: 'rgba(255,255,255,0.05)'}}, beginAtZero: true}},
            y2: {{position: 'right', grid: {{display: false}}, beginAtZero: true}}
          }}
        }}
      }});
    }}

    // Chart 2 — Soatlik heatmap
    const ctx2 = document.getElementById('heatmapChart').getContext('2d');
    const hmData = {heatmap_data};
    const maxHm  = Math.max(...hmData, 1);
    new Chart(ctx2, {{
      type: 'bar',
      data: {{
        labels: {heatmap_labels},
        datasets: [{{
          label: '{lbl_tests}',
          data: hmData,
          backgroundColor: {heatmap_bg_js},
          borderColor: 'rgba(56,211,159,0.4)',
          borderWidth: 1,
          borderRadius: 4
        }}]
      }},
      options: {{
        responsive: true, maintainAspectRatio: true,
        plugins: {{legend: {{display: false}}}},
        scales: {{
          x: {{grid: {{color: 'rgba(255,255,255,0.05)'}}, ticks: {{maxTicksLimit: 12}}}},
          y: {{grid: {{color: 'rgba(255,255,255,0.05)'}}, beginAtZero: true, display: false}}
        }}
      }}
    }});
    </script>
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

# ==========================================
# 📜 SERTIFIKAT SAHIFASI
# ==========================================
@app.route("/cert/<cert_code>")
def web_certificate(cert_code):
    cert = db.get_certificate(cert_code)
    lang = session.get("lang", "uz")
    if not cert:
        return f"<h2>❌ Sertifikat topilmadi: {html.escape(cert_code)}</h2>", 404

    issued = datetime.fromtimestamp(int(cert.get('issued_at') or 0), tz=TZ).strftime("%d.%m.%Y")
    name = html.escape((cert.get('first_name') or '') + ' ' + (cert.get('username') and f"@{cert['username']}" or '')).strip()
    title = html.escape(cert.get('title') or 'Test')
    score = float(cert.get('score') or 0)
    code = html.escape(cert_code)

    return f"""<!DOCTYPE html>
<html lang="uz">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Sertifikat - {title}</title>
  <style>
    *{{margin:0;padding:0;box-sizing:border-box}}
    body{{font-family:'Georgia',serif;background:linear-gradient(135deg,#1a1a2e,#16213e);min-height:100vh;display:flex;align-items:center;justify-content:center;padding:20px}}
    .cert{{background:linear-gradient(135deg,#fff9f0,#fff);border:3px solid #d4af37;border-radius:20px;padding:50px 40px;max-width:700px;width:100%;text-align:center;box-shadow:0 20px 60px rgba(0,0,0,.3),inset 0 0 0 8px rgba(212,175,55,.15)}}
    .logo{{font-size:48px;margin-bottom:10px}}
    .cert-title{{font-size:13px;letter-spacing:4px;text-transform:uppercase;color:#888;margin-bottom:20px}}
    .presents{{font-size:18px;color:#555;margin-bottom:10px}}
    .name{{font-size:36px;font-weight:700;color:#1a1a2e;border-bottom:2px solid #d4af37;padding-bottom:12px;margin-bottom:20px}}
    .body-text{{font-size:16px;color:#444;line-height:1.6;margin-bottom:20px}}
    .test-title{{font-size:22px;font-weight:700;color:#d4af37;margin:10px 0}}
    .score{{font-size:48px;font-weight:900;color:#2d7d46;margin:20px 0}}
    .score-label{{font-size:14px;color:#888}}
    .footer{{margin-top:30px;padding-top:20px;border-top:1px dashed #d4af37;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px}}
    .date{{font-size:14px;color:#888}}
    .code{{font-family:monospace;font-size:13px;color:#aaa;background:#f5f5f5;padding:4px 10px;border-radius:6px}}
    .seal{{font-size:40px}}
    @media print{{body{{background:#fff}}.cert{{box-shadow:none;border:3px solid #d4af37}}}}
    .print-btn{{margin-top:20px;padding:12px 24px;background:#d4af37;color:#fff;border:none;border-radius:10px;font-size:15px;cursor:pointer;font-weight:600}}
    .print-btn:hover{{background:#c49b2e}}
  </style>
</head>
<body>
  <div class="cert">
    <div class="logo">🎓</div>
    <div class="cert-title">Geo Ustoz Platform</div>
    <div class="presents">Ushbu sertifikat taqdim etiladi</div>
    <div class="name">{name}</div>
    <div class="body-text">quyidagi test muvaffaqiyatli yakunlaganligi uchun</div>
    <div class="test-title">{title}</div>
    <div class="score">{score:g}</div>
    <div class="score-label">ball</div>
    <div class="footer">
      <div class="date">📅 {issued}</div>
      <div class="seal">🏅</div>
      <div class="code">#{code}</div>
    </div>
    <button class="print-btn" onclick="window.print()">🖨️ Chop etish</button>
  </div>
</body>
</html>"""

# ==========================================
# 🔗 VAQTINCHALIK HAVOLA
# ==========================================
@app.route("/t/<token_val>")
def temp_link_redirect(token_val):
    lang = session.get("lang", "uz")
    link = db.get_temp_link(token_val)
    if not link:
        return """<html><body style="font-family:sans-serif;text-align:center;padding:60px;background:#0f172a;color:#fff">
            <h2>❌ Havola yaroqsiz yoki muddati tugagan</h2>
            <p style="color:#888;margin-top:10px">Bu vaqtinchalik havola endi ishlamaydi.</p></body></html>""", 404
    link = dict(link)
    db.use_temp_link(token_val)
    # Token bo'lmasa - foydalanuvchi botdan kiritishi kerak
    user_token = request.args.get("token", "")
    target = f"/solve/{link['test_id']}?token={user_token}&via_temp=1" if user_token else f"/solve/{link['test_id']}"
    return redirect(target)

# ==========================================
# 🃏 FLASHCARD SAHIFALARI
# ==========================================
@app.route("/flashcards")
def web_flashcards():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user:
        return abort(401)
    uid = int(user["user_id"])

    my_sets = db.get_user_flashcard_sets(uid)
    public_sets = db.get_public_flashcard_sets(limit=20)
    cats = db.get_categories()

    my_rows = ""
    for s in my_sets:
        s = dict(s)
        my_rows += f"""<div class="row" style="justify-content:space-between">
          <div><b>{html.escape(s.get('title',''))}</b>
          <span style="color:#a0aec0;font-size:13px;margin-left:8px">{s.get('card_count',0)} karta</span></div>
          <a class="btn" href="/flashcards/{s['id']}?token={token}">▶️ O'rganish</a>
        </div>"""

    pub_rows = ""
    for s in public_sets:
        s = dict(s)
        if s.get('owner_id') == uid:
            continue
        pub_rows += f"""<div class="row" style="justify-content:space-between">
          <div><b>{html.escape(s.get('title',''))}</b>
          <span style="color:#a0aec0;font-size:13px;margin-left:8px">{s.get('card_count',0)} karta · {html.escape(s.get('first_name','') or '')}</span></div>
          <a class="btn" href="/flashcards/{s['id']}?token={token}">▶️</a>
        </div>"""

    # O'zgaruvchilar (til bo'yicha)
    title_lbl = {"ru": "Мои наборы",      "uz_cyrl": "Менинг тўпламларим"}.get(lang, "Mening to'plamlarim")
    pub_lbl   = {"ru": "Публичные наборы", "uz_cyrl": "Оммавий тўпламлар"}.get(lang, "Ommaviy to'plamlar")
    new_lbl   = {"ru": "Создать набор",   "uz_cyrl": "Тўплам яратиш"}.get(lang, "Yangi to'plam yaratish")
    name_ph   = {"ru": "Название",        "uz_cyrl": "Номи"}.get(lang, "To'plam nomi")
    btn_create = {"ru": "Создать",        "uz_cyrl": "Яратиш"}.get(lang, "Yaratish")
    my_empty  = {"ru": "Нет наборов",     "uz_cyrl": "Тўплам йўқ"}.get(lang, "Hali to'plam yo'q")
    pub_empty = {"ru": "Нет публичных наборов", "uz_cyrl": "Оммавий тўплам йўқ"}.get(lang, "Hali ommaviy to'plam yo'q")

    html_page = _PAGE_STYLE + f"""
    <div class="wrap">
      {_nav_html(token, lang)}
      <h1>🃏 Flashcards</h1>
      <div class="card">
        <h2>➕ {new_lbl}</h2>
        <div style="display:flex;gap:10px">
          <input type="text" id="fs-title" placeholder="{name_ph}" style="flex:1;padding:11px;border-radius:8px;border:1px solid rgba(255,255,255,.2);background:rgba(0,0,0,.3);color:#f3f6ff">
          <button class="btn" onclick="createSet()" style="background:#667eea;color:#fff;border:none">{btn_create}</button>
        </div>
      </div>
      <div class="card"><h2>📚 {title_lbl}</h2>
        {my_rows if my_rows else '<div class="empty">' + my_empty + '</div>'}
      </div>
      <div class="card"><h2>🌐 {pub_lbl}</h2>
        {pub_rows if pub_rows else '<div class="empty">' + pub_empty + '</div>'}
      </div>
    </div>
    <script>
    const token="{token}";
    function createSet(){{
      const title=document.getElementById('fs-title').value.trim();
      if(!title)return;
      fetch('/api/flashcard/create',{{method:'POST',headers:{{'Content-Type':'application/json'}},
        body:JSON.stringify({{token,title}})}})
      .then(r=>r.json()).then(d=>{{if(d.success)location.reload();else alert(d.error||'Xato');}});
    }}
    </script>"""
    return html_page

@app.route("/flashcards/<int:set_id>")
def web_flashcard_study(set_id):
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user:
        return abort(401)
    uid = int(user["user_id"])
    fset = db.get_flashcard_set(set_id)
    if not fset:
        return abort(404)
    cards = db.get_flashcards(set_id, user_id=uid)
    cards_json = json.dumps([{
        "id": c["id"] if isinstance(c, dict) else dict(c)["id"],
        "front": (c if isinstance(c, dict) else dict(c)).get("front",""),
        "back":  (c if isinstance(c, dict) else dict(c)).get("back",""),
        "hint":  (c if isinstance(c, dict) else dict(c)).get("hint",""),
    } for c in cards], ensure_ascii=False)

    title = html.escape((fset.get('title') or '') if isinstance(fset, dict) else dict(fset).get('title',''))
    html_page = _PAGE_STYLE + f"""
    <div class="wrap" style="max-width:600px">
      {_nav_html(token, lang)}
      <h1>🃏 {title}</h1>
      <div class="card" style="text-align:center">
        <div id="progress" style="color:#a0aec0;font-size:14px;margin-bottom:16px">0 / {len(cards)}</div>
        <div id="card-box" onclick="flipCard()" style="min-height:200px;cursor:pointer;display:flex;align-items:center;justify-content:center;flex-direction:column;padding:20px">
          <div id="card-front" style="font-size:22px;font-weight:700">Boshlash uchun bosing ▶️</div>
          <div id="card-back" style="font-size:18px;color:#38d39f;margin-top:16px;display:none"></div>
          <div id="card-hint" style="font-size:13px;color:#718096;margin-top:8px;display:none"></div>
        </div>
        <div id="actions" style="display:none;gap:12px;justify-content:center;margin-top:20px">
          <button class="btn" onclick="answer(false)" style="background:#f56565;color:#fff;border:none;padding:14px 28px;font-size:16px">❌ Bilmadim</button>
          <button class="btn" onclick="answer(true)" style="background:#48bb78;color:#fff;border:none;padding:14px 28px;font-size:16px">✅ Bildim</button>
        </div>
        <div id="done-box" style="display:none;padding:30px">
          <div style="font-size:48px">🎉</div>
          <div style="font-size:20px;font-weight:700;margin:12px 0">Tugatdingiz!</div>
          <div id="done-stats" style="color:#a0aec0"></div>
          <button class="btn" onclick="restart()" style="margin-top:16px;background:#667eea;color:#fff;border:none">🔄 Qayta boshlash</button>
        </div>
      </div>
      <div class="card">
        <h2>➕ Karta qo'shish</h2>
        <input type="text" id="fc-front" placeholder="Old (savol)" style="width:100%;padding:10px;border-radius:8px;border:1px solid rgba(255,255,255,.2);background:rgba(0,0,0,.3);color:#f3f6ff;margin-bottom:8px">
        <input type="text" id="fc-back" placeholder="Orqa (javob)" style="width:100%;padding:10px;border-radius:8px;border:1px solid rgba(255,255,255,.2);background:rgba(0,0,0,.3);color:#f3f6ff;margin-bottom:8px">
        <input type="text" id="fc-hint" placeholder="Maslahat (ixtiyoriy)" style="width:100%;padding:10px;border-radius:8px;border:1px solid rgba(255,255,255,.2);background:rgba(0,0,0,.3);color:#f3f6ff;margin-bottom:8px">
        <button class="btn" onclick="addCard()" style="background:#48bb78;color:#fff;border:none">➕ Qo'shish</button>
      </div>
    </div>
    <script>
    const token="{token}", setId={set_id};
    const cards={cards_json};
    let idx=0, flipped=false, correct=0, wrong=0;
    function showCard(){{
      if(idx>=cards.length){{
        document.getElementById('card-box').style.display='none';
        document.getElementById('actions').style.display='none';
        document.getElementById('done-box').style.display='block';
        document.getElementById('done-stats').innerHTML='✅ '+correct+' | ❌ '+wrong;
        return;
      }}
      document.getElementById('progress').textContent=(idx+1)+' / '+cards.length;
      document.getElementById('card-front').textContent=cards[idx].front;
      document.getElementById('card-back').textContent=cards[idx].back;
      document.getElementById('card-back').style.display='none';
      document.getElementById('card-hint').textContent=cards[idx].hint||'';
      document.getElementById('card-hint').style.display='none';
      document.getElementById('actions').style.display='none';
      flipped=false;
    }}
    function flipCard(){{
      if(flipped)return;
      flipped=true;
      document.getElementById('card-back').style.display='block';
      if(cards[idx].hint)document.getElementById('card-hint').style.display='block';
      document.getElementById('actions').style.display='flex';
    }}
    function answer(ok){{
      if(ok)correct++;else wrong++;
      fetch('/api/flashcard/answer',{{method:'POST',headers:{{'Content-Type':'application/json'}},
        body:JSON.stringify({{token,card_id:cards[idx].id,correct:ok}})}});
      idx++;showCard();
    }}
    function restart(){{idx=0;correct=0;wrong=0;
      document.getElementById('done-box').style.display='none';
      document.getElementById('card-box').style.display='flex';
      showCard();}}
    function addCard(){{
      const front=document.getElementById('fc-front').value.trim();
      const back=document.getElementById('fc-back').value.trim();
      const hint=document.getElementById('fc-hint').value.trim();
      if(!front||!back)return;
      fetch('/api/flashcard/add',{{method:'POST',headers:{{'Content-Type':'application/json'}},
        body:JSON.stringify({{token,set_id:setId,front,back,hint}})}})
      .then(r=>r.json()).then(d=>{{if(d.success)location.reload();else alert(d.error||'Xato');}});
    }}
    if(cards.length>0){{showCard();}}
    </script>"""
    return html_page

# ==========================================
# 📣 ADMIN - BROADCAST WEB PANEL
# ==========================================
@app.route("/admin/broadcast")
def admin_broadcast():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return abort(403)

    broadcasts = [dict(b) for b in db.get_broadcasts(limit=15)]
    rows = ""
    for b in broadcasts:
        status_color = "#48bb78" if b.get('status') == 'done' else "#ed8936"
        rows += f"""<div class="row">
          <div>
            <b style="font-size:14px">{html.escape((b.get('message') or '')[:60])}...</b><br>
            <span style="color:#a0aec0;font-size:12px">
              👥 {b.get('target','all')} · ✅{b.get('sent_count',0)} · ❌{b.get('fail_count',0)}
            </span>
          </div>
          <span style="color:{status_color};font-weight:700;font-size:13px">{b.get('status','?')}</span>
        </div>"""

    all_count = len(db.get_all_user_ids())
    prem_count = len(db.get_all_user_ids(status_filter='premium'))

    bc_empty = "Hali broadcast yo'q"
    page = _PAGE_STYLE + f"""
    <div class="wrap">
      {_nav_html(token, lang)}
      <h1>📢 Broadcast - Ommaviy Xabar</h1>
      <div class="card">
        <div class="stat-grid" style="margin-bottom:20px">
          <div class="stat-box"><div class="stat-num">{all_count}</div><div class="stat-lbl">Jami foydalanuvchi</div></div>
          <div class="stat-box"><div class="stat-num" style="color:#fbbf24">{prem_count}</div><div class="stat-lbl">Premium</div></div>
        </div>
        <h2>📝 Yangi xabar</h2>
        <div id="bc-alert" style="display:none;padding:12px;border-radius:8px;margin-bottom:12px;font-size:14px"></div>
        <textarea id="bc-text" placeholder="Xabar matni (HTML: b, i, a teglar)" style="width:100%;height:120px;padding:12px;border-radius:8px;border:1px solid rgba(255,255,255,.2);background:rgba(0,0,0,.3);color:#f3f6ff;font-size:14px;resize:vertical"></textarea>
        <div style="display:flex;gap:10px;margin-top:12px;flex-wrap:wrap">
          <button class="btn" onclick="sendBroadcast('all')" style="background:#3b82f6;color:#fff;border:none;flex:1">
            📢 Barchaga ({all_count})
          </button>
          <button class="btn" onclick="sendBroadcast('premium')" style="background:#d4af37;color:#fff;border:none;flex:1">
            💎 Faqat Premium ({prem_count})
          </button>
        </div>
      </div>
      <div class="card">
        <h2>📋 So'nggi broadcastlar</h2>
        {rows if rows else '<div class="empty">' + bc_empty + '</div>'}
      </div>
    </div>
    <script>
    const token="{token}";
    function showAlert(msg,ok){{
      const el=document.getElementById('bc-alert');
      el.style.display='block';
      el.style.background=ok?'rgba(72,187,120,.2)':'rgba(245,101,101,.2)';
      el.style.color=ok?'#68d391':'#fc8181';
      el.innerHTML=msg;
      setTimeout(()=>el.style.display='none',5000);
    }}
    function sendBroadcast(target){{
      const text=document.getElementById('bc-text').value.trim();
      if(!text){{showAlert('❌ Xabar matni kiritilmagan!',false);return;}}
      if(!confirm('Haqiqatan ham '+target+' foydalanuvchilarga xabar yubormoqchimisiz?'))return;
      const btn=event.target;btn.disabled=true;btn.textContent='⏳ Yuborilmoqda...';
      fetch('/api/admin/broadcast',{{method:'POST',headers:{{'Content-Type':'application/json'}},
        body:JSON.stringify({{token,message:text,target}})}})
      .then(r=>r.json()).then(d=>{{
        btn.disabled=false;btn.textContent='Yuborish';
        if(d.success){{showAlert('✅ Broadcast boshlandi! '+d.queued+' ta xabar navbatda.',true);}}
        else showAlert('❌ '+(d.error||'Xato'),false);
      }}).catch(()=>{{btn.disabled=false;showAlert('❌ Server xatosi',false);}});
    }}
    </script>"""
    return page

# ==========================================
# 📣 REKLAMALAR BOSHQARUVI
# ==========================================
@app.route("/admin/ads")
def admin_ads():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return abort(403)

    ads = [dict(a) for a in db.get_all_ads()]
    rows = ""
    ad_empty_div = "<div class='empty'>Hali reklama yo'q</div>"
    for a in ads:
        active = int(a.get('is_active') or 0)
        badge = '<span style="color:#68d391">✅ Aktiv</span>' if active else '<span style="color:#fc8181">❌ To\'xtatilgan</span>'
        rows += f"""<div class="row">
          <div style="flex:1">
            <b>{html.escape(a.get('title') or '')}</b> {badge}<br>
            <span style="color:#a0aec0;font-size:13px">
              👁 {a.get('show_count',0)} ko'rish · 🖱 {a.get('click_count',0)} klik ·
              CTR {round(a['click_count']*100/max(1,a['show_count']),1)}%
            </span>
          </div>
          <div style="display:flex;gap:8px">
            <button class="btn" onclick="toggleAd({a['id']},{1 if not active else 0})"
              style="background:{'#f56565' if active else '#48bb78'};color:#fff;border:none;padding:8px 12px">
              {'⏸' if active else '▶️'}
            </button>
            <button class="btn" onclick="deleteAd({a['id']})"
              style="background:#718096;color:#fff;border:none;padding:8px 12px">🗑</button>
          </div>
        </div>"""

    page = _PAGE_STYLE + f"""
    <div class="wrap">
      {_nav_html(token, lang)}
      <h1>📣 Reklamalar Boshqaruvi</h1>
      <div class="card">
        <h2>➕ Yangi Reklama</h2>
        <div id="ad-alert" style="display:none;padding:12px;border-radius:8px;margin-bottom:12px"></div>
        <input type="text" id="ad-title" placeholder="Sarlavha" style="width:100%;padding:10px;border-radius:8px;border:1px solid rgba(255,255,255,.2);background:rgba(0,0,0,.3);color:#f3f6ff;margin-bottom:8px">
        <textarea id="ad-body" placeholder="Reklama matni (HTML qo'llab-quvvatlanadi)" style="width:100%;height:80px;padding:10px;border-radius:8px;border:1px solid rgba(255,255,255,.2);background:rgba(0,0,0,.3);color:#f3f6ff;margin-bottom:8px;resize:vertical"></textarea>
        <input type="text" id="ad-url" placeholder="Havola URL (ixtiyoriy)" style="width:100%;padding:10px;border-radius:8px;border:1px solid rgba(255,255,255,.2);background:rgba(0,0,0,.3);color:#f3f6ff;margin-bottom:8px">
        <div style="display:flex;gap:10px">
          <input type="number" id="ad-days" placeholder="Necha kun (default: 30)" style="flex:1;padding:10px;border-radius:8px;border:1px solid rgba(255,255,255,.2);background:rgba(0,0,0,.3);color:#f3f6ff">
          <button class="btn" onclick="createAd()" style="background:#3b82f6;color:#fff;border:none">Yaratish</button>
        </div>
      </div>
      <div class="card">
        <h2>📋 Mavjud Reklamalar ({len(ads)} ta)</h2>
        {rows if rows else ad_empty_div}
      </div>
    </div>
    <script>
    const token="{token}";
    function showAlert(el,msg,ok){{
      const e=document.getElementById(el);e.style.display='block';
      e.style.background=ok?'rgba(72,187,120,.2)':'rgba(245,101,101,.2)';
      e.style.color=ok?'#68d391':'#fc8181';e.innerHTML=msg;
      setTimeout(()=>e.style.display='none',4000);
    }}
    function createAd(){{
      const title=document.getElementById('ad-title').value.trim();
      const body=document.getElementById('ad-body').value.trim();
      const url=document.getElementById('ad-url').value.trim();
      const days=parseInt(document.getElementById('ad-days').value)||30;
      if(!title||!body){{showAlert('ad-alert','❌ Sarlavha va matn kiritilishi shart',false);return;}}
      fetch('/api/admin/ad/create',{{method:'POST',headers:{{'Content-Type':'application/json'}},
        body:JSON.stringify({{token,title,body,url,days}})}})
      .then(r=>r.json()).then(d=>{{
        if(d.success)location.reload();
        else showAlert('ad-alert','❌ '+(d.error||'Xato'),false);
      }});
    }}
    function toggleAd(id,val){{
      fetch('/api/admin/ad/toggle',{{method:'POST',headers:{{'Content-Type':'application/json'}},
        body:JSON.stringify({{token,ad_id:id,is_active:val}})}})
      .then(r=>r.json()).then(d=>{{if(d.success)location.reload();}});
    }}
    function deleteAd(id){{
      if(!confirm('Reklama o\'chirilsinmi?'))return;
      fetch('/api/admin/ad/delete',{{method:'POST',headers:{{'Content-Type':'application/json'}},
        body:JSON.stringify({{token,ad_id:id}})}})
      .then(r=>r.json()).then(d=>{{if(d.success)location.reload();}});
    }}
    </script>"""
    return page

# ==========================================
# 🤝 AFFILIATE PANEL (Foydalanuvchi)
# ==========================================
@app.route("/affiliate")
def web_affiliate():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user:
        return abort(401)
    uid = int(user["user_id"])
    aff = db.get_or_create_affiliate(uid)
    stats = db.get_affiliate_stats(uid) or aff

    ref_link = f"https://t.me/bot?start=ref_{uid}"  # bot username web.py da get_bot_username() orqali olish mumkin
    try:
        bot_username = get_bot_username()
        ref_link = f"https://t.me/{bot_username}?start=ref_{uid}"
    except Exception:
        pass

    earned = float(aff.get('total_earned') or 0)
    balance = float(aff.get('balance') or 0)
    ref_count = int(stats.get('referral_count') or aff.get('total_referrals') or 0)
    prem_refs = int(stats.get('premium_referrals') or 0)
    code = html.escape(aff.get('ref_code') or '')

    top = db.get_top_affiliates(limit=10)
    top_rows = ""
    top_empty_div = "<div class='empty'>Hali hamkorlar yo'q</div>"
    for i, a in enumerate(top, 1):
        a = dict(a)
        name = html.escape(a.get('first_name') or a.get('username') or f"User{a['user_id']}")
        top_rows += f"<div class='row'><span>{'🥇' if i==1 else '🥈' if i==2 else '🥉' if i==3 else str(i)+'.'} {name}</span><span>{a.get('total_referrals',0)} ta</span></div>"

    html_page = _PAGE_STYLE + f"""
    <div class="wrap">
      {_nav_html(token, lang)}
      <h1>🤝 Hamkor Dasturi</h1>
      <div class="card">
        <div class="stat-grid">
          <div class="stat-box"><div class="stat-num">{ref_count}</div><div class="stat-lbl">Referal</div></div>
          <div class="stat-box"><div class="stat-num" style="color:#fbbf24">{prem_refs}</div><div class="stat-lbl">Premium referal</div></div>
          <div class="stat-box"><div class="stat-num" style="color:#38d39f">{earned:.2f}</div><div class="stat-lbl">Jami topilgan GWT</div></div>
          <div class="stat-box"><div class="stat-num" style="color:#6cb2ff">{balance:.2f}</div><div class="stat-lbl">Balans (GWT)</div></div>
        </div>
      </div>
      <div class="card">
        <h2>🔗 Referal havolangiz</h2>
        <p style="color:#a0aec0;margin-bottom:12px">Do'stlaringizni taklif qiling va har bir ro'yxatdan o'tgan uchun bonus oling!</p>
        <div style="background:rgba(0,0,0,.3);padding:14px;border-radius:10px;font-family:monospace;word-break:break-all;margin-bottom:12px">
          {html.escape(ref_link)}
        </div>
        <div style="display:flex;gap:10px">
          <button class="btn" onclick="copyLink()" style="background:#667eea;color:#fff;border:none;flex:1">📋 Nusxalash</button>
          <span style="display:flex;align-items:center;color:#a0aec0;font-size:14px">Kod: <b style="margin-left:6px">{code}</b></span>
        </div>
      </div>
      <div class="card">
        <h2>🏆 Top Hamkorlar</h2>
        {top_rows if top_rows else top_empty_div}
      </div>
    </div>
    <script>
    function copyLink(){{
      navigator.clipboard.writeText("{ref_link}").then(()=>alert('✅ Nusxalandi!'));
    }}
    </script>"""
    return html_page

# ==========================================
# 🌐 IP WHITELIST ADMIN
# ==========================================
@app.route("/admin/security")
def admin_security():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return abort(403)

    whitelist = [dict(w) for w in db.get_ip_whitelist()]
    ban_log_rows = ""
    try:
        with db._conn() as c:
            recent_bans = c.execute("""SELECT bl.*, u.first_name, u.username
                FROM ban_logs bl LEFT JOIN users u ON bl.user_id=u.user_id
                ORDER BY bl.created_at DESC LIMIT 20""").fetchall()
        for b in recent_bans:
            b = dict(b)
            action_color = "#fc8181" if b.get('action') == 'ban' else "#68d391"
            ban_log_rows += f"""<div class="row">
              <div>
                <b>{html.escape(b.get('first_name') or str(b.get('user_id')))}</b>
                <span style="color:{action_color};font-weight:700;margin-left:8px">{b.get('action','?').upper()}</span><br>
                <span style="color:#a0aec0;font-size:13px">{html.escape(b.get('reason') or '')}</span>
              </div>
            </div>"""
    except Exception:
        ban_log_rows = '<div class="empty">—</div>'

    wl_rows = "".join([
        f'<div class="row"><span><b>{html.escape(w.get("ip",""))}</b> · {html.escape(w.get("label",""))}</span>'
        f'<button class="btn" onclick="removeIP(\'{html.escape(w.get("ip",""))}\''
        f')" style="background:#f56565;color:#fff;border:none;padding:8px 12px">🗑</button></div>'
        for w in whitelist
    ])
    wl_empty = "<div class='empty'>Whitelist bo'sh (hamma IP ruxsat)</div>"
    if not wl_rows:
        wl_rows = wl_empty

    page = _PAGE_STYLE + f"""
    <div class="wrap">
      {_nav_html(token, lang)}
      <h1>🔐 Xavfsizlik Boshqaruvi</h1>
      <div class="card">
        <h2>🌐 IP Whitelist</h2>
        <p style="color:#a0aec0;font-size:13px;margin-bottom:14px">Ro'yxatga qo'shilgan IP lardan har doim kirish ruxsat etiladi (DDoS filtri ularga tegmaydi)</p>
        <div style="display:flex;gap:10px;margin-bottom:14px">
          <input type="text" id="wl-ip" placeholder="IP manzil (masalan: 192.168.1.1)" style="flex:1;padding:10px;border-radius:8px;border:1px solid rgba(255,255,255,.2);background:rgba(0,0,0,.3);color:#f3f6ff">
          <input type="text" id="wl-label" placeholder="Izoh" style="flex:1;padding:10px;border-radius:8px;border:1px solid rgba(255,255,255,.2);background:rgba(0,0,0,.3);color:#f3f6ff">
          <button class="btn" onclick="addIP()" style="background:#48bb78;color:#fff;border:none">Qo'shish</button>
        </div>
        {wl_rows}
      </div>
      <div class="card">
        <h2>🚫 So'nggi Ban/Unban tarixi</h2>
        {ban_log_rows}
      </div>
    </div>
    <script>
    const token="{token}";
    function addIP(){{
      const ip=document.getElementById('wl-ip').value.trim();
      const label=document.getElementById('wl-label').value.trim();
      if(!ip)return;
      fetch('/api/admin/security/whitelist/add',{{method:'POST',headers:{{'Content-Type':'application/json'}},
        body:JSON.stringify({{token,ip,label}})}})
      .then(r=>r.json()).then(d=>{{if(d.success)location.reload();else alert(d.error||'Xato');}});
    }}
    function removeIP(ip){{
      if(!confirm('IP o\'chirilsinmi? '+ip))return;
      fetch('/api/admin/security/whitelist/remove',{{method:'POST',headers:{{'Content-Type':'application/json'}},
        body:JSON.stringify({{token,ip}})}})
      .then(r=>r.json()).then(d=>{{if(d.success)location.reload();}});
    }}
    </script>"""
    return page

# ==========================================
# 📦 SAVOL BANKI
# ==========================================
@app.route("/question-bank")
def web_question_bank():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user:
        return abort(401)
    uid = int(user["user_id"])
    questions = db.get_question_bank(owner_id=uid, limit=100)
    cats = db.get_categories()

    q_rows = ""
    qb_empty_div = "<div class='empty'>Savol banki bo'sh. Testdan import qiling.</div>"
    for q in questions:
        q = dict(q)
        opts = json.loads(q.get('options_json') or '[]')
        ci = int(q.get('correct_index') or 0)
        correct_opt = html.escape(opts[ci] if ci < len(opts) else '?')
        q_rows += f"""<div class="row">
          <div style="flex:1">
            <b style="font-size:14px">{html.escape((q.get('question') or '')[:80])}</b><br>
            <span style="color:#68d391;font-size:13px">✅ {correct_opt}</span>
            <span style="color:#a0aec0;font-size:12px;margin-left:8px">· {q.get('use_count',0)} marta ishlatilgan</span>
          </div>
        </div>"""

    html_page = _PAGE_STYLE + f"""
    <div class="wrap">
      {_nav_html(token, lang)}
      <h1>📦 Savol Banki</h1>
      <div class="card">
        <h2>ℹ️ Ma'lumot</h2>
        <p style="color:#a0aec0">Savol bankiga savollar qo'shib, ulardan turli testlar yaratishingiz mumkin.</p>
        <div style="margin-top:12px;display:flex;gap:10px;flex-wrap:wrap">
          <input type="text" id="import-tid" placeholder="Test ID (importlash uchun)"
            style="flex:1;min-width:150px;padding:10px;border-radius:8px;border:1px solid rgba(255,255,255,.2);background:rgba(0,0,0,.3);color:#f3f6ff">
          <button class="btn" onclick="importTest()" style="background:#667eea;color:#fff;border:none">📥 Import</button>
        </div>
        <div id="import-msg" style="display:none;margin-top:8px;font-size:13px"></div>
      </div>
      <div class="card">
        <h2>📋 Savollarim ({len(questions)} ta)</h2>
        {q_rows if q_rows else qb_empty_div}
      </div>
    </div>
    <script>
    const token="{token}";
    function importTest(){{
      const tid=document.getElementById('import-tid').value.trim();
      if(!tid)return;
      fetch('/api/qbank/import',{{method:'POST',headers:{{'Content-Type':'application/json'}},
        body:JSON.stringify({{token,test_id:tid}})}})
      .then(r=>r.json()).then(d=>{{
        const el=document.getElementById('import-msg');
        el.style.display='block';
        if(d.success){{el.style.color='#68d391';el.textContent='✅ '+d.count+' ta savol import qilindi!';setTimeout(()=>location.reload(),1500);}}
        else{{el.style.color='#fc8181';el.textContent='❌ '+(d.error||'Xato');}}
      }});
    }}
    </script>"""
    return html_page

# ==========================================
# 📡 BARCHA YANGI API ENDPOINTLAR
# ==========================================
@app.route("/api/flashcard/create", methods=["POST"])
def api_flashcard_create():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user:
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    title = (data.get("title") or "").strip()
    if not title:
        return jsonify({"success": False, "error": "Title required"}), 400
    set_id = db.create_flashcard_set(int(user["user_id"]), title,
                                      is_public=int(data.get("is_public", 0)))
    return jsonify({"success": True, "set_id": set_id})

@app.route("/api/flashcard/add", methods=["POST"])
def api_flashcard_add():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user:
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    card_id = db.add_flashcard(
        int(data.get("set_id", 0)),
        (data.get("front") or "").strip(),
        (data.get("back") or "").strip(),
        (data.get("hint") or "").strip()
    )
    return jsonify({"success": True, "card_id": card_id})

@app.route("/api/flashcard/answer", methods=["POST"])
def api_flashcard_answer():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user:
        return jsonify({"success": False}), 401
    db.record_flashcard_answer(int(user["user_id"]),
                                int(data.get("card_id", 0)),
                                bool(data.get("correct", False)))
    return jsonify({"success": True})

@app.route("/api/admin/broadcast", methods=["POST"])
def api_admin_broadcast():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return jsonify({"success": False, "error": "Unauthorized"}), 403
    message = (data.get("message") or "").strip()
    target = data.get("target", "all")
    if not message:
        return jsonify({"success": False, "error": "Message required"}), 400
    if target == "premium":
        user_ids = db.get_all_user_ids(status_filter="premium")
    else:
        user_ids = db.get_all_user_ids()
    # Web orqali broadcast — faqat DB ga yozamiz, bot async yuboradi
    broadcast_id = db.create_broadcast(int(user["user_id"]), message, target=target)
    return jsonify({"success": True, "broadcast_id": broadcast_id, "queued": len(user_ids)})

@app.route("/api/admin/ad/create", methods=["POST"])
def api_admin_ad_create():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return jsonify({"success": False, "error": "Unauthorized"}), 403
    days = int(data.get("days", 30))
    now = int(time.time())
    ad_id = db.create_ad(
        title=data.get("title",""), body=data.get("body",""),
        media_id=None, media_type=None, url=data.get("url",""),
        target="all", starts_at=now, ends_at=now + days*86400,
        created_by=int(user["user_id"])
    )
    return jsonify({"success": True, "ad_id": ad_id})

@app.route("/api/admin/ad/toggle", methods=["POST"])
def api_admin_ad_toggle():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return jsonify({"success": False}), 403
    db.toggle_ad(int(data.get("ad_id", 0)), int(data.get("is_active", 0)))
    return jsonify({"success": True})

@app.route("/api/admin/ad/delete", methods=["POST"])
def api_admin_ad_delete():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return jsonify({"success": False}), 403
    db.delete_ad(int(data.get("ad_id", 0)))
    return jsonify({"success": True})

@app.route("/api/admin/security/whitelist/add", methods=["POST"])
def api_whitelist_add():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return jsonify({"success": False}), 403
    ip = (data.get("ip") or "").strip()
    label = (data.get("label") or "").strip()
    if not ip:
        return jsonify({"success": False, "error": "IP required"}), 400
    db.add_ip_whitelist(ip, label, int(user["user_id"]))
    return jsonify({"success": True})

@app.route("/api/admin/security/whitelist/remove", methods=["POST"])
def api_whitelist_remove():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return jsonify({"success": False}), 403
    db.remove_ip_whitelist((data.get("ip") or "").strip())
    return jsonify({"success": True})

@app.route("/api/qbank/import", methods=["POST"])
def api_qbank_import():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user:
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    test_id = data.get("test_id", "").strip()
    test = db.get_test(test_id)
    if not test:
        return jsonify({"success": False, "error": "Test topilmadi"}), 404
    count = db.import_test_to_bank(test_id, int(user["user_id"]))
    return jsonify({"success": True, "count": count})

@app.route("/api/admin/user/ban", methods=["POST"])
def api_admin_ban():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return jsonify({"success": False}), 403
    target_id = int(data.get("user_id", 0))
    reason = (data.get("reason") or "Admin tomonidan ban").strip()
    action = data.get("action", "ban")
    if action == "unban":
        db.unban_user(target_id, int(user["user_id"]), reason)
    else:
        db.ban_user_with_reason(target_id, int(user["user_id"]), reason)
    return jsonify({"success": True})

@app.route("/api/cert/verify/<cert_code>")
def api_cert_verify(cert_code):
    cert = db.get_certificate(cert_code)
    if not cert:
        return jsonify({"valid": False, "error": "Sertifikat topilmadi"})
    return jsonify({"valid": True, "user": cert.get("first_name"), "test": cert.get("title"),
                    "score": float(cert.get("score") or 0), "issued_at": cert.get("issued_at")})

@app.route("/api/temp-link/list/<test_id>")
def api_temp_link_list(test_id):
    token = request.args.get("token")
    user = validate_token(token)
    if not user:
        return jsonify({"success": False}), 401
    links = [dict(l) for l in db.get_test_temp_links(test_id)]
    return jsonify({"success": True, "links": links})

@app.route("/api/temp-link/delete", methods=["POST"])
def api_temp_link_delete():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user:
        return jsonify({"success": False}), 401
    db.delete_temp_link(data.get("link_token", ""))
    return jsonify({"success": True})

@app.route("/api/activity/heatmap")
def api_activity_heatmap():
    user = validate_token(request.args.get("token"))
    if not user:
        return jsonify({"success": False}), 401
    heatmap = db.get_user_activity_heatmap(int(user["user_id"]))
    return jsonify({"success": True, "data": heatmap})

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
        return render_template("create_test.html", token=token, user_id=user_id, chats=eligible_chats, base_url=WEB_BASE_URL, current_user_bg=user.get("custom_bg"), current_lock_bg=user.get("custom_lock_bg"), lang=lang, get_text=get_text, is_edit=False, test=None, questions=[])

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
        logging.error(f"Test yaratish (API) xatosi: {e}")
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
        logging.error(f"O'chirishda xato: {e}")
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
        logging.error(f"Test yechish sahifasi xatosi: {e}")
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

# ══════════════════════════════════════════════════════════════════════
#  🌐  AGENTIC WEB BROWSER  — AI brauzer kabi internetda yuradi
# ══════════════════════════════════════════════════════════════════════

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🌐  AGENTIC WEB BROWSER  (requests + BeautifulSoup)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━



# ── HTML → toza matn + linklar ro'yxati ─────────────────────────────
def _parse_page(html_text: str, base_url: str = "") -> tuple[str, list[dict]]:
    """
    HTML → toza matn + interaktiv elementlar (linklar + ko'zga ko'ringan tugmalar).
    Qaytaradi: (text[:14000], elements[:40])
    """
    from bs4 import BeautifulSoup
    import urllib.parse

    soup = BeautifulSoup(html_text, "lxml")

    for tag in soup(["script", "style", "noscript", "svg",
                     "iframe", "head", "meta", "link"]):
        tag.decompose()

    elements = []
    idx = 1

    # Linklar
    seen_hrefs = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("javascript") or href in ("#", ""):
            continue
        if not href.startswith("http"):
            href = urllib.parse.urljoin(base_url, href)
        if href in seen_hrefs:
            continue
        seen_hrefs.add(href)
        label = a.get_text(" ", strip=True)[:100] or href[:60]
        elements.append({"idx": idx, "type": "link",
                          "label": label, "href": href})
        idx += 1

    # Ko'zga ko'ringan tugmalar (matnli)
    for btn in soup.find_all("button"):
        label = btn.get_text(" ", strip=True)[:80]
        if not label:
            label = btn.get("aria-label", btn.get("title", ""))[:80]
        if label:
            elements.append({"idx": idx, "type": "button",
                              "label": label, "href": None})
            idx += 1

    # Toza matn
    for tag in soup(["nav", "footer", "header", "aside"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    text = re.sub(r'\n{3,}', '\n\n', text)

    return text[:14000], elements[:40]


# ── HTTP sahifani yuklab olish ────────────────────────────────────────
def _fetch_page(url: str, timeout: int = 10) -> tuple[str, list[dict], str]:
    """requests bilan sahifani yuklab, toza matn + elementlar qaytaradi."""
    try:
        hdrs = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "uz,en;q=0.9,ru;q=0.8",
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        }
        resp = requests.get(url, headers=hdrs, timeout=timeout,
                            allow_redirects=True)
        resp.encoding = resp.apparent_encoding or "utf-8"
        text, elements = _parse_page(resp.text, base_url=resp.url)
        return text, elements, resp.url
    except Exception as e:
        return f"[Sahifa yuklanmadi: {e}]", [], url


# ── Elementlar ro'yxatini AI uchun matn formatida ────────────────────
def _elements_to_text(elements: list[dict]) -> str:
    if not elements:
        return ""
    lines = ["\n\n📌 Sahifadagi havolalar va tugmalar:"]
    for el in elements:
        icon = {"link": "🔗", "button": "🔘"}.get(el["type"], "•")
        line = f"  [{el['idx']}] {icon} {el['label']}"
        if el.get("href"):
            line += f"\n       → {el['href'][:80]}"
        lines.append(line)
    return "\n".join(lines)


# ── OpenRouter sinxron chaqiruv ──────────────────────────────────────
def _ai_call(messages: list, ai_headers: dict, model: str,
             max_tokens: int = 600, temperature: float = 0.0) -> str:
    try:
        resp = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers=ai_headers,
            json={"model": model, "messages": messages,
                  "temperature": temperature, "max_tokens": max_tokens},
            timeout=25
        ).json()
        return resp["choices"][0]["message"]["content"].strip()
    except Exception as e:
        return f"[AI xatosi: {e}]"


# ── ASOSIY AGENT LOOP ────────────────────────────────────────────────
def browse_web(query: str, ai_headers: dict, model: str, emit_fn):
    """
    requests + BeautifulSoup asosidagi agentic browsing.
    AI buyruqlari:
      TOPILDI: <javob>     → tayyor, chiqish
      CLICK: <idx>         → [idx] linkni ochish
      LINK: <url>          → yangi URLga o'tish
      SCROLL               → (hozircha keyingi sahifaga o'tish kabi)
      KEYINGISI            → bu sahifada hech narsa yo'q
    Qaytaradi: (answer: str | None, log: list)
    """
    from ddgs import DDGS

    MAX_STEPS = 8
    visited   = set()
    log       = []
    cur_url   = None
    cur_elements = []

    # ── 1. DuckDuckGo qidiruv ──
    emit_fn({"status": f"🔍 DuckDuckGo: \"{query[:50]}\" qidirilmoqda..."})
    try:
        with DDGS() as d:
            raw = list(d.text(query, region="wt-wt",
                              safesearch="moderate", max_results=7))
    except Exception as e:
        emit_fn({"status": f"⚠️ Qidiruv xatosi: {e}"})
        return None, []

    if not raw:
        emit_fn({"status": "⚠️ Qidiruv natijasi yo'q."})
        return None, []

    search_links = [(r["title"][:80], r["href"])
                    for r in raw if r.get("href")]
    emit_fn({"status": f"📋 {len(search_links)} ta havola topildi, AI tanlayapti..."})

    # ── 2. AI eng mos linkni tanlaydi ──
    links_txt = "\n".join(
        f"{i+1}. {t} → {u}" for i, (t, u) in enumerate(search_links)
    )
    choice_raw = _ai_call([
        {"role": "system", "content":
             "Veb-brauzer agentisan. So'rovga eng mos linkni tanla. "
             "FAQAT bitta raqam yoz (1-7). Izohsiz."},
        {"role": "user", "content":
             f"So'rov: \"{query}\"\n\nHavolalar:\n{links_txt}\n\n"
             "Qaysi raqam? Faqat raqam:"}
    ], ai_headers, model, max_tokens=4)

    num = re.search(r'\d', choice_raw)
    idx = (int(num.group()) - 1) if num else 0
    idx = max(0, min(idx, len(search_links) - 1))

    # Navbat: tanlangan birinchi, qolganlari zaxira
    queue = ([search_links[idx][1]]
             + [u for i, (_, u) in enumerate(search_links) if i != idx])

    # ── 3. Agent loop ──
    step = 0

    while step < MAX_STEPS and queue:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)
        cur_url = url
        step += 1

        short = url[:65] + ("..." if len(url) > 65 else "")
        emit_fn({"status": f"🌐 {step}-qadam: sahifaga kirilmoqda..."})
        emit_fn({"status": f"   → {short}"})

        # ── Sahifani yukla ──
        page_text, cur_elements, cur_url = _fetch_page(url)
        chars = len(page_text)
        emit_fn({"status": f"📖 Sahifa o'qildi ({chars:,} belgi, "
                            f"{len(cur_elements)} element)..."})
        log.append({"url": cur_url, "chars": chars,
                    "elements": len(cur_elements)})

        # ── AI: nima qilsin? ──
        emit_fn({"status": "🤔 AI qaror qilmoqda..."})

        elements_txt = _elements_to_text(cur_elements)

        decision = _ai_call([
            {"role": "system", "content": (
                "Sen veb-brauzer agentisan. Sahifa matnini o'qib, "
                "foydalanuvchi so'roviga javob topishga harakat qilasan.\n\n"
                "Buyruqlar (FAQAT bittasini yoz):\n"
                "  TOPILDI: <to'liq javob matni>\n"
                "  CLICK: <raqam>   → o'sha havolani ochish\n"
                "  LINK: <to'liq URL>\n"
                "  KEYINGISI        → bu sahifada hech narsa yo'q\n\n"
                "MUHIM: Sahifada so'rovga oid ma'lumot bo'lsa — "
                "TOPILDI deb to'liq javob yoz."
            )},
            {"role": "user", "content": (
                f"So'rov: \"{query}\"\n"
                f"Joriy URL: {cur_url}\n\n"
                f"Sahifa matni:\n{page_text[:11000]}"
                f"{elements_txt}"
            )}
        ], ai_headers, model, max_tokens=1800, temperature=0.1)

        # ── Qarorni parse qilish ──
        if decision.upper().startswith("TOPILDI:"):
            answer = decision[len("TOPILDI:"):].strip()
            emit_fn({"status": f"✅ Javob topildi! "
                               f"({step} qadam, {chars:,} belgi o'qildi)"})
            return answer, log

        elif decision.upper().startswith("CLICK:"):
            raw_idx = decision[len("CLICK:"):].strip().split()[0]
            m = re.search(r'\d+', raw_idx)
            if m:
                el_idx = int(m.group())
                el = next((e for e in cur_elements if e["idx"] == el_idx), None)
                if el and el.get("href") and el["href"] not in visited:
                    emit_fn({"status": f"🔗 AI link tanladi: \"{el['label'][:50]}\"..."})
                    queue.insert(0, el["href"])
                else:
                    emit_fn({"status": f"⚠️ [{el_idx}] element topilmadi yoki avval ko'rilgan..."})

        elif decision.upper().startswith("LINK:"):
            next_url = decision[len("LINK:"):].strip().split()[0]
            if next_url not in visited:
                emit_fn({"status": f"🔗 Yangi URL → {next_url[:55]}..."})
                queue.insert(0, next_url)
            else:
                emit_fn({"status": "↩️ Bu URL avval ko'rilgan, keyingisi..."})

        else:  # KEYINGISI yoki noaniq
            emit_fn({"status": f"➡️ Bu sahifada ma'lumot yo'q "
                               f"({step}/{MAX_STEPS}), keyingisi..."})

    emit_fn({"status": f"⚠️ {step} qadam bajarildi — aniq javob topilmadi. "
                       "AI o'z bilimidan javob beradi..."})
    return None, log
    """
    HTML dan:
      • toza matn  (script/style/nav olib tashlanadi)
      • interaktiv elementlar ro'yxati qaytaradi:
          [{"idx": 1, "type": "link"|"button"|"input"|"select",
            "label": "...", "href": "..."|None, "selector": "..."}]
    """
    from bs4 import BeautifulSoup
    import urllib.parse

    soup = BeautifulSoup(html, "lxml")

    # Keraksiz taglarni o'chirish
    for tag in soup(["script", "style", "noscript", "svg",
                     "iframe", "head", "meta", "link"]):
        tag.decompose()

    elements = []
    idx = 1

    # 1. <a href> — linklar
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("javascript") or href == "#":
            continue
        if not href.startswith("http"):
            href = urllib.parse.urljoin(base_url, href)
        label = a.get_text(" ", strip=True)[:100] or href[:60]
        elements.append({
            "idx": idx, "type": "link",
            "label": label, "href": href,
            "selector": None
        })
        idx += 1

    # 2. <button> — JS tugmalar
    for btn in soup.find_all("button"):
        label = btn.get_text(" ", strip=True)[:80]
        if not label:
            label = btn.get("aria-label", btn.get("title", "tugma"))[:80]
        if not label:
            continue
        # CSS selector (id > class > tag)
        sel = _make_selector(btn)
        elements.append({
            "idx": idx, "type": "button",
            "label": label, "href": None,
            "selector": sel
        })
        idx += 1

    # 3. <input type=submit|button|search|text|email|password>
    for inp in soup.find_all("input"):
        itype = (inp.get("type") or "text").lower()
        if itype in ("hidden", "checkbox", "radio", "file", "image"):
            continue
        label = (inp.get("placeholder") or inp.get("aria-label") or
                 inp.get("name") or inp.get("id") or itype)[:80]
        sel = _make_selector(inp)
        elements.append({
            "idx": idx,
            "type": "submit" if itype in ("submit", "button") else "input",
            "label": label, "href": None,
            "selector": sel
        })
        idx += 1

    # 4. <select> — dropdown
    for sel_tag in soup.find_all("select"):
        label = (sel_tag.get("aria-label") or sel_tag.get("name") or
                 sel_tag.get("id") or "dropdown")[:80]
        options = [o.get_text(strip=True)[:50]
                   for o in sel_tag.find_all("option")][:8]
        sel = _make_selector(sel_tag)
        elements.append({
            "idx": idx, "type": "select",
            "label": f"{label} [{', '.join(options)}]",
            "href": None, "selector": sel
        })
        idx += 1

    # Toza matn
    for tag in soup(["nav", "footer", "header", "aside", "ads"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    text = re.sub(r'\n{3,}', '\n\n', text)

    return text[:14000], elements[:40]   # max 14k belgi, 40 element

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
Senga qo'yilgan qoidalarga QAT'IY amal qilishing SHART.

😊 MULOQOT USLUBI — ENG MUHIM:
- Foydalanuvchiga DOIM iliq, do'stona va qiziqarli tarzda murojaat qil
- Har bir javobda kamida 2-3 ta emoji ishlat — his-tuyg'ularni ifodalash uchun
- Javobni hayajon, qiziqish va mehr bilan yoz — robotdek emas, insoniylik bilan
- Qisqa savollarga ham qisqacha lekin chiroyli javob ber
- Foydalanuvchi muvaffaqiyatga erishsa — quvon, xato qilsa — rag'batlantir
- Agar savol aniq bo'lmasa — "Sal aniqroq aytib bera olasizmi? 🤔" deb so'ra
- Javob oxirida qo'shimcha savol yoki taklif bilan tugat (agar mos bo'lsa)

Misol uslub:
❌ Yomon: "Python da list yaratish uchun [] ishlatiladi."
✅ Yaxshi: "Zo'r savol! 🎉 Python da ro'yxat (list) yaratish juda oson — shunchaki
  kvadrat qavslar `[]` ichiga elementlarni yoz. Masalan: `mevalar = ['olma', 'nok', 'shaftoli']` 🍎
  Ko'proq bilib olishni xohlaysizmi? 😊"

📝 JAVOB FORMATI:
- Sarlavhalar: # Katta, ## O'rta, ### Kichik
- Qalin: **matn**, kursiv: *matn*, chizilgan: ~~matn~~
- Inline kod: `kod`, havola: [matn](url)
- Kod bloki MAJBURIY (dasturlash, SQL, buyruq uchun):
  ```python
  print("Salom!")
  ```
- Tilni aniq ko'rsat: python, javascript, sql, html, bash va h.k.
- Ro'yxat: - element yoki 1. element
- Iqtibos: > matn
- Jadval: | Ustun1 | Ustun2 |
- Ajratuvchi: ---

🔴 INTERNET QIDIRUVI BUYRUG'I:
Agar foydalanuvchi so'nggi yangilik, hozirgi vaqt ma'lumoti, haqiqiy fakt,
sport natijasi, ob-havo, kurs, narx yoki boshqa real-vaqt ma'lumot so'rasa —
FAQAT quyidagi formatda yoz:
/interdan_qidirish [inglizcha yoki o'zbekcha qidiruv so'zi]

Misol:
- "O'zbekiston prezidenti kim?" → /interdan_qidirish O'zbekiston prezidenti 2024
- "Dollar kursi?" → /interdan_qidirish dollar kursi O'zbekiston bugun
- "Real Madrid oxirgi o'yini?" → /interdan_qidirish Real Madrid last match result 2024

🚫 MUTLAQ TAQIQ — bu gaplarni HECH QACHON yozma:
- "men internetga chiqa olmayman"
- "real vaqtda ma'lumot ololmayman"
- "ma'lumotlarim ...gacha"
- "internetga ulanishim yo'q"
- "BBC, Gazeta.uz, Kun.uz kabi saytlarga o'ting"
Bunday o'rniga — DOIM /interdan_qidirish buyrug'ini ishlat!

✅ ODDIY SAVOLLARDA:
O'quv, ta'lim, matematika, tarix, ilm-fan, til, kod yozish va boshqa bilim
sohasidagi savollarga — to'g'ridan to'g'ri javob ber, /interdan_qidirish ishlatma."""

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
        def emit(data: dict):
            """JSON chunk yuboradi va darhol flushlanadi."""
            yield json.dumps(data, ensure_ascii=False) + "\n"

        try:
            yield from emit({"status": "💬 So'rovingiz qabul qilindi..."})

            history_len = len([m for m in messages if m["role"] != "system"])
            if history_len > 2:
                yield from emit({"status": f"📂 Suhbat tarixi yuklandi ({history_len} ta xabar)..."})
            else:
                yield from emit({"status": "📂 Yangi suhbat boshlandi..."})

            word_count = len(text.split())
            if word_count > 30:
                yield from emit({"status": f"📝 Uzun matn tahlil qilinmoqda ({word_count} so'z)..."})
            else:
                yield from emit({"status": "📝 Savol tahlil qilinmoqda..."})

            yield from emit({"status": "🔍 Internet qidiruv kerakmi — aniqlanmoqda..."})
            yield from emit({"status": f"🤖 AI modeliga ({MODEL_NAME.split('/')[-1]}) so'rov yuborilmoqda..."})

            t_api_start = time.time()
            api_response = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers=headers, json=payload, timeout=15
            )
            t_api_elapsed = round(time.time() - t_api_start, 1)
            yield from emit({"status": f"⚡ AI javob yaratdi ({t_api_elapsed}s)..."})

            res_data = api_response.json()

            if "choices" in res_data and len(res_data["choices"]) > 0:
                ai_initial_reply = res_data["choices"][0]["message"]["content"].strip()
                final_reply = ai_initial_reply

                if "/interdan_qidirish" in ai_initial_reply:
                    query_part = ai_initial_reply.split("/interdan_qidirish")[-1].split('\n')[0].strip()
                    query_part = query_part.replace('"', '').replace("'", "").strip()

                    yield from emit({"status": f"🌐 Brauzer agenti ishga tushdi: \"{query_part[:45]}\"..."})

                    import threading
                    import queue as _queue_mod

                    # Thread-safe queue: browse_web statuslarini oqimga uzatish uchun
                    status_q   = _queue_mod.Queue()
                    browse_ans = [None]   # [answer_str | None]
                    _SENTINEL  = object()  # thread tugaganini bildiruvchi marker

                    def _emit_fn(d):
                        """browse_web thread ichidan chaqiriladi, statusni queue ga qo'yadi."""
                        status_q.put(d)

                    def _run_browse():
                        try:
                            ans, _ = browse_web(query_part, headers, MODEL_NAME, _emit_fn)
                            browse_ans[0] = ans
                        except Exception as ex:
                            status_q.put({"status": f"⚠️ Brauzer xatosi: {ex}"})
                        finally:
                            status_q.put(_SENTINEL)  # ishni tugadik

                    t_browse = threading.Thread(target=_run_browse, daemon=True)
                    t_browse.start()

                    # Generator: queue dan statuslarni real-time stream qilamiz
                    while True:
                        try:
                            item = status_q.get(timeout=30)  # max 30s kutish
                        except _queue_mod.Empty:
                            # 30 soniya o'tdi — timeout
                            yield from emit({"status": "⚠️ Brauzer 30s da javob bermadi, to'xtatildi..."})
                            break
                        if item is _SENTINEL:
                            break   # thread tugadi
                        yield from emit(item)

                    t_browse.join(timeout=2)  # thread tamom bo'lishini kutamiz
                    answer = browse_ans[0]

                    if answer:
                        final_reply = answer
                    else:
                        # Topilmadi — AI o'zidan javob bersin
                        yield from emit({"status": "🤖 AI o'z bilimi asosida javob tayyorlamoqda..."})
                        messages.append({"role": "assistant", "content": ai_initial_reply})
                        messages.append({
                            "role": "user",
                            "content": (
                                f"Internet sahifalarida '{query_part}' bo'yicha aniq ma'lumot topilmadi. "
                                "O'zing bilgan ma'lumotlar asosida imkon qadar to'liq javob ber."
                            )
                        })
                        payload["messages"] = messages
                        t2_start = time.time()
                        resp2 = requests.post(
                            "https://openrouter.ai/api/v1/chat/completions",
                            headers=headers, json=payload, timeout=20
                        ).json()
                        t2_elapsed = round(time.time() - t2_start, 1)
                        yield from emit({"status": f"💡 AI javobi tayyor ({t2_elapsed}s)..."})
                        final_reply = resp2["choices"][0]["message"]["content"]

                else:
                    reply_len = len(ai_initial_reply.split())
                    yield from emit({"status": f"💡 AI javob yaratdi ({reply_len} so'z)..."})

                yield from emit({"status": "🎨 Javob formatlashtirilmoqda..."})

                # Markdown render frontend (ai_chat.html) tomonida bajariladi.
                # Backend faqat xom matnni saqlaydi.
                reply_formatted = final_reply

                final_len = len(reply_formatted.split())
                if final_len > 100:
                    yield from emit({"status": f"📊 Katta javob tayyorlandi ({final_len} so'z)..."})

                yield from emit({"status": "💾 Javob bazaga saqlanmoqda..."})

                with db._conn() as c:
                    c.execute(
                        "INSERT INTO ai_chat_history (user_id, session_id, role, content, created_at) "
                        "VALUES (%s, %s, 'assistant', %s, %s)",
                        (user_id, session_id, reply_formatted, int(time.time()))
                    )
                    c.execute("COMMIT")
                    c.execute(
                        "SELECT id FROM ai_chat_history WHERE user_id=%s ORDER BY id DESC LIMIT 1",
                        (user_id,)
                    )
                    row = to_dict(c.fetchone())
                    ai_msg_id = row.get("id", int(time.time())) if row else int(time.time())

                yield from emit({"status": "🗂️ Suhbat tarixi yangilandi..."})
                yield from emit({"status": "🖥️ Javob ekranga chiqarilmoqda..."})
                yield from emit({"reply": reply_formatted, "id": ai_msg_id, "session_id": session_id})

            else:
                real_error = res_data.get("error", {}).get("message", "Noma'lum API xatosi")
                yield from emit({"error": f"OpenRouter: {real_error}"})

        except requests.exceptions.Timeout:
            yield from emit({"error": "⏱️ Server 15 soniya ichida javob bermadi. Qayta urinib ko'ring."})
        except requests.exceptions.ConnectionError:
            yield from emit({"error": "🌐 AI serveriga ulanib bo'lmadi. Internetni tekshiring."})
        except requests.exceptions.RequestException as e:
            yield from emit({"error": f"Tarmoq xatosi: {str(e)}"})
        except Exception as e:
            yield from emit({"error": f"Ichki xato: {str(e)}"})

    return Response(
        stream_with_context(generate()),
        mimetype='application/x-ndjson',
        headers={
            'X-Accel-Buffering': 'no',   # nginx bufferni o'chiradi
            'Cache-Control': 'no-cache',
            'Transfer-Encoding': 'chunked',
        }
    )

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



@app.route("/api/leaderboard")
def api_leaderboard():
    token = request.args.get("token")
    user = validate_token(token)
    if not user: return jsonify({"success": False, "error": "Unauthorized"}), 401
    try:
        top_100, user_info = db.get_current_month_leaderboard(user["user_id"])
        return jsonify({"success": True, "top": top_100, "user": user_info})
    except Exception as e:
        logging.error(f"Leaderboard API xatosi: {e}")
        return jsonify({"success": False, "error": "Server xatosi"}), 500

# ==========================================
# 📄 TASK 2: PAGINATION API
# ==========================================
@app.route("/api/my-tests")
def api_my_tests():
    """Sahifalash bilan foydalanuvchi testlarini qaytaradi"""
    token = request.args.get("token")
    user = validate_token(token)
    if not user: return jsonify({"success": False, "error": "Unauthorized"}), 401
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 10))
    try:
        tests, total = db.get_tests_paginated(int(user["user_id"]), page, limit)
        return jsonify({
            "success": True, "tests": tests, "total": total,
            "page": page, "limit": limit,
            "has_more": (page * limit) < total
        })
    except Exception as e:
        logging.error(f"api_my_tests xato: {e}")
        return jsonify({"success": False, "error": "Server xatosi"}), 500

@app.route("/api/public-tests")
def api_public_tests():
    """Ommaviy testlar — sahifalash + filter"""
    token = request.args.get("token")
    user = validate_token(token)
    if not user: return jsonify({"success": False}), 401
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 12))
    query = request.args.get("q", "").strip() or None
    cat = request.args.get("category", type=int)
    try:
        tests, total = db.get_public_tests_paginated(query, cat, page, limit)
        return jsonify({
            "success": True, "tests": tests, "total": total,
            "page": page, "has_more": (page * limit) < total
        })
    except Exception as e:
        logging.error(f"api_public_tests xato: {e}")
        return jsonify({"success": False, "error": "Server xatosi"}), 500

# ==========================================
# 📡 TASK 3: REAL-TIME NATIJALAR (SSE)
# ==========================================
@app.route("/api/results-stream/<test_id>")
def results_stream(test_id):
    """Server-Sent Events orqali real-time natijalar"""
    token = request.args.get("token")
    user = validate_token(token)
    if not user: return abort(401)

    def generate():
        last_count = -1
        tries = 0
        while tries < 60:  # max 5 daqiqa (5s * 60)
            try:
                results = db.all_results(test_id)
                cur_count = len(results)
                if cur_count != last_count:
                    last_count = cur_count
                    data = []
                    test = db.get_test(test_id)
                    scoring = dict(test).get("scoring_type", "standard") if test else "standard"
                    for i, r in enumerate(results, 1):
                        r = dict(r)
                        name = (r.get("username") and f"@{r['username']}") or \
                               f"{r.get('first_name','')}{r.get('last_name','')}".strip() or \
                               f"User{r.get('user_id','')}"
                        data.append({
                            "rank": i, "name": name,
                            "score": float(r.get("score") or 0),
                            "duration": int(r.get("duration_sec") or 0),
                            "user_id": r.get("user_id")
                        })
                    yield f"data: {json.dumps({'count': cur_count, 'results': data})}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'error': str(e)})}\n\n"
                break
            import time as _time; _time.sleep(5)
            tries += 1
        yield "data: {\"done\": true}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )

@app.route("/live-results/<test_id>")
def live_results_page(test_id):
    """Test natijalari real-time sahifasi"""
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user: return abort(401)
    test = db.get_test(test_id)
    if not test: return abort(404)
    test = dict(test)
    title = html.escape(test.get("title", "Test"))

    page = f"""<!DOCTYPE html>
<html lang="uz">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>📡 {title} — Real-time</title>
  <style>
    *{{margin:0;padding:0;box-sizing:border-box;font-family:-apple-system,sans-serif}}
    body{{background:#0f172a;color:#f3f6ff;padding:20px;min-height:100vh}}
    h1{{font-size:20px;margin-bottom:6px;color:#38d39f}}
    .sub{{color:#718096;font-size:13px;margin-bottom:20px}}
    .badge{{display:inline-block;background:rgba(56,211,159,.2);color:#38d39f;padding:4px 12px;border-radius:999px;font-size:13px;margin-bottom:16px}}
    .row{{display:flex;align-items:center;justify-content:space-between;padding:12px 16px;background:rgba(255,255,255,.06);border-radius:12px;margin-bottom:8px;border:1px solid rgba(255,255,255,.08);animation:fadeIn .3s ease}}
    .row.me{{background:rgba(56,211,159,.1);border-color:rgba(56,211,159,.3)}}
    @keyframes fadeIn{{from{{opacity:0;transform:translateY(6px)}}to{{opacity:1;transform:translateY(0)}}}}
    .medal{{font-size:20px;width:32px;flex-shrink:0}}
    .name{{flex:1;padding:0 12px;font-size:15px}}
    .score{{font-weight:700;color:#38d39f;font-size:16px}}
    .dur{{color:#718096;font-size:12px;margin-top:2px}}
    #dot{{display:inline-block;width:8px;height:8px;border-radius:50%;background:#38d39f;margin-right:6px;animation:pulse 1.5s infinite}}
    @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
    .back{{display:inline-block;color:#6cb2ff;font-size:14px;margin-bottom:16px;text-decoration:none}}
  </style>
</head>
<body>
  <a class="back" href="/test/{test_id}?token={token}">← Orqaga</a>
  <h1>📡 {title}</h1>
  <p class="sub">Real-time natijalar</p>
  <div class="badge"><span id="dot"></span><span id="cnt">0</span> ta qatnashchi</div>
  <div id="list"></div>
  <script>
    const myId = {int(user["user_id"])};
    const medals = ["🥇","🥈","🥉"];
    const src = new EventSource("/api/results-stream/{test_id}?token={token}");
    src.onmessage = (e) => {{
      const d = JSON.parse(e.data);
      if(d.done){{ src.close(); document.getElementById('dot').style.animation='none'; document.getElementById('dot').style.background='#718096'; return; }}
      if(d.error)return;
      document.getElementById('cnt').textContent = d.count;
      const list = document.getElementById('list');
      list.innerHTML = '';
      d.results.forEach(r => {{
        const medal = medals[r.rank-1] || r.rank+'.';
        const me = r.user_id == myId ? ' me' : '';
        const mins = Math.floor(r.duration/60), secs = r.duration%60;
        const durStr = mins>0 ? mins+'d '+secs+'s' : secs+'s';
        list.innerHTML += `<div class="row${{me}}"><div class="medal">${{medal}}</div><div class="name">${{r.name}}</div><div><div class="score">${{r.score}}</div><div class="dur">⏱ ${{durStr}}</div></div></div>`;
      }});
    }};
  </script>
</body>
</html>"""
    return page

# ==========================================
# 📥 TASK 4: TEST IMPORT (Excel/CSV)
# ==========================================
@app.route("/import-test", methods=["GET", "POST"])
def import_test():
    """Excel yoki CSV fayldan test yaratish"""
    token = request.args.get("token") or request.form.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user: return abort(401)
    uid = int(user["user_id"])

    if request.method == "GET":
        chats = [c for c in db.chats_for_user(uid) if c.get("bot_is_admin")]
        chat_opts = "".join([
            f'<option value="{c["chat_id"]}">{html.escape(c.get("title",""))}</option>'
            for c in chats
        ])
        return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Test Import</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0;font-family:-apple-system,sans-serif}}
body{{background:#0f172a;color:#f3f6ff;padding:20px;min-height:100vh}}
.wrap{{max-width:600px;margin:0 auto}}
.card{{background:rgba(255,255,255,.07);border:1px solid rgba(255,255,255,.12);border-radius:14px;padding:22px;margin-bottom:18px}}
h1{{font-size:22px;margin-bottom:4px;color:#38d39f}}
.sub{{color:#718096;font-size:13px;margin-bottom:20px}}
label{{display:block;font-size:13px;color:#a0aec0;margin-bottom:6px;margin-top:14px}}
input,select{{width:100%;padding:11px;border-radius:8px;border:1px solid rgba(255,255,255,.15);background:rgba(0,0,0,.3);color:#f3f6ff;font-size:14px}}
.btn{{background:#38d39f;color:#000;border:none;padding:13px 24px;border-radius:10px;font-weight:700;font-size:15px;cursor:pointer;width:100%;margin-top:16px}}
.hint{{background:rgba(108,178,255,.1);border:1px solid rgba(108,178,255,.2);border-radius:10px;padding:14px;margin-top:14px;font-size:13px;color:#6cb2ff;line-height:1.7}}
a{{color:#6cb2ff;font-size:14px}}
</style></head>
<body><div class="wrap">
<a href="/?token={token}">← Orqaga</a>
<br><br>
<h1>📥 Test Import</h1>
<p class="sub">Excel (.xlsx) yoki CSV (.csv) fayldan test yarating</p>
<div class="card">
<form method="POST" enctype="multipart/form-data">
  <input type="hidden" name="token" value="{token}">
  <label>Test fayli (.xlsx yoki .csv)</label>
  <input type="file" name="file" accept=".xlsx,.csv" required>
  <label>Test nomi (ixtiyoriy, fayldan olinadi)</label>
  <input type="text" name="title" placeholder="Masalan: Tarix testi">
  <label>Guruh/kanal</label>
  <select name="chat_id">
    <option value="{uid}">🔒 Private (faqat men)</option>
    {chat_opts}
  </select>
  <button class="btn" type="submit">📥 Import qilish</button>
</form>
<div class="hint">
  <b>📋 Excel format (A-E ustunlar):</b><br>
  A: Savol matni<br>
  B: 1-variant<br>
  C: 2-variant<br>
  D: 3-variant (ixtiyoriy)<br>
  E: 4-variant (ixtiyoriy)<br>
  F: To'g'ri javob (1, 2, 3 yoki 4)<br><br>
  <b>CSV format:</b> vergul bilan ajratilgan, xuddi shu tartibda
</div>
</div>
</div></body></html>"""

    # POST — faylni qayta ishlash
    file = request.files.get("file")
    if not file or file.filename == "":
        return redirect(f"/import-test?token={token}&msg=Fayl tanlanmadi")
    title_override = request.form.get("title", "").strip()
    try:
        chat_id = int(request.form.get("chat_id", uid))
    except Exception:
        chat_id = uid

    ext = file.filename.rsplit(".", 1)[-1].lower()
    questions = []
    title = title_override or "Import test"

    try:
        if ext == "xlsx":
            from openpyxl import load_workbook
            wb = load_workbook(file)
            ws = wb.active
            if not title_override and ws.title and ws.title != "Sheet":
                title = ws.title
            for row in ws.iter_rows(min_row=2, values_only=True):
                if not row[0]: continue
                q_text = str(row[0]).strip()
                opts = [str(row[i]).strip() for i in range(1, 5) if i < len(row) and row[i]]
                try:
                    correct_idx = int(row[5]) - 1 if len(row) > 5 and row[5] else 0
                except Exception:
                    correct_idx = 0
                if q_text and len(opts) >= 2:
                    questions.append({"question": q_text, "options": opts,
                                      "correct_index": max(0, min(correct_idx, len(opts)-1))})
        elif ext == "csv":
            import csv, io
            content = file.read().decode("utf-8-sig")
            reader = csv.reader(io.StringIO(content))
            next(reader, None)  # header
            for row in reader:
                if not row or not row[0].strip(): continue
                q_text = row[0].strip()
                opts = [row[i].strip() for i in range(1, 5) if i < len(row) and row[i].strip()]
                try:
                    correct_idx = int(row[5]) - 1 if len(row) > 5 and row[5].strip() else 0
                except Exception:
                    correct_idx = 0
                if q_text and len(opts) >= 2:
                    questions.append({"question": q_text, "options": opts,
                                      "correct_index": max(0, min(correct_idx, len(opts)-1))})
    except Exception as e:
        logging.error(f"Import parse xato: {e}")
        return redirect(f"/import-test?token={token}&msg=Faylni o'qishda xato: {e}")

    if not questions:
        return redirect(f"/import-test?token={token}&msg=Savollar topilmadi. Format to'g'riligini tekshiring.")

    test_id = uuid.uuid4().hex[:10]
    db.create_test(test_id, uid, chat_id, title, 60, int(time.time()))
    for i, q in enumerate(questions):
        db.add_question(test_id, i, q["question"], q["options"], q["correct_index"])

    manage_url = f"/test/{test_id}?token={token}"
    return redirect(f"{manage_url}&msg=✅ {len(questions)} ta savol import qilindi!")

# ==========================================
# 📤 TASK 4b: CSV EXPORT
# ==========================================
@app.route("/export-csv/<test_id>")
def export_csv(test_id):
    """Test natijalarini CSV sifatida yuklab olish"""
    token = request.args.get("token")
    user = validate_token(token)
    if not user: return abort(401)
    test = db.get_test(test_id)
    if not test: return abort(404)
    test = dict(test)
    if int(test.get("owner_user_id", 0)) != int(user["user_id"]) and int(user["user_id"]) not in SUPERADMINS:
        return abort(403)

    import csv, io
    allr = db.all_results(test_id)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["O'rin", "Foydalanuvchi", "Username", "Ball", "Vaqt (soniya)"])
    for i, r in enumerate(allr, 1):
        r = dict(r)
        name = f"{r.get('first_name','')} {r.get('last_name','')}".strip() or f"User{r.get('user_id','')}"
        writer.writerow([i, name, r.get("username", ""), float(r.get("score") or 0), int(r.get("duration_sec") or 0)])

    filename = f"{(test.get('title') or 'test')[:40]}_natijalar.csv"
    return Response(
        output.getvalue(),
        mimetype="text/csv; charset=utf-8-sig",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

# ==========================================
# 🔲 QR KOD — Python serverida generatsiya (CDN kerak emas)
# ==========================================

def _generate_qr_svg(data: str, size: int = 300) -> str:
    """
    Tashqi kutubxonasiz oddiy QR kod SVG generatsiyasi.
    qrcode kutubxonasi o'rnatilgan bo'lsa — undan foydalanadi,
    o'rnatilmagan bo'lsa — fallback sifatida Google Charts API ishlatadi.
    """
    # 1-usul: qrcode kutubxonasi (PythonAnywhere'da bo'lishi mumkin)
    try:
        import qrcode
        import qrcode.image.svg
        import io
        factory = qrcode.image.svg.SvgImage
        qr = qrcode.make(data, image_factory=factory, box_size=10, border=4)
        buf = io.BytesIO()
        qr.save(buf)
        return buf.getvalue().decode('utf-8')
    except ImportError:
        pass

    # 2-usul: segno kutubxonasi
    try:
        import segno
        import io
        qr = segno.make(data)
        buf = io.StringIO()
        qr.save(buf, kind='svg', scale=8, border=4)
        return buf.getvalue()
    except ImportError:
        pass

    # 3-usul: Google Charts API orqali (img tag)
    return None


@app.route("/qr/<test_id>")
def test_qr(test_id):
    """Test uchun QR kod sahifasi"""
    token = request.args.get("token")
    user = validate_token(token)
    if not user: return abort(401)
    test = db.get_test(test_id)
    if not test: return abort(404)
    test = dict(test)

    bot_username = get_bot_username()
    deep_link = f"https://t.me/{bot_username}?start=test_{test_id}"
    title = html.escape(test.get("title", "Test"))
    pub_name = test.get("public_name")
    solve_url = f"{WEB_BASE_URL.rstrip('/')}/solve/{test_id}?token={token}" if pub_name else deep_link
    url_escaped = html.escape(solve_url)

    # QR SVG generatsiya qilish
    svg_content = _generate_qr_svg(solve_url)

    if svg_content:
        # Python kutubxonasi orqali generatsiya qilindi
        qr_html = f"""
        <div id="qr-box" style="background:#fff;padding:16px;border-radius:16px;display:inline-block">
            {svg_content}
        </div>"""
        download_btn = f"""
        <button class="btn" onclick="downloadQR()" style="margin-top:16px">
            📥 Yuklab olish (SVG)
        </button>
        <script>
        function downloadQR() {{
            const svg = document.querySelector('#qr-box svg');
            if (!svg) return;
            const blob = new Blob([svg.outerHTML], {{type: 'image/svg+xml'}});
            const a = document.createElement('a');
            a.href = URL.createObjectURL(blob);
            a.download = 'qr_{test_id}.svg';
            a.click();
        }}
        </script>"""
    else:
        # Fallback: Google Charts API
        gc_url = f"https://chart.googleapis.com/chart?chs=300x300&cht=qr&chl={requests.utils.quote(solve_url)}&choe=UTF-8"
        qr_html = f"""
        <div id="qr-box" style="background:#fff;padding:16px;border-radius:16px;display:inline-block">
            <img src="{html.escape(gc_url)}" alt="QR kod" width="280" height="280"
                 style="display:block"
                 onerror="this.parentElement.innerHTML='<p style=color:#f56565>QR kod yuklanmadi. URL ni nusxalab oling.</p>'">
        </div>"""
        download_btn = f"""
        <a href="{html.escape(gc_url)}" download="qr_{test_id}.png" class="btn" style="margin-top:16px;display:inline-block;text-decoration:none">
            📥 Yuklab olish
        </a>"""

    page = f"""<!DOCTYPE html>
<html><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>QR — {title}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0;font-family:-apple-system,sans-serif}}
body{{background:#0f172a;color:#f3f6ff;display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:100vh;padding:20px;text-align:center}}
h2{{margin-bottom:8px;color:#38d39f;font-size:20px}}
.sub{{color:#718096;font-size:13px;margin-bottom:24px}}
.link{{margin-top:18px;padding:12px 16px;background:rgba(255,255,255,.07);border:1px solid rgba(255,255,255,.1);border-radius:10px;font-size:12px;color:#6cb2ff;word-break:break-all;max-width:340px;cursor:pointer}}
.btn{{background:#38d39f;color:#000;border:none;padding:12px 24px;border-radius:10px;font-weight:700;cursor:pointer;font-size:14px}}
.back{{color:#6cb2ff;font-size:14px;margin-top:18px;text-decoration:none;display:block}}
.copy-hint{{font-size:11px;color:#718096;margin-top:6px}}
</style>
</head>
<body>
<a class="back" href="/test/{test_id}?token={token}">← Orqaga</a>
<br><br>
<h2>🔲 {title}</h2>
<p class="sub">QR kodni skanerlang yoki havolani nusxalab oling</p>

{qr_html}

{download_btn}

<div class="link" onclick="copyUrl()" title="Bosib nusxalash">
    {url_escaped}
</div>
<div class="copy-hint">👆 Bosib nusxalash</div>

<a class="back" href="{url_escaped}">🔗 Havolani ochish</a>

<script>
function copyUrl() {{
    navigator.clipboard.writeText({json.dumps(solve_url)}).then(() => {{
        const el = document.querySelector('.link');
        const old = el.textContent;
        el.style.color = '#68d391';
        el.textContent = '✅ Nusxalandi!';
        setTimeout(() => {{ el.style.color = '#6cb2ff'; el.textContent = old; }}, 2000);
    }}).catch(() => {{
        // Eski brauzer fallback
        const ta = document.createElement('textarea');
        ta.value = {json.dumps(solve_url)};
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
        alert('✅ Nusxalandi!');
    }});
}}
</script>
</body></html>"""
    return page

# ==========================================
# 🎨 TASK 6: DARK/LIGHT TEMA SWITCH
# ==========================================
@app.route("/api/theme/toggle", methods=["POST"])
def toggle_theme():
    """Foydalanuvchi temasini saqlash"""
    data = request.json or {}
    token = data.get("token")
    user = validate_token(token)
    if not user: return jsonify({"success": False}), 401
    theme = data.get("theme", "dark")
    if theme not in ("dark", "light"):
        return jsonify({"success": False, "error": "Invalid theme"}), 400
    session[f"theme_{user['user_id']}"] = theme
    try:
        with db._conn() as c:
            # users jadvaliga theme ustuni qo'shish (migration)
            try:
                c.execute("ALTER TABLE users ADD COLUMN theme VARCHAR(10) DEFAULT 'dark'")
            except Exception:
                pass
            c.execute("UPDATE users SET theme=%s WHERE user_id=%s", (theme, user["user_id"]))
    except Exception as e:
        logging.error(f"Theme save xato: {e}")
    return jsonify({"success": True, "theme": theme})

@app.route("/api/theme/get")
def get_theme():
    """Foydalanuvchi temasini olish"""
    token = request.args.get("token")
    user = validate_token(token)
    if not user: return jsonify({"theme": "dark"})
    uid = int(user["user_id"])
    theme = session.get(f"theme_{uid}", "dark")
    try:
        with db._conn() as c:
            row = c.execute("SELECT theme FROM users WHERE user_id=%s", (uid,)).fetchone()
            if row and dict(row).get("theme"):
                theme = dict(row)["theme"]
    except Exception:
        pass
    return jsonify({"theme": theme})

# ==========================================
# 📅 TASK 9: SCHEDULED TESTS WEB API
# ==========================================
@app.route("/api/schedule-test", methods=["POST"])
def api_schedule_test():
    """Testni ma'lum vaqtda ochish uchun rejalashtirish"""
    data = request.json or {}
    token = data.get("token")
    user = validate_token(token)
    if not user: return jsonify({"success": False, "error": "Unauthorized"}), 401
    test_id = data.get("test_id")
    open_at_str = data.get("open_at", "").strip()  # "DD/MM/YYYY HH:MM"
    if not test_id or not open_at_str:
        return jsonify({"success": False, "error": "test_id va open_at kerak"}), 400
    test = db.get_test(test_id)
    if not test: return jsonify({"success": False, "error": "Test topilmadi"}), 404
    test = dict(test)
    if int(test.get("owner_user_id", 0)) != int(user["user_id"]) and int(user["user_id"]) not in SUPERADMINS:
        return jsonify({"success": False, "error": "Ruxsat yo'q"}), 403
    try:
        from datetime import datetime
        open_dt = datetime.strptime(open_at_str, "%d/%m/%Y %H:%M")
        open_ts = int(open_dt.replace(tzinfo=TZ).timestamp())
    except Exception:
        return jsonify({"success": False, "error": "Vaqt formati noto'g'ri (DD/MM/YYYY HH:MM)"}), 400
    if open_ts <= int(time.time()):
        return jsonify({"success": False, "error": "Vaqt o'tib ketgan"}), 400
    db.schedule_test_open(test_id, open_ts)
    return jsonify({"success": True, "open_at": open_at_str, "test_id": test_id})

# ============================================================
# ⚙️ TASK 4: ADMIN SOZLAMALARI PANELI
# ============================================================
@app.route("/admin/settings")
def admin_settings():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return abort(403)

    settings = [dict(s) for s in (db.get_all_settings() if hasattr(db, 'get_all_settings') else [])]
    saved_msg = request.args.get("msg", "")

    rows = ""
    for s in settings:
        key  = html.escape(s.get("key_name", ""))
        val  = html.escape(str(s.get("value_text") or s.get("value_int") or ""))
        desc = html.escape(s.get("description") or "")
        rows += f"""<div class="srow">
          <div class="sinfo">
            <code class="skey">{key}</code>
            <span class="sdesc">{desc}</span>
          </div>
          <div class="sinput">
            <input type="text" id="s_{key}" value="{val}" placeholder="{val}"
              onkeydown="if(event.key==='Enter')saveSetting('{key}')">
            <button onclick="saveSetting('{key}')">💾</button>
          </div>
        </div>"""

    page = f"""<!DOCTYPE html>
<html lang="uz"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>⚙️ Bot Sozlamalari</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0;font-family:-apple-system,sans-serif}}
body{{background:#0f172a;color:#f3f6ff;min-height:100vh;padding:20px}}
.wrap{{max-width:960px;margin:0 auto}}
h1{{font-size:22px;color:#38d39f;margin-bottom:4px}}
.sub{{color:#718096;font-size:13px;margin-bottom:20px}}
.back{{color:#6cb2ff;font-size:14px;text-decoration:none}}
.card{{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.1);border-radius:14px;padding:20px;margin-bottom:16px}}
.card h2{{font-size:16px;color:#6cb2ff;margin-bottom:14px}}
.srow{{display:flex;align-items:center;justify-content:space-between;padding:10px 0;border-bottom:1px solid rgba(255,255,255,.06);gap:12px;flex-wrap:wrap}}
.srow:last-child{{border-bottom:none}}
.sinfo{{flex:1;min-width:200px}}
code.skey{{font-size:13px;color:#fbbf24;background:rgba(251,191,36,.1);padding:2px 8px;border-radius:4px}}
.sdesc{{display:block;font-size:12px;color:#718096;margin-top:4px}}
.sinput{{display:flex;gap:8px;min-width:220px}}
.sinput input{{flex:1;padding:9px 12px;border-radius:8px;border:1px solid rgba(255,255,255,.15);background:rgba(0,0,0,.3);color:#f3f6ff;font-size:13px}}
.sinput button{{padding:9px 14px;background:#38d39f;color:#000;border:none;border-radius:8px;cursor:pointer;font-weight:700}}
.alert{{padding:10px 14px;border-radius:8px;margin-bottom:16px;font-size:13px;display:none}}
.alert-ok{{background:rgba(56,211,159,.2);color:#38d39f;border:1px solid rgba(56,211,159,.3)}}
.alert-err{{background:rgba(245,101,101,.2);color:#fc8181;border:1px solid rgba(245,101,101,.3)}}
.tab-btns{{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px}}
.tab-btn{{padding:8px 16px;border-radius:8px;border:1px solid rgba(255,255,255,.15);background:rgba(255,255,255,.07);color:#f3f6ff;cursor:pointer;font-size:13px}}
.tab-btn.active{{background:rgba(56,211,159,.2);border-color:#38d39f;color:#38d39f}}
.tab{{display:none}}.tab.active{{display:block}}
</style></head>
<body><div class="wrap">
<a class="back" href="/?token={token}">← Orqaga</a>
<br><br>
<h1>⚙️ Bot Sozlamalari</h1>
<p class="sub">Barcha sozlamalar real-time saqlanadi va bot ishiga darhol ta'sir qiladi</p>
{"<div class='alert alert-ok' style='display:block'>✅ " + html.escape(saved_msg) + "</div>" if saved_msg else ""}
<div id="alert-box" class="alert"></div>

<div class="tab-btns">
  <button class="tab-btn active" onclick="showTab('general')">⚙️ Umumiy</button>
  <button class="tab-btn" onclick="showTab('premium')">💎 Premium</button>
  <button class="tab-btn" onclick="showTab('finance')">💰 Moliya</button>
  <button class="tab-btn" onclick="showTab('security')">🔒 Xavfsizlik</button>
  <button class="tab-btn" onclick="showTab('moderation')">🛡️ Moderatsiya</button>
</div>

<div id="tab-general" class="tab card active">
<h2>⚙️ Umumiy Sozlamalar</h2>
{_settings_rows(settings, ['site_name','support_username','maintenance_mode',
  'ai_daily_limit','max_q_per_test','min_q_per_test'])}
</div>

<div id="tab-premium" class="tab card">
<h2>💎 Premium Narxlari (so'm)</h2>
{_settings_rows(settings, ['premium_price_1','premium_price_3','premium_price_6','premium_price_12'])}
</div>

<div id="tab-finance" class="tab card">
<h2>💰 Moliyaviy Sozlamalar</h2>
{_settings_rows(settings, ['gwt_price_usd',
  'registration_bonus','referral_premium_n','challenge_expire_h'])}
</div>

<div id="tab-security" class="tab card">
<h2>🔒 Xavfsizlik</h2>
{_settings_rows(settings, ['auto_moderation','max_broadcast_delay'])}
<div style="margin-top:16px">
  <a href="/admin/security?token={token}" style="color:#6cb2ff;font-size:14px">→ IP Whitelist boshqaruvi</a>
</div>
</div>

<div id="tab-moderation" class="tab card">
<h2>🛡️ Moderatsiya Qoidalari</h2>
<div id="mod-rules-list"></div>
<div style="display:flex;gap:10px;margin-top:14px">
  <input type="text" id="new-rule-val" placeholder="Taqiqlangan so'z yoki ibora"
    style="flex:1;padding:10px;border-radius:8px;border:1px solid rgba(255,255,255,.15);background:rgba(0,0,0,.3);color:#f3f6ff">
  <button onclick="addRule()" style="padding:10px 18px;background:#f56565;color:#fff;border:none;border-radius:8px;cursor:pointer;font-weight:700">
    ➕ Qo'shish
  </button>
</div>
</div>

</div>
<script>
const TOKEN = "{token}";

function showAlert(msg, ok) {{
  const el = document.getElementById('alert-box');
  el.className = 'alert ' + (ok ? 'alert-ok' : 'alert-err');
  el.style.display = 'block';
  el.textContent = msg;
  clearTimeout(el._t);
  el._t = setTimeout(() => el.style.display='none', 3500);
}}

function showTab(name) {{
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  event.target.classList.add('active');
  if(name === 'moderation') loadRules();
}}

function saveSetting(key) {{
  const val = document.getElementById('s_' + key)?.value?.trim();
  if(val === undefined) return;
  fetch('/api/admin/settings/save', {{
    method: 'POST',
    headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify({{token: TOKEN, key, value: val}})
  }}).then(r=>r.json()).then(d => {{
    if(d.success) showAlert('✅ ' + key + ' saqlandi!', true);
    else showAlert('❌ ' + (d.error||'Xato'), false);
  }}).catch(()=>showAlert('❌ Server xatosi', false));
}}

function loadRules() {{
  fetch('/api/admin/moderation/rules?token=' + TOKEN)
    .then(r=>r.json()).then(d => {{
      const el = document.getElementById('mod-rules-list');
      if(!d.rules || !d.rules.length) {{
        el.innerHTML = "<p style='color:#718096;font-size:13px'>Hali qoidalar yo'q</p>";
        return;
      }}
      el.innerHTML = d.rules.map(r =>
        `<div style="display:flex;align-items:center;justify-content:space-between;padding:8px 0;border-bottom:1px solid rgba(255,255,255,.06)">
          <span style="color:#f3f6ff;font-size:14px"><code style="color:#f87171">${{r.value}}</code></span>
          <button onclick="deleteRule(${{r.id}})" style="background:#f56565;color:#fff;border:none;border-radius:6px;padding:5px 12px;cursor:pointer;font-size:12px">O'chirish</button>
        </div>`
      ).join('');
    }});
}}

function addRule() {{
  const val = document.getElementById('new-rule-val').value.trim();
  if(!val) return;
  fetch('/api/admin/moderation/add', {{
    method: 'POST',
    headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify({{token: TOKEN, value: val, rule_type: 'banned_word'}})
  }}).then(r=>r.json()).then(d => {{
    if(d.success) {{ document.getElementById('new-rule-val').value=''; loadRules(); showAlert('✅ Qoida qo\'shildi', true); }}
    else showAlert('❌ ' + (d.error||'Xato'), false);
  }});
}}

function deleteRule(id) {{
  if(!confirm('Qoidani o\'chirasizmi?')) return;
  fetch('/api/admin/moderation/delete', {{
    method: 'POST',
    headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify({{token: TOKEN, rule_id: id}})
  }}).then(r=>r.json()).then(d => {{
    if(d.success) {{ loadRules(); showAlert('✅ O\'chirildi', true); }}
    else showAlert('❌ ' + (d.error||'Xato'), false);
  }});
}}
</script>
</body></html>"""
    return page

def _settings_rows(settings, keys):
    """Berilgan kalit nomlari uchun settings qatorlarini HTML sifatida qaytaradi"""
    result = ""
    settings_map = {dict(s)['key_name']: dict(s) for s in settings}
    for key in keys:
        s = settings_map.get(key, {})
        if not s:
            continue
        val  = html.escape(str(s.get("value_text") or s.get("value_int") or ""))
        desc = html.escape(s.get("description") or "")
        result += f"""<div class="srow">
          <div class="sinfo">
            <code class="skey">{html.escape(key)}</code>
            <span class="sdesc">{desc}</span>
          </div>
          <div class="sinput">
            <input type="text" id="s_{html.escape(key)}" value="{val}" placeholder="{val}"
              onkeydown="if(event.key==='Enter')saveSetting('{html.escape(key)}')">
            <button onclick="saveSetting('{html.escape(key)}')">💾</button>
          </div>
        </div>"""
    return result or "<p style='color:#718096'>Sozlamalar topilmadi</p>"

# Settings API
@app.route("/api/admin/settings/save", methods=["POST"])
def api_settings_save():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return jsonify({"success": False, "error": "Unauthorized"}), 403
    key   = (data.get("key") or "").strip()
    value = (data.get("value") or "").strip()
    if not key:
        return jsonify({"success": False, "error": "Key kerak"}), 400
    if hasattr(db, 'set_setting'):
        db.set_setting(key, value, int(user["user_id"]))
    return jsonify({"success": True})

@app.route("/api/admin/moderation/rules")
def api_moderation_rules():
    user = validate_token(request.args.get("token"))
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return jsonify({"success": False}), 403
    rules = [dict(r) for r in db.get_moderation_rules()]
    return jsonify({"success": True, "rules": rules})

@app.route("/api/admin/moderation/add", methods=["POST"])
def api_moderation_add():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return jsonify({"success": False}), 403
    db.add_moderation_rule(
        data.get("rule_type", "banned_word"),
        (data.get("value") or "").strip(),
        int(user["user_id"])
    )
    return jsonify({"success": True})

@app.route("/api/admin/moderation/delete", methods=["POST"])
def api_moderation_delete():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return jsonify({"success": False}), 403
    db.delete_moderation_rule(int(data.get("rule_id", 0)))
    return jsonify({"success": True})

# ============================================================
# TASK 5: PDF HISOBOT, KUPON PANEL, STAKING PANEL
# ============================================================
@app.route("/export-pdf/<test_id>")
def export_pdf(test_id):
    """Test natijalarini HTML-to-PDF sahifasi (print bilan)"""
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user: return abort(401)
    test = db.get_test(test_id)
    if not test: return abort(404)
    test = dict(test)
    if int(test.get("owner_user_id", 0)) != int(user["user_id"]) and int(user["user_id"]) not in SUPERADMINS:
        return abort(403)

    results = db.all_results(test_id)
    analytics = dict(db.get_test_analytics(test_id) or {})
    scoring = test.get("scoring_type", "standard")
    title = html.escape(test.get("title", "Test"))
    from datetime import datetime as _dt
    now_str = _dt.now(tz=TZ).strftime("%d.%m.%Y %H:%M")

    rows = ""
    for i, r in enumerate(results, 1):
        r = dict(r)
        name = html.escape((r.get("username") and f"@{r['username']}") or
                           f"{r.get('first_name','')}{r.get('last_name','')}".strip() or
                           f"User{r.get('user_id','')}")
        score = float(r.get("score") or 0)
        dur = int(r.get("duration_sec") or 0)
        m, s = divmod(dur, 60)
        rows += f"<tr><td>{i}</td><td>{name}</td><td><b>{score:g}</b></td><td>{m}:{s:02d}</td></tr>"

    page = f"""<!DOCTYPE html>
<html lang="uz"><head>
<meta charset="UTF-8">
<title>Hisobot — {title}</title>
<style>
  @page {{size:A4; margin:20mm}}
  *{{margin:0;padding:0;box-sizing:border-box;font-family:Arial,sans-serif}}
  body{{padding:24px;color:#1a202c}}
  .header{{text-align:center;border-bottom:2px solid #38d39f;padding-bottom:14px;margin-bottom:20px}}
  .header h1{{font-size:22px;color:#2d3748}}
  .header .meta{{color:#718096;font-size:13px;margin-top:6px}}
  .stats{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px}}
  .stat-box{{border:1px solid #e2e8f0;border-radius:8px;padding:12px;text-align:center}}
  .stat-num{{font-size:22px;font-weight:700;color:#38d39f}}
  .stat-lbl{{font-size:11px;color:#718096;margin-top:4px}}
  table{{width:100%;border-collapse:collapse;font-size:13px}}
  th{{background:#2d3748;color:white;padding:10px 12px;text-align:left}}
  td{{padding:9px 12px;border-bottom:1px solid #e2e8f0}}
  tr:nth-child(even) td{{background:#f7fafc}}
  tr:first-child td{{background:#f0fff4;font-weight:700}}
  .footer{{margin-top:20px;text-align:center;color:#718096;font-size:12px;border-top:1px solid #e2e8f0;padding-top:12px}}
  @media print{{
    .no-print{{display:none!important}}
    body{{padding:0}}
  }}
</style></head>
<body>
<div class="no-print" style="padding:12px;background:#0f172a;color:#f3f6ff;text-align:center;margin-bottom:20px;border-radius:8px">
  <button onclick="window.print()" style="background:#38d39f;color:#000;border:none;padding:10px 24px;border-radius:8px;font-weight:700;cursor:pointer;font-size:14px;margin-right:10px">🖨️ Chop etish / PDF</button>
  <a href="/export-excel/{test_id}?token={token}" style="color:#6cb2ff;font-size:14px">📥 Excel</a>
  <a href="/export-csv/{test_id}?token={token}" style="color:#6cb2ff;font-size:14px;margin-left:12px">📊 CSV</a>
</div>
<div class="header">
  <h1>📊 Test Hisoboti</h1>
  <div class="meta">
    <b>{title}</b> &nbsp;|&nbsp; Sana: {now_str}
  </div>
</div>
<div class="stats">
  <div class="stat-box"><div class="stat-num">{int(analytics.get("total_sessions") or 0)}</div><div class="stat-lbl">Qatnashchilar</div></div>
  <div class="stat-box"><div class="stat-num">{round(float(analytics.get("avg_score") or 0),1)}</div><div class="stat-lbl">O'rtacha ball</div></div>
  <div class="stat-box"><div class="stat-num">{round(float(analytics.get("max_score") or 0),1)}</div><div class="stat-lbl">Eng yuqori</div></div>
  <div class="stat-box"><div class="stat-num">{round(float(analytics.get("min_score") or 0),1)}</div><div class="stat-lbl">Eng past</div></div>
</div>
<table>
  <thead><tr><th>#</th><th>Foydalanuvchi</th><th>Ball</th><th>Vaqt</th></tr></thead>
  <tbody>{rows or "<tr><td colspan='4' style='text-align:center;color:#718096'>Hali natijalar yo'q</td></tr>"}</tbody>
</table>
<div class="footer">Geo Ustoz — Avtomatlashtirilgan Test Tizimi · {now_str}</div>
</body></html>"""
    return page

@app.route("/admin/coupons")
def admin_coupons():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return abort(403)

    coupons = [dict(c) for c in safe_db('get_all_coupons', default=[])]
    coupon_empty = "Hali kuponlar yo'q"

    rows = ""
    for c in coupons:
        from datetime import datetime as _dt
        exp = _dt.fromtimestamp(int(c['expires_at']), tz=TZ).strftime("%d.%m.%Y") if c.get('expires_at') else "∞"
        status = "✅" if c.get('is_active') else "❌"
        rows += f"""<div class="row" style="flex-wrap:wrap;gap:8px">
          <div style="flex:1;min-width:180px">
            <code style="color:#fbbf24;font-size:15px">{html.escape(c.get('code',''))}</code>
            <span class="badge" style="margin-left:8px">{status}</span><br>
            <span style="color:#a0aec0;font-size:12px">
              💎{c.get('months_free',0)}oy 🪙{c.get('gwt_bonus',0)}GWT
              | {c.get('use_count',0)}/{c.get('max_uses',0) or '∞'} | ⏰{exp}
            </span>
          </div>
          <div style="display:flex;gap:6px;flex-shrink:0">
            <button class="btn" onclick="toggleCoupon({c['id']},{0 if c.get('is_active') else 1})"
              style="background:{'#f56565' if c.get('is_active') else '#48bb78'};color:#fff;border:none;padding:7px 12px">
              {"⏸" if c.get('is_active') else "▶️"}
            </button>
            <button class="btn" onclick="deleteCoupon({c['id']})"
              style="background:#718096;color:#fff;border:none;padding:7px 12px">🗑</button>
          </div>
        </div>"""

    page = _PAGE_STYLE + f"""
    <div class="wrap">
      {_nav_html(token, lang)}
      <h1>🎟️ Kupon Boshqaruvi</h1>
      <div class="card">
        <h2>➕ Yangi Kupon Yaratish</h2>
        <div id="cp-alert" style="display:none;padding:10px;border-radius:8px;margin-bottom:12px;font-size:13px"></div>
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin-bottom:12px">
          <div><label style="font-size:12px;color:#a0aec0">Kod</label>
            <input id="cp-code" type="text" placeholder="PROMO50"
              style="width:100%;padding:10px;border-radius:8px;border:1px solid rgba(255,255,255,.15);background:rgba(0,0,0,.3);color:#f3f6ff;margin-top:4px"></div>
          <div><label style="font-size:12px;color:#a0aec0">Premium (oy)</label>
            <input id="cp-months" type="number" min="0" value="0"
              style="width:100%;padding:10px;border-radius:8px;border:1px solid rgba(255,255,255,.15);background:rgba(0,0,0,.3);color:#f3f6ff;margin-top:4px"></div>
          <div><label style="font-size:12px;color:#a0aec0">GWT Bonus</label>
            <input id="cp-gwt" type="number" min="0" step="0.1" value="0"
              style="width:100%;padding:10px;border-radius:8px;border:1px solid rgba(255,255,255,.15);background:rgba(0,0,0,.3);color:#f3f6ff;margin-top:4px"></div>
          <div><label style="font-size:12px;color:#a0aec0">Max foydalanish (0=∞)</label>
            <input id="cp-max" type="number" min="0" value="0"
              style="width:100%;padding:10px;border-radius:8px;border:1px solid rgba(255,255,255,.15);background:rgba(0,0,0,.3);color:#f3f6ff;margin-top:4px"></div>
          <div><label style="font-size:12px;color:#a0aec0">Muddat (soat, 0=∞)</label>
            <input id="cp-hours" type="number" min="0" value="168"
              style="width:100%;padding:10px;border-radius:8px;border:1px solid rgba(255,255,255,.15);background:rgba(0,0,0,.3);color:#f3f6ff;margin-top:4px"></div>
        </div>
        <button class="btn" onclick="createCoupon()"
          style="background:#38d39f;color:#000;border:none;padding:12px 24px;font-weight:700">
          ➕ Kupon Yaratish
        </button>
      </div>
      <div class="card">
        <h2>📋 Kuponlar ({len(coupons)} ta)</h2>
        {rows if rows else "<div class='empty'>" + coupon_empty + "</div>"}
      </div>
    </div>
    <script>
    const TOKEN = "{token}";
    function showAlert(el,msg,ok){{
      const e=document.getElementById(el);e.style.display='block';
      e.style.background=ok?'rgba(56,211,159,.2)':'rgba(245,101,101,.2)';
      e.style.color=ok?'#68d391':'#fc8181';e.textContent=msg;
      setTimeout(()=>e.style.display='none',4000);
    }}
    function createCoupon(){{
      const code=document.getElementById('cp-code').value.trim();
      if(!code)return showAlert('cp-alert','❌ Kod kiritilmagan',false);
      fetch('/api/admin/coupon/create',{{method:'POST',headers:{{'Content-Type':'application/json'}},
        body:JSON.stringify({{token:TOKEN,code,
          months_free:+document.getElementById('cp-months').value,
          gwt_bonus:+document.getElementById('cp-gwt').value,
          max_uses:+document.getElementById('cp-max').value,
          expire_hours:+document.getElementById('cp-hours').value||null}})}})
      .then(r=>r.json()).then(d=>{{
        if(d.success){{showAlert('cp-alert','✅ Kupon yaratildi!',true);setTimeout(()=>location.reload(),1200);}}
        else showAlert('cp-alert','❌ '+(d.error||'Xato'),false);
      }});
    }}
    function toggleCoupon(id,val){{
      fetch('/api/admin/coupon/toggle',{{method:'POST',headers:{{'Content-Type':'application/json'}},
        body:JSON.stringify({{token:TOKEN,coupon_id:id,is_active:val}})}})
      .then(r=>r.json()).then(d=>{{if(d.success)location.reload();}});
    }}
    function deleteCoupon(id){{
      if(!confirm('Kupon o\'chirilsinmi?'))return;
      fetch('/api/admin/coupon/delete',{{method:'POST',headers:{{'Content-Type':'application/json'}},
        body:JSON.stringify({{token:TOKEN,coupon_id:id}})}})
      .then(r=>r.json()).then(d=>{{if(d.success)location.reload();}});
    }}
    </script>"""
    return page

# Coupon API endpoints
@app.route("/api/admin/coupon/create", methods=["POST"])
def api_coupon_create():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return jsonify({"success": False, "error": "Unauthorized"}), 403
    code = (data.get("code") or "").strip().upper()
    if not code:
        return jsonify({"success": False, "error": "Kod kerak"}), 400
    expire_h = data.get("expire_hours")
    if expire_h is not None and int(expire_h) <= 0:
        expire_h = None
    try:
        db.create_coupon(code,
            months_free=int(data.get("months_free") or 0),
            gwt_bonus=float(data.get("gwt_bonus") or 0),
            max_uses=int(data.get("max_uses") or 0),
            expires_hours=int(expire_h) if expire_h else None,
            created_by=int(user["user_id"]))
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

@app.route("/api/admin/coupon/toggle", methods=["POST"])
def api_coupon_toggle():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return jsonify({"success": False}), 403
    db.toggle_coupon(int(data.get("coupon_id", 0)), int(data.get("is_active", 0)))
    return jsonify({"success": True})

@app.route("/api/admin/coupon/delete", methods=["POST"])
def api_coupon_delete():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return jsonify({"success": False}), 403
    db.delete_coupon(int(data.get("coupon_id", 0)))
    return jsonify({"success": True})

@app.route("/api/coupon/use", methods=["POST"])
def api_coupon_use():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user: return jsonify({"success": False, "error": "Unauthorized"}), 401
    code = (data.get("code") or "").strip().upper()
    coupon, err = db.get_coupon(code)
    if err:
        return jsonify({"success": False, "error": err}), 400
    ok, use_err = db.use_coupon(coupon['id'], int(user["user_id"]))
    if not ok:
        return jsonify({"success": False, "error": use_err}), 400
    applied = {}
    if coupon.get('months_free', 0) > 0:
        db.add_premium_months(int(user["user_id"]), int(coupon['months_free']))
        applied['months'] = coupon['months_free']
    if float(coupon.get('gwt_bonus', 0)) > 0:
        db.system_sell_token(int(user["user_id"]), float(coupon['gwt_bonus']), method="COUPON")
        applied['gwt'] = coupon['gwt_bonus']
    return jsonify({"success": True, "applied": applied})

# ============================================================
# 📜 MY CERTIFICATES
# ============================================================
@app.route("/my-certs")
def web_my_certs():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user:
        return abort(401)
    uid = int(user["user_id"])
    certs = db.get_user_certificates(uid)

    lbl = {"ru": "Мои сертификаты", "uz_cyrl": "Менинг сертификатларим"}.get(lang, "Mening sertifikatlarim")
    no_cert = {"ru": "Нет сертификатов", "uz_cyrl": "Сертификат йўқ"}.get(lang, "Hali sertifikat yo'q")
    lbl_view = {"ru": "Посмотреть", "uz_cyrl": "Кўриш"}.get(lang, "Ko'rish")
    lbl_print = {"ru": "Распечатать", "uz_cyrl": "Чоп этиш"}.get(lang, "Chop etish")

    rows = ""
    for cert in certs:
        cert = dict(cert)
        from datetime import datetime as _dt
        issued = _dt.fromtimestamp(int(cert.get('issued_at') or 0), tz=TZ).strftime("%d.%m.%Y")
        score = float(cert.get('score') or 0)
        title = html.escape(cert.get('title') or 'Test')
        code = cert.get('cert_code', '')
        cert_url = f"/cert/{code}"
        rows += f"""<div class="row" style="flex-direction:column;align-items:flex-start;gap:8px">
          <div><b>🏅 {title}</b><br>
          <span style="color:#a0aec0;font-size:13px">
            🎯 {score:g} ball · 📅 {issued}
          </span></div>
          <div style="display:flex;gap:8px">
            <a class="btn" href="{cert_url}" target="_blank"
              style="background:rgba(56,211,159,.15);color:#38d39f;border-color:rgba(56,211,159,.3);padding:8px 14px;text-decoration:none">
              👀 {lbl_view}
            </a>
            <code style="background:rgba(0,0,0,.3);padding:6px 10px;border-radius:6px;font-size:12px;color:#a0aec0">
              #{html.escape(code)}
            </code>
          </div>
        </div>"""

    html_page = _PAGE_STYLE + f"""
    <div class="wrap">
      {_nav_html(token, lang)}
      <h1>📜 {lbl}</h1>
      <div class="card">
        {rows if rows else f'<div class="empty">{no_cert}</div>'}
      </div>
      <div class="card" style="background:rgba(56,211,159,.05)">
        <h2>ℹ️ {"Как получить сертификат?" if lang=="ru" else ("Сертификат қандай олинади?" if lang=="uz_cyrl" else "Sertifikat qanday olinadi?")}</h2>
        <p style="color:#a0aec0;font-size:14px;margin-top:8px">
          {"Пройдите тест и получите высокий балл — сертификат выдаётся автоматически." if lang=="ru" else
           ("Тестни ишланг ва юқори балл олинг — сертификат автоматик берилади." if lang=="uz_cyrl" else
            "Testni ishlang va yuqori ball oling — sertifikat avtomatik beriladi.")}
        </p>
      </div>
    </div>
    """
    return html_page

# ============================================================
# TASK 6: CHALLENGE WEB, FREE-TEXT, SAVOL REPORT
# ============================================================
@app.route("/challenges")
def web_challenges():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user: return abort(401)
    uid = int(user["user_id"])
    challenges = safe_db('get_user_challenges', uid, default=[])

    rows = ""
    status_map = {
        "pending": ("⏳", "#fbbf24"),
        "active": ("⚡", "#6cb2ff"),
        "finished": ("🏁", "#68d391"),
        "expired": ("❌", "#718096"),
        "rejected": ("🚫", "#f56565"),
    }
    for ch in challenges:
        ch = dict(ch)
        is_challenger = ch['challenger_id'] == uid
        rival_name = html.escape(ch.get('challenged_name' if is_challenger else 'challenger_name', 'Noma\'lum'))
        status = ch.get('status', 'pending')
        icon, color = status_map.get(status, ("❓", "#718096"))
        result_txt = ""
        if status == 'finished':
            won = ch.get('winner_id') == uid
            result_txt = f"<span style='color:{'#68d391' if won else '#f56565'};font-weight:700'>{'🏆 Yutdim!' if won else '💔 Yutqazdim'}</span>"
        bet_txt = f"🪙 {float(ch.get('gwt_bet',0)):.1f} GWT" if float(ch.get('gwt_bet',0)) > 0 else ""
        my_score = ch.get('challenger_score' if is_challenger else 'challenged_score')
        rival_score = ch.get('challenged_score' if is_challenger else 'challenger_score')
        rows += f"""<div class="row" style="flex-direction:column;align-items:flex-start;gap:6px">
          <div style="display:flex;align-items:center;justify-content:space-between;width:100%">
            <span style="font-size:18px">{icon}</span>
            <span style="color:{color};font-size:13px;font-weight:600">{status.upper()}</span>
          </div>
          <b>⚔️ vs {rival_name}</b>
          <span style="color:#a0aec0;font-size:13px">📝 {html.escape(ch.get('title','Test'))} {bet_txt}</span>
          {f"<span style='font-size:13px'>Sizning ballingiz: <b>{my_score or '-'}</b> | Raqib: <b>{rival_score or '-'}</b></span>" if my_score or rival_score else ""}
          {result_txt}
        </div>"""

    no_ch = {"ru":"Нет challeng'ей","uz_cyrl":"Чалленджлар йўқ"}.get(lang,"Hali challengelar yo'q")
    page = _PAGE_STYLE + f"""
    <div class="wrap">
      {_nav_html(token, lang)}
      <h1>⚔️ Challenge — Bellashuvlar</h1>
      <div class="card">
        <h2>🆕 Yangi Challenge</h2>
        <p style="color:#a0aec0;font-size:13px;margin-bottom:12px">
          Do'stingizni bellashuvga taklif qiling! Botda: /challenge @username test_id
        </p>
        <div style="display:flex;gap:10px;flex-wrap:wrap">
          <input id="ch-user" type="text" placeholder="@raqib_username"
            style="flex:1;min-width:140px;padding:11px;border-radius:8px;border:1px solid rgba(255,255,255,.15);background:rgba(0,0,0,.3);color:#f3f6ff">
          <input id="ch-test" type="text" placeholder="test_id"
            style="flex:1;min-width:120px;padding:11px;border-radius:8px;border:1px solid rgba(255,255,255,.15);background:rgba(0,0,0,.3);color:#f3f6ff">
          <input id="ch-bet" type="number" min="0" step="0.1" value="0" placeholder="GWT stavka"
            style="width:120px;padding:11px;border-radius:8px;border:1px solid rgba(255,255,255,.15);background:rgba(0,0,0,.3);color:#f3f6ff">
          <button class="btn" onclick="createChallenge()"
            style="background:#6cb2ff;color:#000;border:none;padding:11px 18px;font-weight:700">⚔️ Taklif</button>
        </div>
        <div id="ch-alert" style="display:none;padding:10px;border-radius:8px;margin-top:10px;font-size:13px"></div>
      </div>
      <div class="card">
        <h2>📋 Challengelarim ({len(challenges)} ta)</h2>
        {rows if rows else "<div class='empty'>" + no_ch + "</div>"}
      </div>
    </div>
    <script>
    const TOKEN="{token}";
    function showAlert(el,msg,ok){{
      const e=document.getElementById(el);e.style.display='block';
      e.style.background=ok?'rgba(108,178,255,.2)':'rgba(245,101,101,.2)';
      e.style.color=ok?'#6cb2ff':'#fc8181';e.textContent=msg;
      setTimeout(()=>e.style.display='none',4000);
    }}
    function createChallenge(){{
      const username=document.getElementById('ch-user').value.replace('@','').trim();
      const test_id=document.getElementById('ch-test').value.trim();
      const bet=+document.getElementById('ch-bet').value||0;
      if(!username||!test_id)return showAlert('ch-alert','❌ Username va test_id kiritilmagan',false);
      fetch('/api/challenge/create',{{method:'POST',headers:{{'Content-Type':'application/json'}},
        body:JSON.stringify({{token:TOKEN,username,test_id,gwt_bet:bet}})}})
      .then(r=>r.json()).then(d=>{{
        if(d.success){{showAlert('ch-alert','✅ Challenge yuborildi!',true);setTimeout(()=>location.reload(),1500);}}
        else showAlert('ch-alert','❌ '+(d.error||'Xato'),false);
      }});
    }}
    </script>"""
    return page

@app.route("/api/challenge/create", methods=["POST"])
def api_challenge_create():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user: return jsonify({"success": False, "error": "Unauthorized"}), 401
    uid = int(user["user_id"])
    username = (data.get("username") or "").strip()
    test_id = (data.get("test_id") or "").strip()
    gwt_bet = float(data.get("gwt_bet") or 0)

    if not username or not test_id:
        return jsonify({"success": False, "error": "Username va test_id kerak"}), 400
    test = db.get_test(test_id)
    if not test:
        return jsonify({"success": False, "error": "Test topilmadi"}), 404
    with db._conn() as c:
        rival = c.execute("SELECT user_id, first_name FROM users WHERE username=%s", (username,)).fetchone()
    if not rival:
        return jsonify({"success": False, "error": f"@{username} topilmadi"}), 404
    rival = dict(rival)
    if rival['user_id'] == uid:
        return jsonify({"success": False, "error": "O'zingizga challenge yubora olmaysiz"}), 400
    expire_h = int(safe_get_setting('challenge_expire_h', 24))
    challenge_id = safe_db('create_challenge', test_id, uid, rival['user_id'], gwt_bet, expire_hours=expire_h)
    if challenge_id is None:
        return jsonify({"success": False, "error": "Challenge funksiyasi mavjud emas."}), 503
    return jsonify({"success": True, "challenge_id": challenge_id})

@app.route("/api/question/report", methods=["POST"])
def api_question_report():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user: return jsonify({"success": False, "error": "Unauthorized"}), 401
    safe_db('report_question',
        data.get("test_id", ""),
        int(data.get("q_index", 0)),
        int(user["user_id"]),
        data.get("report_type", "other"),
        (data.get("comment") or "")[:500]
    )
    return jsonify({"success": True})

@app.route("/api/question/comment", methods=["POST"])
def api_question_comment():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user: return jsonify({"success": False, "error": "Unauthorized"}), 401
    test_id = data.get("test_id", "")
    comment = (data.get("comment") or "").strip()[:1000]
    if not test_id or not comment:
        return jsonify({"success": False, "error": "test_id va comment kerak"}), 400
    cid = safe_db('add_test_comment', test_id, int(user["user_id"]), comment, data.get("parent_id"), default=0)
    return jsonify({"success": True, "comment_id": cid})

@app.route("/api/question/comments")
def api_question_comments():
    user = validate_token(request.args.get("token"))
    if not user: return jsonify({"success": False}), 401
    test_id = request.args.get("test_id", "")
    comments = [dict(c) for c in safe_db('get_test_comments', test_id, default=[])]
    return jsonify({"success": True, "comments": comments})

# ============================================================
# TASK 7: ADMIN REPORTS, BULK OPS, A/B TAQQOSLASH
# ============================================================
@app.route("/admin/reports")
def admin_reports():
    token = request.args.get("token")
    user = validate_token(token)
    lang = session.get("lang", "uz")
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return abort(403)

    reports = [dict(r) for r in safe_db('get_question_reports', status='pending', default=[])]
    no_rep = "Kutilayotgan reportlar yo'q"

    rows = ""
    for r in reports:
        rows += f"""<div class="row" style="flex-direction:column;align-items:flex-start;gap:6px">
          <b>🚩 {html.escape(r.get('question','')[:80])}</b>
          <span style="color:#a0aec0;font-size:13px">
            Test: <code>{html.escape(r.get('test_id',''))}</code> |
            Tur: <span style="color:#fbbf24">{html.escape(r.get('report_type',''))}</span>
          </span>
          {f"<span style='color:#f3f6ff;font-size:13px'>{html.escape(r.get('comment',''))}</span>" if r.get('comment') else ""}
          <div style="display:flex;gap:8px;margin-top:6px">
            <button class="btn" onclick="resolveReport({r['id']},'resolved')"
              style="background:#48bb78;color:#fff;border:none;padding:7px 14px">✅ Hal qilindi</button>
            <button class="btn" onclick="resolveReport({r['id']},'dismissed')"
              style="background:#718096;color:#fff;border:none;padding:7px 14px">🗑 Rad etish</button>
          </div>
        </div>"""

    # Bulk operations qismi
    page = _PAGE_STYLE + f"""
    <div class="wrap">
      {_nav_html(token, lang)}
      <h1>📊 Admin Hisobotlar va Bulk Operatsiyalar</h1>

      <div class="card">
        <h2>🚩 Savol Reportlari ({len(reports)} ta kutilmoqda)</h2>
        {rows if rows else "<div class='empty'>" + no_rep + "</div>"}
      </div>

      <div class="card">
        <h2>⚡ Bulk Operatsiyalar</h2>
        <div id="bulk-alert" style="display:none;padding:10px;border-radius:8px;margin-bottom:12px;font-size:13px"></div>

        <h3 style="font-size:15px;color:#fbbf24;margin-bottom:10px">💎 Birdan ko'p foydalanuvchiga Premium</h3>
        <textarea id="bulk-ids" placeholder="Foydalanuvchi ID larini qatorma-qator yoki vergul bilan kiriting:&#10;123456789&#10;987654321"
          style="width:100%;height:80px;padding:11px;border-radius:8px;border:1px solid rgba(255,255,255,.15);background:rgba(0,0,0,.3);color:#f3f6ff;resize:vertical;margin-bottom:8px"></textarea>
        <div style="display:flex;gap:10px;align-items:center">
          <input id="bulk-months" type="number" min="1" max="60" value="1" placeholder="Oylar"
            style="width:100px;padding:10px;border-radius:8px;border:1px solid rgba(255,255,255,.15);background:rgba(0,0,0,.3);color:#f3f6ff">
          <button class="btn" onclick="bulkPremium()"
            style="background:#fbbf24;color:#000;border:none;padding:10px 20px;font-weight:700">
            💎 Premium Berish
          </button>
        </div>

        <hr style="border-color:rgba(255,255,255,.08);margin:20px 0">

        <h3 style="font-size:15px;color:#f56565;margin-bottom:10px">🚫 Birdan ko'p foydalanuvchini ban qilish</h3>
        <textarea id="bulk-ban-ids" placeholder="Ban qilinadigan ID lar..."
          style="width:100%;height:60px;padding:11px;border-radius:8px;border:1px solid rgba(255,255,255,.15);background:rgba(0,0,0,.3);color:#f3f6ff;resize:vertical;margin-bottom:8px"></textarea>
        <div style="display:flex;gap:10px;align-items:center">
          <input id="bulk-ban-reason" type="text" placeholder="Sabab (ixtiyoriy)"
            style="flex:1;padding:10px;border-radius:8px;border:1px solid rgba(255,255,255,.15);background:rgba(0,0,0,.3);color:#f3f6ff">
          <button class="btn" onclick="bulkBan()"
            style="background:#f56565;color:#fff;border:none;padding:10px 20px;font-weight:700">
            🚫 Ban Qilish
          </button>
        </div>
      </div>

      <div class="card">
        <h2>📈 Tezkor Statistika</h2>
        <a href="/admin/stats?token={token}" class="btn"
          style="background:rgba(56,211,159,.15);color:#38d39f;border-color:rgba(56,211,159,.3);margin-right:10px">
          📊 Global Statistika
        </a>
        <a href="/admin/coupons?token={token}" class="btn"
          style="background:rgba(251,191,36,.15);color:#fbbf24;border-color:rgba(251,191,36,.3)">
          🎟️ Kuponlar
        </a>
      </div>
    </div>
    <script>
    const TOKEN="{token}";
    function showAlert(el,msg,ok){{
      const e=document.getElementById(el);e.style.display='block';
      e.style.background=ok?'rgba(56,211,159,.2)':'rgba(245,101,101,.2)';
      e.style.color=ok?'#68d391':'#fc8181';e.textContent=msg;
      setTimeout(()=>e.style.display='none',5000);
    }}
    function resolveReport(id,status){{
      fetch('/api/admin/report/resolve',{{method:'POST',headers:{{'Content-Type':'application/json'}},
        body:JSON.stringify({{token:TOKEN,report_id:id,status}})}})
      .then(r=>r.json()).then(d=>{{if(d.success)location.reload();}});
    }}
    function bulkPremium(){{
      const raw=document.getElementById('bulk-ids').value;
      const months=+document.getElementById('bulk-months').value||1;
      const ids=raw.split(/[,\\n\\s]+/).map(s=>s.trim()).filter(s=>s.match(/^\\d+$/)).map(Number);
      if(!ids.length)return showAlert('bulk-alert','❌ ID lar kiritilmagan',false);
      if(!confirm(ids.length+" ta foydalanuvchiga "+months+" oy premium berasizmi?"))return;
      fetch('/api/admin/bulk/premium',{{method:'POST',headers:{{'Content-Type':'application/json'}},
        body:JSON.stringify({{token:TOKEN,user_ids:ids,months}})}})
      .then(r=>r.json()).then(d=>{{
        if(d.success)showAlert('bulk-alert','✅ '+d.count+' ta foydalanuvchiga premium berildi!',true);
        else showAlert('bulk-alert','❌ '+(d.error||'Xato'),false);
      }});
    }}
    function bulkBan(){{
      const raw=document.getElementById('bulk-ban-ids').value;
      const reason=document.getElementById('bulk-ban-reason').value.trim()||'Bulk ban';
      const ids=raw.split(/[,\\n\\s]+/).map(s=>s.trim()).filter(s=>s.match(/^\\d+$/)).map(Number);
      if(!ids.length)return showAlert('bulk-alert','❌ ID lar kiritilmagan',false);
      if(!confirm(ids.length+" ta foydalanuvchini ban qilasizmi?"))return;
      fetch('/api/admin/bulk/ban',{{method:'POST',headers:{{'Content-Type':'application/json'}},
        body:JSON.stringify({{token:TOKEN,user_ids:ids,reason}})}})
      .then(r=>r.json()).then(d=>{{
        if(d.success)showAlert('bulk-alert','✅ '+d.count+' ta foydalanuvchi ban qilindi',true);
        else showAlert('bulk-alert','❌ '+(d.error||'Xato'),false);
      }});
    }}
    </script>"""
    return page

# Bulk & Report API
@app.route("/api/admin/report/resolve", methods=["POST"])
def api_report_resolve():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return jsonify({"success": False}), 403
    safe_db('resolve_report', int(data.get("report_id", 0)), data.get("status", "resolved"))
    return jsonify({"success": True})

@app.route("/api/admin/bulk/premium", methods=["POST"])
def api_bulk_premium():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return jsonify({"success": False}), 403
    count = safe_db('bulk_give_premium', data.get("user_ids", []), int(data.get("months", 1)), int(user["user_id"]), default=0)
    return jsonify({"success": True, "count": count})

@app.route("/api/admin/bulk/ban", methods=["POST"])
def api_bulk_ban():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user or int(user["user_id"]) not in SUPERADMINS:
        return jsonify({"success": False}), 403
    count = safe_db('bulk_ban_users', data.get("user_ids", []), int(user["user_id"]), data.get("reason", "Bulk ban"), default=0)
    return jsonify({"success": True, "count": count})

# Email report subscribe API
@app.route("/api/email-report/subscribe", methods=["POST"])
def api_email_subscribe():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user: return jsonify({"success": False}), 401
    email = (data.get("email") or "").strip()
    freq  = data.get("frequency", "weekly")
    if "@" not in email:
        return jsonify({"success": False, "error": "Email noto'g'ri"}), 400
    safe_db('subscribe_email_report', int(user["user_id"]), email, freq)
    return jsonify({"success": True})

@app.route("/api/email-report/unsubscribe", methods=["POST"])
def api_email_unsubscribe():
    data = request.json or {}
    user = validate_token(data.get("token"))
    if not user: return jsonify({"success": False}), 401
    safe_db('unsubscribe_email_report', int(user["user_id"]))
    return jsonify({"success": True})

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