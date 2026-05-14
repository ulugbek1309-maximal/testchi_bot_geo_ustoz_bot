import os
import threading
import uuid
import time
import json
import logging
import hashlib
import pymysql
import pymysql.cursors
from contextlib import contextmanager
from dbutils.pooled_db import PooledDB

# Kriptografiya kutubxonalari (Hamyon avtomat yaratilishi uchun)
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

# ==============================================================
# 🛡️ MYSQL UCHUN MAXSUS QOBIQ (CURSOR WRAPPER)
# ==============================================================
class CursorWrapper:
    def __init__(self, cursor):
        self.cursor = cursor

    def execute(self, query, args=None):
        self.cursor.execute(query, args)
        return self

    def fetchone(self): return self.cursor.fetchone()
    def fetchall(self): return self.cursor.fetchall()
    def fetchmany(self, size): return self.cursor.fetchmany(size)
    def close(self): self.cursor.close()

    @property
    def lastrowid(self): return self.cursor.lastrowid

    @property
    def description(self): return self.cursor.description


# ==============================================================
# 📦 ASOSIY DB (DATABASE) KLASSI - MYSQL UCHUN
# ==============================================================
class DB:
    def __init__(self):
        # 🔒 XAVFSIZLIK: Parollar va ulanish ma'lumotlari .env faylidan olinadi.
        # Default qiymatlar sifatida umumiy nomlar qoldirildi.
        self.host = os.getenv("DB_HOST", "localhost")
        self.user = os.getenv("DB_USER", "root")
        self.password = os.getenv("DB_PASS", "")
        self.database = os.getenv("DB_NAME", "quizbot_db")
        self.port = int(os.getenv("DB_PORT", 3306))


        self.pool = PooledDB(
            creator=pymysql,
            maxconnections=50, # Bir vaqtda 50 kishi baza bilan mustaqil ishlay oladi
            mincached=5,
            maxcached=20,
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
            charset='utf8mb4'
        )
        self._init_db()

    @contextmanager
    def _conn(self):
        # Endi lock kutish yo'q, tayyor ulanishni pooldan olib ishlatamiz
        conn = self.pool.connection()
        cursor = CursorWrapper(conn.cursor())
        try:
            yield cursor
        finally:
            cursor.close()
            conn.close()

    def _init_db(self):
        """Barcha kerakli jadvallarni avtomatik yaratish va migratsiya qilish"""
        with self._conn() as c:
            # 1. Users table
            c.execute('''CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username VARCHAR(255),
                first_name VARCHAR(255),
                last_name VARCHAR(255),
                status VARCHAR(50) DEFAULT 'free',
                registered_at BIGINT,
                premium_expire_at BIGINT,
                api_key VARCHAR(255),
                pin_code VARCHAR(255),
                pin_attempts INT DEFAULT 0,
                last_attempt_at BIGINT DEFAULT 0,
                email VARCHAR(255) DEFAULT NULL,
                secret_word VARCHAR(255) DEFAULT NULL,
                custom_bg VARCHAR(255),
                custom_lock_bg VARCHAR(255),
                lang VARCHAR(10) DEFAULT 'uz',
                is_verified TINYINT DEFAULT 0,
                pending_payload VARCHAR(255) DEFAULT NULL,
                referrer_id BIGINT DEFAULT NULL,
                bound_ip VARCHAR(50) DEFAULT NULL,
                bound_ua VARCHAR(255) DEFAULT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 2. Tests table
            c.execute('''CREATE TABLE IF NOT EXISTS tests (
                test_id VARCHAR(50) PRIMARY KEY,
                owner_user_id BIGINT,
                chat_id BIGINT,
                title VARCHAR(255),
                per_question_sec INT,
                created_at BIGINT,
                status VARCHAR(50) DEFAULT 'open',
                public_name VARCHAR(255) UNIQUE,
                password VARCHAR(255),
                manage_password VARCHAR(255),
                published_message_id BIGINT,
                deadline_ts BIGINT,
                attempts_limit INT DEFAULT 1,
                price_gwt DECIMAL(18,8) DEFAULT 0,
                price_stars INT DEFAULT 0,
                scoring_type VARCHAR(50) DEFAULT 'standard',
                time_limit INT DEFAULT 0,
                is_randomized TINYINT DEFAULT 0
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 3. Questions table
            c.execute('''CREATE TABLE IF NOT EXISTS questions (
                test_id VARCHAR(50),
                q_index INT,
                question TEXT,
                options_json TEXT,
                correct_index INT,
                photo_id VARCHAR(255) DEFAULT NULL,
                question_score DECIMAL(10,2) DEFAULT 1.0,
                PRIMARY KEY (test_id, q_index)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 4. Sessions table
            c.execute('''CREATE TABLE IF NOT EXISTS sessions (
                session_id VARCHAR(64) PRIMARY KEY,
                test_id VARCHAR(50),
                user_id BIGINT,
                started_at BIGINT,
                state VARCHAR(50) DEFAULT 'running',
                finished_at BIGINT,
                score DECIMAL(10,2) DEFAULT 0,
                duration_sec INT DEFAULT 0,
                current_q_index INT DEFAULT 0
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 5. Answers table
            c.execute('''CREATE TABLE IF NOT EXISTS answers (
                session_id VARCHAR(64),
                q_index INT,
                chosen_index INT,
                is_correct TINYINT,
                answered_at BIGINT,
                time_spent_sec INT,
                PRIMARY KEY (session_id, q_index)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 6. Chats table
            c.execute('''CREATE TABLE IF NOT EXISTS chats (
                chat_id BIGINT PRIMARY KEY,
                title VARCHAR(255),
                type VARCHAR(50),
                added_by BIGINT,
                bot_is_admin TINYINT DEFAULT 0,
                updated_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 7. Premium Requests table
            c.execute('''CREATE TABLE IF NOT EXISTS premium_requests (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT,
                photo_id VARCHAR(255),
                status VARCHAR(50) DEFAULT 'pending',
                created_at BIGINT,
                admin_msg_ids TEXT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 8. Ads table
            c.execute('''CREATE TABLE IF NOT EXISTS ads (
                ad_id VARCHAR(50) PRIMARY KEY,
                creator_id BIGINT,
                reply_text TEXT,
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 9. Ad Clicks table
            c.execute('''CREATE TABLE IF NOT EXISTS ad_clicks (
                ad_id VARCHAR(50),
                user_id BIGINT,
                clicked_at BIGINT,
                PRIMARY KEY (ad_id, user_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 10. Support table
            c.execute('''CREATE TABLE IF NOT EXISTS support_messages (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT,
                message_id BIGINT,
                sender VARCHAR(50),
                text TEXT,
                reaction VARCHAR(10) DEFAULT NULL,
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 11. AI Usage table
            c.execute('''CREATE TABLE IF NOT EXISTS ai_usage (
                user_id BIGINT,
                date_str VARCHAR(50),
                count INT DEFAULT 0,
                PRIMARY KEY (user_id, date_str)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 12. AI Chat History
            c.execute('''CREATE TABLE IF NOT EXISTS ai_chat_history (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT,
                role VARCHAR(20),
                content TEXT,
                created_at BIGINT,
                session_id VARCHAR(64) DEFAULT 'default_session'
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 13. IP tracking & Blacklist
            c.execute('''CREATE TABLE IF NOT EXISTS ip_tracking (
                ip VARCHAR(50),
                request_time BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            c.execute('''CREATE TABLE IF NOT EXISTS blacklisted_ips (
                ip VARCHAR(50) PRIMARY KEY,
                banned_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 14. Purchased Tests (Marketplace)
            c.execute('''CREATE TABLE IF NOT EXISTS purchased_tests (
                user_id BIGINT,
                test_id VARCHAR(50),
                price_paid DECIMAL(18,8),
                currency VARCHAR(10),
                purchased_at BIGINT,
                PRIMARY KEY(user_id, test_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 15. Cheat Logs table
            c.execute('''CREATE TABLE IF NOT EXISTS cheat_logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT,
                test_id VARCHAR(50),
                action VARCHAR(100) DEFAULT 'tab_switched',
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 16. Oylik Mukofotlar Jurnali
            c.execute('''CREATE TABLE IF NOT EXISTS monthly_rewards_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                month_str VARCHAR(20),
                user_id BIGINT,
                `rank` INT,
                reward DECIMAL(10,2),
                distributed_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # ==============================================================
            # 🔗 BLOKCHEYN VA HAMYONLAR JADVALLARI
            # ==============================================================

            # Hamyonlar jadvali (Foydalanuvchi manzillari)
            c.execute('''CREATE TABLE IF NOT EXISTS wallets (
                user_id BIGINT PRIMARY KEY,
                public_key VARCHAR(100) UNIQUE,
                encrypted_private_key TEXT,
                balance DECIMAL(18, 8) DEFAULT 0.0,
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # Tranzaksiyalar (Ledger - O'zgarmas kitob)
            c.execute('''CREATE TABLE IF NOT EXISTS transactions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                tx_hash VARCHAR(64) UNIQUE,
                prev_hash VARCHAR(64),
                sender_address VARCHAR(100),
                receiver_address VARCHAR(100),
                amount DECIMAL(18, 8),
                signature TEXT,
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 💎 GENESIS BLOCK YARATISH (Faqat 1 marta va 150 000 ta token)
            row = c.execute("SELECT COUNT(*) as cnt FROM transactions").fetchone()
            if row and row['cnt'] == 0:
                now_ts = int(time.time())
                # Genesis Hamyon yaratish
                c.execute("INSERT IGNORE INTO wallets (user_id, public_key, encrypted_private_key, balance, created_at) VALUES (0, 'GENESIS', 'LOCKED_BY_SYSTEM', 150000.0, %s)", (now_ts,))

                # 150 000.00000000 Token emissiyasi (Yaratilishi)
                amount = 150000.0
                genesis_hash = hashlib.sha256(b"GEO_USTOZ_GENESIS_BLOCK_150000_TOKENS").hexdigest()

                c.execute("""
                    INSERT INTO transactions (tx_hash, prev_hash, sender_address, receiver_address, amount, signature, created_at)
                    VALUES (%s, '0000000000000000000000000000000000000000000000000000000000000000', 'SYSTEM', 'GENESIS', %s, 'SYSTEM_EMISSION', %s)
                """, (genesis_hash, amount, now_ts))
                logging.info("✅ GENESIS BLOCK YARATILDI: Jami 150,000 GWT Token chiqarildi.")


            # Migratsiyalar (Eski bazani yangilash uchun ehtiyot choralari)
            try: c.execute("ALTER TABLE chats ADD COLUMN updated_at BIGINT;")
            except: pass
            try: c.execute("ALTER TABLE users ADD COLUMN pin_code VARCHAR(255);")
            except: pass
            try: c.execute("ALTER TABLE users ADD COLUMN custom_bg VARCHAR(255);")
            except: pass
            try: c.execute("ALTER TABLE users ADD COLUMN custom_lock_bg VARCHAR(255);")
            except: pass
            try: c.execute("ALTER TABLE tests ADD COLUMN attempts_limit INT DEFAULT 1;")
            except: pass
            try: c.execute("ALTER TABLE users ADD COLUMN lang VARCHAR(10) DEFAULT 'uz';")
            except: pass
            try: c.execute("ALTER TABLE users ADD COLUMN is_verified TINYINT DEFAULT 0;")
            except: pass
            try: c.execute("ALTER TABLE users ADD COLUMN pending_payload VARCHAR(255) DEFAULT NULL;")
            except: pass
            try: c.execute("ALTER TABLE users ADD COLUMN referrer_id BIGINT DEFAULT NULL;")
            except: pass

            # --- YANGI XAVFSIZLIK VA TIKLASH USTUNLARINI BAZAGA QO'SHISH ---
            try: c.execute("ALTER TABLE users ADD COLUMN bound_ip VARCHAR(50) DEFAULT NULL;")
            except: pass
            try: c.execute("ALTER TABLE users ADD COLUMN bound_ua VARCHAR(255) DEFAULT NULL;")
            except: pass
            try: c.execute("ALTER TABLE users ADD COLUMN email VARCHAR(255) DEFAULT NULL;")
            except: pass
            try: c.execute("ALTER TABLE users ADD COLUMN secret_word VARCHAR(255) DEFAULT NULL;")
            except: pass
            try: c.execute("ALTER TABLE users ADD COLUMN pin_attempts INT DEFAULT 0;")
            except: pass
            try: c.execute("ALTER TABLE users ADD COLUMN last_attempt_at BIGINT DEFAULT 0;")
            except: pass
            try: c.execute("ALTER TABLE wallets ADD COLUMN balance DECIMAL(18, 8) DEFAULT 0.0;")
            except: pass
            # ----------------------------------------------------

            try: c.execute("ALTER TABLE questions ADD COLUMN photo_id VARCHAR(255) DEFAULT NULL;")
            except: pass
            try: c.execute("ALTER TABLE ai_chat_history ADD COLUMN session_id VARCHAR(64) DEFAULT 'default_session';")
            except: pass
            try: c.execute("ALTER TABLE tests ADD COLUMN price_gwt DECIMAL(18,8) DEFAULT 0;")
            except: pass
            try: c.execute("ALTER TABLE tests ADD COLUMN price_stars INT DEFAULT 0;")
            except: pass
            try: c.execute("ALTER TABLE support_messages ADD COLUMN reaction VARCHAR(10) DEFAULT NULL;")
            except: pass
            try: c.execute("ALTER TABLE premium_requests ADD COLUMN admin_msg_ids TEXT DEFAULT NULL;")
            except: pass
            try: c.execute("ALTER TABLE tests ADD COLUMN scoring_type VARCHAR(50) DEFAULT 'standard';")
            except: pass
            try: c.execute("ALTER TABLE tests ADD COLUMN time_limit INT DEFAULT 0;")
            except: pass
            try: c.execute("ALTER TABLE tests ADD COLUMN is_randomized TINYINT DEFAULT 0;")
            except: pass
            try: c.execute("ALTER TABLE sessions MODIFY COLUMN score DECIMAL(10,2) DEFAULT 0;")
            except: pass
            try: c.execute("ALTER TABLE questions ADD COLUMN question_score DECIMAL(10,2) DEFAULT 1.0;")
            except: pass
            try: c.execute("ALTER TABLE cheat_logs ADD COLUMN action VARCHAR(100) DEFAULT 'tab_switched';")
            except: pass

    # ================= 🔗 HAMYON VA BLOKCHEYN (TOKEN) FUNKSIYALARI =================

    def create_wallet(self, user_id, public_key, encrypted_private_key):
        """Foydalanuvchi uchun yangi hamyon yaratadi."""
        with self._conn() as c:
            c.execute("""
                INSERT INTO wallets (user_id, public_key, encrypted_private_key, created_at)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE public_key=public_key
            """, (user_id, public_key, encrypted_private_key, int(time.time())))

    def get_wallet(self, user_id):
        """Foydalanuvchining hamyon ma'lumotlarini bazadan oladi."""
        with self._conn() as c:
            return c.execute("SELECT * FROM wallets WHERE user_id=%s", (user_id,)).fetchone()

    def get_token_balance(self, user_id):
        """
        Blokcheyn tarixidan aniq balansni hisoblaydi va
        keshni (wallets jadvalini) avtomatik yangilaydi.
        """
        with self._conn() as c:
            # 1. Foydalanuvchi hamyonini topish
            wallet = c.execute("SELECT public_key FROM wallets WHERE user_id=%s", (user_id,)).fetchone()
            if not wallet: return 0.0

            pub_key = wallet['public_key']

            # 2. Kirim va Chiqimni tranzaksiyalar jadvalidan aniq hisoblash
            in_row = c.execute("SELECT SUM(amount) as total_in FROM transactions WHERE receiver_address=%s", (pub_key,)).fetchone()
            out_row = c.execute("SELECT SUM(amount) as total_out FROM transactions WHERE sender_address=%s", (pub_key,)).fetchone()

            total_in = float(in_row['total_in'] or 0.0)
            total_out = float(out_row['total_out'] or 0.0)

            real_balance = round(total_in - total_out, 8)

            # 3. Keshni (wallets jadvalini) haqiqiy tarixga moslab yangilab qo'yamiz
            c.execute("UPDATE wallets SET balance=%s WHERE user_id=%s", (real_balance, user_id))

            return real_balance

    def get_all_wallets_balances(self):
        """
        Statistika uchun barcha foydalanuvchilar balansini avval yangilab, keyin qaytaradi.
        """
        with self._conn() as c:
            # Barcha hamyon egalarini olamiz
            users = c.execute("SELECT user_id FROM wallets").fetchall()

        # Har birining balansini hisoblab, bazadagi keshni yangilab chiqamiz
        for u in users:
            self.get_token_balance(u['user_id'])

        # Endi statistika uchun yangilangan ma'lumotlarni qaytaramiz
        with self._conn() as c:
            query = """
                SELECT
                    w.user_id, u.first_name, u.username, w.public_key, w.balance
                FROM wallets w
                LEFT JOIN users u ON w.user_id = u.user_id
                WHERE w.balance > 0 OR w.user_id = 0
                ORDER BY w.balance DESC
            """
            return c.execute(query).fetchall()

    def transfer_token_by_address_or_id(self, sender_id, target, amount, signature):
        """
        Token o'tkazish logikasi (Hash Chain + Cache Update).
        Har bir o'tkazma bazadagi oldingi o'tkazma xeshiga ulanib ketadi.
        """
        try:
            amount = float(amount)
            if amount <= 0:
                return False, "Miqdor 0 dan katta bo'lishi shart."
        except:
            return False, "Yaroqsiz miqdor."

        with self._conn() as c:
            # 1. Yuboruvchini aniqlash
            sender_wallet = c.execute("SELECT public_key, balance FROM wallets WHERE user_id=%s", (sender_id,)).fetchone()
            if not sender_wallet:
                return False, "Sizda hamyon mavjud emas."

            sender_pub = sender_wallet['public_key']
            current_balance = float(sender_wallet['balance'])

            if current_balance < amount:
                return False, "Balansingizda yetarli token mavjud emas."

            # 2. Qabul qiluvchini aniqlash
            receiver_pub = target
            if str(target).isdigit() and len(str(target)) < 20:  # Agar ID raqam yuborgan bo'lsa
                rec_wallet = c.execute("SELECT public_key FROM wallets WHERE user_id=%s", (int(target),)).fetchone()
                if not rec_wallet:
                    return False, "Qabul qiluvchida hamyon topilmadi."
                receiver_pub = rec_wallet['public_key']

            if sender_pub == receiver_pub:
                return False, "O'zingizga token yubora olmaysiz."

            # 3. Oldingi xeshni olish (Zanjir yaratish)
            last_tx = c.execute("SELECT tx_hash FROM transactions ORDER BY id DESC LIMIT 1").fetchone()
            prev_hash = last_tx['tx_hash'] if last_tx else '0000000000000000000000000000000000000000000000000000000000000000'

            now_ts = int(time.time())

            # 4. YANGI TX HASH (Joriy xesh) = SHA256(prev_hash + sender + receiver + amount + time + sig)
            raw_data = f"{prev_hash}{sender_pub}{receiver_pub}{amount:.8f}{now_ts}{signature}".encode('utf-8')
            tx_hash = hashlib.sha256(raw_data).hexdigest()

            # 5. Bazaga yozish (Tranzaksiya Ledger)
            c.execute("""
                INSERT INTO transactions (tx_hash, prev_hash, sender_address, receiver_address, amount, signature, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (tx_hash, prev_hash, sender_pub, receiver_pub, amount, signature, now_ts))

            # 6. Balanslarni yangilash (Wallets Cache)
            c.execute("UPDATE wallets SET balance = balance - %s WHERE user_id = %s", (amount, sender_id))
            c.execute("UPDATE wallets SET balance = balance + %s WHERE public_key = %s", (amount, receiver_pub))

            return True, "Tranzaksiya muvaffaqiyatli saqlandi!"

    def system_sell_token(self, user_id, amount, method="STARS"):
        """Tizim (Karta/Stars) orqali xarid qilinganda yoki Bonus berilganda token o'tkazish"""
        with self._conn() as c:
            # 1. Xaridorning hamyonini topish
            wallet = c.execute("SELECT public_key FROM wallets WHERE user_id=%s", (user_id,)).fetchone()
            if not wallet: return False
            pub = wallet['public_key']

            # 2. Oldingi tranzaksiya xeshini olish (Zanjir uchun)
            last_tx = c.execute("SELECT tx_hash FROM transactions ORDER BY id DESC LIMIT 1").fetchone()
            prev_hash = last_tx['tx_hash'] if last_tx else '0000000000000000000000000000000000000000000000000000000000000000'

            now_ts = int(time.time())

            # 3. Yangi Xesh yaratish
            raw_data = f"{prev_hash}SYSTEM_SALE{pub}{float(amount):.8f}{now_ts}{method}".encode('utf-8')
            tx_hash = hashlib.sha256(raw_data).hexdigest()

            # 4. Tranzaksiyani bazaga yozish
            c.execute("""
                INSERT INTO transactions (tx_hash, prev_hash, sender_address, receiver_address, amount, signature, created_at)
                VALUES (%s, %s, 'GENESIS', %s, %s, %s, %s)
            """, (tx_hash, prev_hash, pub, float(amount), f"SYS_{method}", now_ts))

            # 5. Balanslarni yangilash
            c.execute("UPDATE wallets SET balance = balance - %s WHERE user_id = 0", (amount,))
            c.execute("UPDATE wallets SET balance = balance + %s WHERE user_id = %s", (amount, user_id))
            return True

    def update_test_full(self, test_id, title, time_limit, scoring_type, is_randomized, questions):
        """Test ma'lumotlarini va savollarini to'liq yangilash (Tahrirlash)"""
        with self._conn() as c:
            # 1. Asosiy sozlamalarni yangilash
            c.execute("""
                UPDATE tests
                SET title=%s, time_limit=%s, scoring_type=%s, is_randomized=%s
                WHERE test_id=%s
            """, (title, time_limit, scoring_type, is_randomized, test_id))

            # 2. Eski savollarni o'chirish
            c.execute("DELETE FROM questions WHERE test_id=%s", (test_id,))

            # 3. Yangi savollarni va rasmlarni joylash
            for i, q in enumerate(questions):
                options_json = json.dumps(q["options"], ensure_ascii=False)
                q_score = float(q.get("score", 1.0))

                # Rasmni (photo_id) xavfsiz formatlash (NULL bilan qat'iy ishlash)
                p_id = q.get("photo_id")
                if p_id is None or str(p_id).lower() in ["none", "null", ""]:
                    p_id = None

                c.execute("""
                    INSERT INTO questions (test_id, q_index, question, options_json, correct_index, question_score, photo_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (test_id, i, q["question"].strip(), options_json, q["correct_index"], q_score, p_id))
            return True

    def log_cheat_attempt(self, user_id, test_id, ts, action="tab_switched"):
        """Hiyla ishlatish (ekrandan chiqish) urinishini jurnalga yozish"""
        with self._conn() as c:
            c.execute("INSERT INTO cheat_logs (user_id, test_id, action, created_at) VALUES (%s, %s, %s, %s)",
                      (user_id, test_id, action, ts))

    # ================= 👤 USER MANAGEMENT & REFERRALS =================
    def search_users(self, query):
        """Admin panel uchun foydalanuvchilarni ID yoki ism bo'yicha qidirish"""
        with self._conn() as c:
            q = f"%{query}%"
            return c.execute("""
                SELECT * FROM users
                WHERE username LIKE %s OR user_id LIKE %s OR first_name LIKE %s
                LIMIT 50
            """, (q, q, q)).fetchall()

    def update_pin_attempts(self, user_id, reset=False):
        """PIN urinishlarini boshqarish (Anti-Bruteforce)"""
        with self._conn() as c:
            if reset:
                c.execute("UPDATE users SET pin_attempts=0, last_attempt_at=0 WHERE user_id=%s", (user_id,))
            else:
                c.execute("UPDATE users SET pin_attempts=pin_attempts+1, last_attempt_at=%s WHERE user_id=%s", (int(time.time()), user_id))

    def set_user_pin(self, user_id, hashed_pin, email, secret_word):
        """PIN va tiklash ma'lumotlarini o'rnatish"""
        with self._conn() as c:
            c.execute("""
                UPDATE users
                SET pin_code=%s, email=%s, secret_word=%s, pin_attempts=0
                WHERE user_id=%s
            """, (hashed_pin, email, secret_word, user_id))

    def upsert_user(self, user_id, username, first_name, last_name, now_ts):
        with self._conn() as c:
            c.execute("""
                INSERT INTO users (user_id, username, first_name, last_name, registered_at)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    username=VALUES(username),
                    first_name=VALUES(first_name),
                    last_name=VALUES(last_name)
            """, (user_id, username, first_name, last_name, now_ts))

    def register_new_user_with_bonus(self, user_id, username, first_name, last_name, now_ts, referrer_id=None):
        """ Yangi foydalanuvchini ro'yxatdan o'tkazish, Referalni yozish va 1 GWT bonus berish """
        with self._conn() as c:
            user = c.execute("SELECT user_id FROM users WHERE user_id = %s", (user_id,)).fetchone()

            if user:
                # Eski foydalanuvchi
                c.execute("UPDATE users SET username=%s, first_name=%s, last_name=%s WHERE user_id=%s",
                          (username, first_name, last_name, user_id))
                return False, 0

            # Yangi foydalanuvchini yaratish
            if referrer_id and referrer_id != user_id:
                c.execute("""
                    INSERT INTO users (user_id, username, first_name, last_name, registered_at, referrer_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (user_id, username, first_name, last_name, now_ts, referrer_id))
            else:
                c.execute("""
                    INSERT INTO users (user_id, username, first_name, last_name, registered_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (user_id, username, first_name, last_name, now_ts))

            # Referal sonini olish
            ref_count = 0
            if referrer_id:
                ref_row = c.execute("SELECT COUNT(*) as cnt FROM users WHERE referrer_id = %s", (referrer_id,)).fetchone()
                ref_count = ref_row['cnt'] if ref_row else 0

        # Yangi userga Hamyon yaratish va 1 GWT berish
        try:
            BOT_TOKEN = os.getenv("BOT_TOKEN", "default_secret_key").strip()
            hasher = hashlib.sha256(BOT_TOKEN.encode())
            fernet = Fernet(base64.urlsafe_b64encode(hasher.digest()))
            private_key = ed25519.Ed25519PrivateKey.generate()
            pub_hex = private_key.public_key().public_bytes(encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw).hex()
            enc_priv = fernet.encrypt(private_key.private_bytes(encoding=serialization.Encoding.Raw, format=serialization.PrivateFormat.Raw, encryption_algorithm=serialization.NoEncryption())).decode('utf-8')

            self.create_wallet(user_id, pub_hex, enc_priv)

            # Genesis hamyonidan 1 GWT o'tkazish
            self.system_sell_token(user_id, 1.0, method="WELCOME_BONUS")
        except Exception as e:
            logging.error(f"Bonus hamyon xatosi: {e}")

        return True, ref_count

    def get_referral_count(self, user_id):
        with self._conn() as c:
            row = c.execute("SELECT COUNT(*) as cnt FROM users WHERE referrer_id = %s", (user_id,)).fetchone()
            return row['cnt'] if row else 0

    def update_user_lang(self, user_id, lang):
        with self._conn() as c:
            c.execute("UPDATE users SET lang=%s WHERE user_id=%s", (lang, user_id))

    def get_or_create_user_api_key(self, user_id):
        with self._conn() as c:
            row = c.execute("SELECT api_key FROM users WHERE user_id=%s", (user_id,)).fetchone()
            if row and row['api_key']:
                return row['api_key']
            new_key = uuid.uuid4().hex
            c.execute("UPDATE users SET api_key=%s WHERE user_id=%s", (new_key, user_id))
            return new_key

    def regenerate_user_api_key(self, user_id):
        new_key = uuid.uuid4().hex
        with self._conn() as c:
            c.execute("UPDATE users SET api_key=%s WHERE user_id=%s", (new_key, user_id))
        return new_key

    def get_user(self, user_id):
        with self._conn() as c:
            return c.execute("SELECT * FROM users WHERE user_id=%s", (user_id,)).fetchone()

    def set_pending_payload(self, user_id, payload):
        with self._conn() as c:
            c.execute("UPDATE users SET pending_payload=%s WHERE user_id=%s", (payload, user_id))

    # ================= 💎 PREMIUM & PAYMENTS =================
    def add_premium_months(self, user_id, months):
        add_sec = months * 30 * 24 * 60 * 60
        with self._conn() as c:
            user = c.execute("SELECT premium_expire_at, status FROM users WHERE user_id=%s", (user_id,)).fetchone()
            now = int(time.time())
            if user and user["status"] == "premium" and user["premium_expire_at"] and user["premium_expire_at"] > now:
                new_exp = user["premium_expire_at"] + add_sec
            else:
                new_exp = now + add_sec
            c.execute("UPDATE users SET status='premium', premium_expire_at=%s WHERE user_id=%s", (new_exp, user_id))

    # ================= 📝 TESTS & QUESTIONS =================
    def create_test(self, test_id, owner_user_id, chat_id, title, per_question_sec, created_at, password=None, manage_password=None, scoring_type='standard', time_limit=0, is_randomized=0):
        with self._conn() as c:
            c.execute("""
                INSERT INTO tests (test_id, owner_user_id, chat_id, title, per_question_sec, created_at, password, manage_password, status, scoring_type, time_limit, is_randomized)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'open', %s, %s, %s)
            """, (test_id, owner_user_id, chat_id, title, per_question_sec, created_at, password, manage_password, scoring_type, time_limit, is_randomized))

    def set_test_deadline(self, test_id, deadline_ts):
        with self._conn() as c:
            c.execute("UPDATE tests SET deadline_ts=%s WHERE test_id=%s", (deadline_ts, test_id))

    def add_question(self, test_id, q_index, question, options_list, correct_index, photo_id=None, score=1.0):
        with self._conn() as c:
            options_json = json.dumps(options_list, ensure_ascii=False)
            c.execute("""
                INSERT INTO questions (test_id, q_index, question, options_json, correct_index, photo_id, question_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    question=VALUES(question),
                    options_json=VALUES(options_json),
                    correct_index=VALUES(correct_index),
                    photo_id=VALUES(photo_id),
                    question_score=VALUES(question_score)
            """, (test_id, q_index, question, options_json, correct_index, photo_id, float(score)))

    def get_test(self, test_id):
        with self._conn() as c:
            return c.execute("SELECT * FROM tests WHERE test_id=%s", (test_id,)).fetchone()

    def get_questions(self, test_id):
        with self._conn() as c:
            return c.execute("SELECT * FROM questions WHERE test_id=%s ORDER BY q_index ASC", (test_id,)).fetchall()

    def tests_for_owner(self, user_id):
        with self._conn() as c:
            return c.execute("SELECT * FROM tests WHERE owner_user_id=%s ORDER BY created_at DESC", (user_id,)).fetchall()

    def get_all_tests_admin(self):
        with self._conn() as c:
            return c.execute("""
                SELECT t.*, u.username, u.first_name, u.last_name
                FROM tests t
                LEFT JOIN users u ON t.owner_user_id = u.user_id
                ORDER BY t.created_at DESC
            """).fetchall()

    def delete_test(self, test_id, user_id):
        with self._conn() as c:
            c.execute("DELETE FROM answers WHERE session_id IN (SELECT session_id FROM sessions WHERE test_id=%s)", (test_id,))
            c.execute("DELETE FROM sessions WHERE test_id=%s", (test_id,))
            c.execute("DELETE FROM questions WHERE test_id=%s", (test_id,))
            c.execute("DELETE FROM tests WHERE test_id=%s", (test_id,))
            return True

    def set_public_link(self, test_id, public_name, password):
        with self._conn() as c:
            exist = c.execute("SELECT test_id FROM tests WHERE public_name=%s AND test_id!=%s", (public_name, test_id)).fetchone()
            if exist: return False
            c.execute("UPDATE tests SET public_name=%s, password=%s WHERE test_id=%s", (public_name, password, test_id))
            c.execute("COMMIT")
            return True

    def search_public_tests(self, query):
        with self._conn() as c:
            return c.execute("SELECT * FROM tests WHERE public_name LIKE %s", (f"%{query}%",)).fetchall()

    def set_published_message(self, test_id, msg_id):
        with self._conn() as c:
            c.execute("UPDATE tests SET published_message_id=%s WHERE test_id=%s", (msg_id, test_id))

    def close_test(self, test_id):
        with self._conn() as c:
            c.execute("UPDATE tests SET status='closed' WHERE test_id=%s", (test_id,))

    # ================= 🕹️ SESSIONS & SOLVING =================
    def create_session(self, session_id, test_id, user_id, started_at):
        with self._conn() as c:
            c.execute("""
                INSERT INTO sessions (session_id, test_id, user_id, started_at, state)
                VALUES (%s, %s, %s, %s, 'running')
            """, (session_id, test_id, user_id, started_at))

    def upsert_answer(self, session_id, q_index, chosen_index, is_correct, answered_at, time_spent):
        with self._conn() as c:
            c.execute("""
                INSERT INTO answers (session_id, q_index, chosen_index, is_correct, answered_at, time_spent_sec)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    chosen_index=VALUES(chosen_index),
                    is_correct=VALUES(is_correct),
                    answered_at=VALUES(answered_at),
                    time_spent_sec=VALUES(time_spent_sec)
            """, (session_id, q_index, chosen_index, is_correct, answered_at, time_spent))

    def finish_session(self, session_id, finished_at, score, duration):
        with self._conn() as c:
            c.execute("""
                UPDATE sessions
                SET state='finished', finished_at=%s, score=%s, duration_sec=%s
                WHERE session_id=%s
            """, (finished_at, score, duration, session_id))

    def set_session_current_q(self, session_id, q_index):
        with self._conn() as c:
            c.execute("UPDATE sessions SET current_q_index=%s WHERE session_id=%s", (q_index, session_id))

    # ================= 📊 RESULTS & STATS =================
    def all_results(self, test_id):
        with self._conn() as c:
            return c.execute("""
                SELECT s.*, u.username, u.first_name, u.last_name
                FROM sessions s
                JOIN users u ON s.user_id = u.user_id
                WHERE s.test_id=%s AND s.state='finished'
                ORDER BY s.score DESC, s.duration_sec ASC
            """, (test_id,)).fetchall()

    def stats(self, test_id):
        with self._conn() as c:
            q_res = c.execute("SELECT COUNT(*) as c FROM questions WHERE test_id=%s", (test_id,)).fetchone()
            p_res = c.execute("SELECT COUNT(*) as c FROM sessions WHERE test_id=%s AND state='finished'", (test_id,)).fetchone()
            q_count = q_res["c"] if q_res else 0
            part_count = p_res["c"] if p_res else 0
            return q_count, part_count

    def leaderboard(self, test_id, limit=20):
        with self._conn() as c:
            return c.execute("""
                SELECT s.user_id, s.score, s.duration_sec, u.username, u.first_name, u.last_name
                FROM sessions s
                JOIN users u ON s.user_id = u.user_id
                WHERE s.test_id=%s AND s.state='finished'
                ORDER BY s.score DESC, s.duration_sec ASC
                LIMIT %s
            """, (test_id, limit)).fetchall()

    # ================= 🏆 OYLIK ANTI-CHEAT REYTING VA MUKOFOTLAR =================
    def get_current_month_leaderboard(self, current_user_id):
        """ Oylik toza natijalarni hisoblaydi (Aldovsiz va o'zining testlarisiz) """
        with self._conn() as c:
            # 1-qoida: O'zining testini ishlamaslik
            c.execute("""
                SELECT s.user_id, s.test_id, s.score, s.duration_sec, s.started_at,
                       u.first_name, u.username
                FROM sessions s
                JOIN tests t ON s.test_id = t.test_id
                JOIN users u ON s.user_id = u.user_id
                WHERE s.state = 'finished'
                  AND t.owner_user_id != s.user_id
                  AND FROM_UNIXTIME(s.started_at) >= DATE_FORMAT(NOW() ,'%Y-%m-01')
                ORDER BY s.started_at ASC
            """)
            rows = c.fetchall()

        user_scores = {}
        user_info = {}
        seen_attempts = set()

        for r in rows:
            uid = r["user_id"]
            tid = r["test_id"]
            score = float(r["score"])
            duration = int(r["duration_sec"])

            # 2-qoida: Faqat birinchi ishlagan urinishi olinadi
            if (uid, tid) in seen_attempts:
                continue
            seen_attempts.add((uid, tid))

            # 3-qoida: Speedrun ban (Juda tez ishlangan natijalar qabul qilinmaydi)
            if score > 0 and duration < (score * 1.0):
                continue

            if uid not in user_scores:
                user_scores[uid] = 0.0
                user_info[uid] = {"name": r["first_name"], "username": r["username"]}

            user_scores[uid] += score

        # Reyting ro'yxatini tuzamiz
        leaderboard = []
        for uid, total_score in user_scores.items():
            if total_score > 0:
                leaderboard.append({
                    "user_id": uid,
                    "name": user_info[uid]["name"],
                    "username": user_info[uid]["username"],
                    "score": round(total_score, 1)
                })

        # Eng ko'p ball yig'ganni oldinga chiqarish
        leaderboard.sort(key=lambda x: x["score"], reverse=True)

        top_100 = []
        user_data = None

        for idx, item in enumerate(leaderboard):
            rank = idx + 1
            item["rank"] = rank
            if rank <= 100:
                top_100.append(item)

            if item["user_id"] == current_user_id:
                user_data = {"rank": rank, "score": item["score"]}

        return top_100, user_data

    def distribute_monthly_rewards(self):
        """ Har oy oxirida GWT tangalarini tarqatuvchi qism """
        from datetime import datetime
        month_str = datetime.now().strftime("%Y-%m")

        with self._conn() as c:
            already = c.execute("SELECT 1 FROM monthly_rewards_log WHERE month_str=%s LIMIT 1", (month_str,)).fetchone()
            if already:
                return []

        top_100, _ = self.get_current_month_leaderboard(0)
        if not top_100:
            return []

        winners = []
        for user in top_100:
            rank = user["rank"]
            uid = user["user_id"]

            # GWT sovrin miqdori
            if rank == 1: reward = 5.0
            elif rank == 2: reward = 2.5
            elif rank == 3: reward = 1.5
            else: reward = 1.0

            # Mukofotni yuborish
            self.system_sell_token(uid, reward, method=f"MONTHLY_REWARD_{rank}")

            with self._conn() as c:
                c.execute("""
                    INSERT INTO monthly_rewards_log (month_str, user_id, `rank`, reward, distributed_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (month_str, uid, rank, reward, int(time.time())))

            user["reward"] = reward
            winners.append(user)

        return winners

    # ================= 🤖 🟢 ALOHIDA AI CHAT FUNKSIYALARI =================
    def add_ai_message(self, user_id, role, content):
        with self._conn() as c:
            c.execute("""
                INSERT INTO ai_chat_history (user_id, role, content, created_at)
                VALUES (%s, %s, %s, %s)
            """, (user_id, role, content, int(time.time())))

    def get_ai_history(self, user_id, limit=10):
        with self._conn() as c:
            return c.execute("""
                SELECT role, content FROM ai_chat_history
                WHERE user_id=%s
                ORDER BY created_at ASC
                LIMIT %s
            """, (user_id, limit)).fetchall()

    def clear_ai_history(self, user_id):
        with self._conn() as c:
            c.execute("DELETE FROM ai_chat_history WHERE user_id=%s", (user_id,))

    def increment_ai_usage(self, user_id, date_str):
        with self._conn() as c:
            c.execute("""
                INSERT INTO ai_usage (user_id, date_str, count)
                VALUES (%s, %s, 1)
                ON DUPLICATE KEY UPDATE count=count+1
            """, (user_id, date_str))

    def get_ai_usage(self, user_id, date_str):
        with self._conn() as c:
            row = c.execute("SELECT count FROM ai_usage WHERE user_id=%s AND date_str=%s", (user_id, date_str)).fetchone()
            return row["count"] if row else 0

    # ================= 📢 CHATS & ADS =================
    def upsert_chat(self, chat_id, title, type_, added_by, bot_is_admin, updated_at):
        with self._conn() as c:
            c.execute("""
                INSERT INTO chats (chat_id, title, type, added_by, bot_is_admin, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    title=VALUES(title),
                    bot_is_admin=VALUES(bot_is_admin),
                    updated_at=VALUES(updated_at)
            """, (chat_id, title, type_, added_by, bot_is_admin, updated_at))

    def chats_for_user(self, user_id):
        with self._conn() as c:
            return c.execute("SELECT * FROM chats WHERE added_by=%s ORDER BY updated_at DESC", (user_id,)).fetchall()

    def set_bot_admin(self, chat_id, status, ts):
        with self._conn() as c:
            c.execute("UPDATE chats SET bot_is_admin=%s, updated_at=%s WHERE chat_id=%s", (status, ts, chat_id))

    def get_chats_list(self):
        with self._conn() as c:
            return c.execute("SELECT * FROM chats ORDER BY updated_at DESC").fetchall()

    def create_ad(self, ad_id, creator_id, reply_text):
        with self._conn() as c:
            c.execute("INSERT INTO ads (ad_id, creator_id, reply_text, created_at) VALUES (%s, %s, %s, %s)",
                      (ad_id, creator_id, reply_text, int(time.time())))

    def get_ad(self, ad_id):
        with self._conn() as c:
            return c.execute("SELECT * FROM ads WHERE ad_id=%s", (ad_id,)).fetchone()

    def register_ad_click(self, ad_id, user_id):
        with self._conn() as c:
            c.execute("INSERT IGNORE INTO ad_clicks (ad_id, user_id, clicked_at) VALUES (%s, %s, %s)",
                      (ad_id, user_id, int(time.time())))

    def get_ad_stats(self, user_id, is_superadmin):
        with self._conn() as c:
            if is_superadmin:
                return c.execute("""
                    SELECT a.*, COUNT(c.user_id) as clicks, u.username, u.first_name
                    FROM ads a
                    LEFT JOIN ad_clicks c ON a.ad_id = c.ad_id
                    LEFT JOIN users u ON a.creator_id = u.user_id
                    GROUP BY a.ad_id ORDER BY a.created_at DESC
                """).fetchall()
            else:
                return c.execute("""
                    SELECT a.*, COUNT(c.user_id) as clicks
                    FROM ads a
                    LEFT JOIN ad_clicks c ON a.ad_id = c.ad_id
                    WHERE a.creator_id=%s
                    GROUP BY a.ad_id ORDER BY a.created_at DESC
                """, (user_id,)).fetchall()

    # ================= 🆘 SUPPORT & MESSAGES =================
    def save_message(self, target_id, msg_id, sender_role, text, ts):
        with self._conn() as c:
            c.execute("INSERT INTO support_messages (user_id, message_id, sender, text, created_at) VALUES (%s, %s, %s, %s, %s)",
                      (target_id, msg_id, sender_role, text, ts))

    def get_user_messages(self, user_id):
        with self._conn() as c:
            return c.execute("SELECT * FROM support_messages WHERE user_id=%s ORDER BY created_at ASC", (user_id,)).fetchall()