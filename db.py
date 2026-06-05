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

            # 17. Majburiy Kanallar Jadvali
            c.execute('''CREATE TABLE IF NOT EXISTS required_channels (
                id INT AUTO_INCREMENT PRIMARY KEY,
                channel_id VARCHAR(255) UNIQUE,
                channel_title VARCHAR(255),
                added_by BIGINT,
                added_at BIGINT,
                is_active TINYINT DEFAULT 1
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 18. Test Kategoriyalari
            c.execute('''CREATE TABLE IF NOT EXISTS categories (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) UNIQUE,
                emoji VARCHAR(20) DEFAULT '📚',
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 19. Foydalanuvchi Statistikasi (Gamification: Level, XP, Streak)
            c.execute('''CREATE TABLE IF NOT EXISTS user_stats (
                user_id BIGINT PRIMARY KEY,
                xp INT DEFAULT 0,
                level INT DEFAULT 1,
                tests_taken INT DEFAULT 0,
                tests_created INT DEFAULT 0,
                total_correct INT DEFAULT 0,
                total_questions INT DEFAULT 0,
                current_streak INT DEFAULT 0,
                longest_streak INT DEFAULT 0,
                last_activity_date VARCHAR(20) DEFAULT NULL,
                updated_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 20. Yutuqlar (Achievements) ta'riflari
            c.execute('''CREATE TABLE IF NOT EXISTS achievements (
                code VARCHAR(50) PRIMARY KEY,
                title_uz VARCHAR(255),
                title_ru VARCHAR(255),
                description_uz VARCHAR(255),
                description_ru VARCHAR(255),
                emoji VARCHAR(20) DEFAULT '🏅',
                xp_reward INT DEFAULT 0
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 21. Foydalanuvchi yutuqlari
            c.execute('''CREATE TABLE IF NOT EXISTS user_achievements (
                user_id BIGINT,
                achievement_code VARCHAR(50),
                earned_at BIGINT,
                PRIMARY KEY (user_id, achievement_code)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 22. O'quv Guruhlari (Sinflar)
            c.execute('''CREATE TABLE IF NOT EXISTS study_groups (
                group_id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(255),
                owner_id BIGINT,
                join_code VARCHAR(20) UNIQUE,
                description TEXT,
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 23. Guruh A'zolari
            c.execute('''CREATE TABLE IF NOT EXISTS group_members (
                group_id VARCHAR(50),
                user_id BIGINT,
                role VARCHAR(20) DEFAULT 'student',
                joined_at BIGINT,
                PRIMARY KEY (group_id, user_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 24. Guruhga biriktirilgan testlar (Uy vazifalari)
            c.execute('''CREATE TABLE IF NOT EXISTS group_assignments (
                id INT AUTO_INCREMENT PRIMARY KEY,
                group_id VARCHAR(50),
                test_id VARCHAR(50),
                assigned_by BIGINT,
                deadline_ts BIGINT,
                assigned_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 25. Bildirishnomalar
            c.execute('''CREATE TABLE IF NOT EXISTS notifications (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT,
                title VARCHAR(255),
                body TEXT,
                type VARCHAR(50) DEFAULT 'info',
                is_read TINYINT DEFAULT 0,
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 26. Test Sharhlari va Baholari (Reviews)
            c.execute('''CREATE TABLE IF NOT EXISTS test_reviews (
                id INT AUTO_INCREMENT PRIMARY KEY,
                test_id VARCHAR(50),
                user_id BIGINT,
                rating INT DEFAULT 5,
                comment TEXT,
                created_at BIGINT,
                UNIQUE KEY uniq_review (test_id, user_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 27. Saqlangan testlar (Bookmarks)
            c.execute('''CREATE TABLE IF NOT EXISTS test_bookmarks (
                user_id BIGINT,
                test_id VARCHAR(50),
                created_at BIGINT,
                PRIMARY KEY (user_id, test_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 28. Savol bo'yicha javob statistikasi
            c.execute('''CREATE TABLE IF NOT EXISTS question_stats (
                test_id VARCHAR(50),
                q_index INT,
                total_answers INT DEFAULT 0,
                correct_answers INT DEFAULT 0,
                PRIMARY KEY (test_id, q_index)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 29. Broadcast xabarlari
            c.execute('''CREATE TABLE IF NOT EXISTS broadcasts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                admin_id BIGINT,
                message TEXT,
                media_id VARCHAR(255),
                media_type VARCHAR(20),
                target VARCHAR(50) DEFAULT 'all',
                sent_count INT DEFAULT 0,
                fail_count INT DEFAULT 0,
                status VARCHAR(20) DEFAULT 'pending',
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 30. Vaqtinchalik havolalar (test uchun)
            c.execute('''CREATE TABLE IF NOT EXISTS temp_links (
                token VARCHAR(64) PRIMARY KEY,
                test_id VARCHAR(50),
                created_by BIGINT,
                expires_at BIGINT,
                max_uses INT DEFAULT 0,
                use_count INT DEFAULT 0,
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 31. Savol banki
            c.execute('''CREATE TABLE IF NOT EXISTS question_bank (
                id INT AUTO_INCREMENT PRIMARY KEY,
                owner_id BIGINT,
                question TEXT,
                options_json TEXT,
                correct_index INT,
                photo_id VARCHAR(255),
                category_id INT DEFAULT NULL,
                tags VARCHAR(255),
                use_count INT DEFAULT 0,
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 32. Sertifikatlar
            c.execute('''CREATE TABLE IF NOT EXISTS certificates (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT,
                test_id VARCHAR(50),
                session_id VARCHAR(64),
                score DECIMAL(10,2),
                cert_code VARCHAR(32) UNIQUE,
                issued_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 33. O'quv rejalari (haftalik jadval)
            c.execute('''CREATE TABLE IF NOT EXISTS study_plans (
                id INT AUTO_INCREMENT PRIMARY KEY,
                group_id VARCHAR(50),
                week_start DATE,
                day_of_week TINYINT,
                test_id VARCHAR(50),
                note VARCHAR(255),
                created_by BIGINT,
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 34. Reklamalar boshqaruvi
            c.execute('''CREATE TABLE IF NOT EXISTS advertisements (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255),
                body TEXT,
                media_id VARCHAR(255),
                media_type VARCHAR(20),
                url VARCHAR(500),
                target VARCHAR(50) DEFAULT 'all',
                show_count INT DEFAULT 0,
                click_count INT DEFAULT 0,
                is_active TINYINT DEFAULT 1,
                starts_at BIGINT,
                ends_at BIGINT,
                created_by BIGINT,
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 35. Reklama ko'rishlar (duplicate oldini olish)
            c.execute('''CREATE TABLE IF NOT EXISTS ad_impressions (
                ad_id INT,
                user_id BIGINT,
                seen_at BIGINT,
                clicked TINYINT DEFAULT 0,
                PRIMARY KEY (ad_id, user_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 36. Affiliate (hamkor) tizimi
            c.execute('''CREATE TABLE IF NOT EXISTS affiliates (
                user_id BIGINT PRIMARY KEY,
                ref_code VARCHAR(20) UNIQUE,
                total_referrals INT DEFAULT 0,
                total_earned DECIMAL(10,2) DEFAULT 0,
                balance DECIMAL(10,2) DEFAULT 0,
                is_active TINYINT DEFAULT 1,
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 37. 2FA kodlari
            c.execute('''CREATE TABLE IF NOT EXISTS twofa_codes (
                user_id BIGINT,
                phone VARCHAR(20),
                code VARCHAR(10),
                expires_at BIGINT,
                verified TINYINT DEFAULT 0,
                PRIMARY KEY (user_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 38. Ban sabablari
            c.execute('''CREATE TABLE IF NOT EXISTS ban_logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT,
                admin_id BIGINT,
                reason TEXT,
                action VARCHAR(20) DEFAULT 'ban',
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 39. IP Whitelist
            c.execute('''CREATE TABLE IF NOT EXISTS ip_whitelist (
                ip VARCHAR(50) PRIMARY KEY,
                label VARCHAR(100),
                added_by BIGINT,
                added_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 40. A/B Test variantlari
            c.execute('''CREATE TABLE IF NOT EXISTS ab_tests (
                id INT AUTO_INCREMENT PRIMARY KEY,
                test_id VARCHAR(50),
                q_index INT,
                variant_a TEXT,
                variant_b TEXT,
                options_a_json TEXT,
                options_b_json TEXT,
                correct_a INT,
                correct_b INT,
                a_count INT DEFAULT 0,
                b_count INT DEFAULT 0,
                a_correct INT DEFAULT 0,
                b_correct INT DEFAULT 0,
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 41. Flashcard to'plamlari
            c.execute('''CREATE TABLE IF NOT EXISTS flashcard_sets (
                id INT AUTO_INCREMENT PRIMARY KEY,
                owner_id BIGINT,
                title VARCHAR(255),
                description TEXT,
                category_id INT DEFAULT NULL,
                is_public TINYINT DEFAULT 0,
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 42. Flashcard kartalar
            c.execute('''CREATE TABLE IF NOT EXISTS flashcards (
                id INT AUTO_INCREMENT PRIMARY KEY,
                set_id INT,
                front TEXT,
                back TEXT,
                hint TEXT,
                photo_id VARCHAR(255),
                difficulty TINYINT DEFAULT 1,
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 43. Flashcard foydalanish tarixi
            c.execute('''CREATE TABLE IF NOT EXISTS flashcard_progress (
                user_id BIGINT,
                card_id INT,
                correct_count INT DEFAULT 0,
                wrong_count INT DEFAULT 0,
                last_seen BIGINT,
                PRIMARY KEY (user_id, card_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 44. AI izohlar (savol bo'yicha)
            c.execute('''CREATE TABLE IF NOT EXISTS ai_explanations (
                test_id VARCHAR(50),
                q_index INT,
                explanation TEXT,
                lang VARCHAR(10) DEFAULT 'uz',
                created_at BIGINT,
                PRIMARY KEY (test_id, q_index, lang)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 45. Migratsiyalar: yangi ustunlar
            try: c.execute("ALTER TABLE users ADD COLUMN phone VARCHAR(20) DEFAULT NULL;")
            except: pass
            try: c.execute("ALTER TABLE users ADD COLUMN twofa_enabled TINYINT DEFAULT 0;")
            except: pass
            try: c.execute("ALTER TABLE users ADD COLUMN ban_reason TEXT DEFAULT NULL;")
            except: pass
            try: c.execute("ALTER TABLE tests ADD COLUMN ab_enabled TINYINT DEFAULT 0;")
            except: pass
            try: c.execute("ALTER TABLE tests ADD COLUMN has_certificate TINYINT DEFAULT 0;")
            except: pass

            # ==============================================================
            # 🆕 YANGI FUNKSIYALAR JADVALLARI (v7.0)
            # ==============================================================

            # 46. Adaptive testing
            c.execute('''CREATE TABLE IF NOT EXISTS adaptive_progress (
                user_id BIGINT,
                q_bank_id INT,
                correct_streak INT DEFAULT 0,
                wrong_streak INT DEFAULT 0,
                difficulty_level INT DEFAULT 1,
                last_seen BIGINT,
                PRIMARY KEY (user_id, q_bank_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 47. Free-text (ochiq) savollar va AI baholash
            c.execute('''CREATE TABLE IF NOT EXISTS freetext_answers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                session_id VARCHAR(64),
                q_index INT,
                user_answer TEXT,
                ai_score DECIMAL(5,2) DEFAULT NULL,
                ai_feedback TEXT,
                checked_at BIGINT DEFAULT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 48. Kupon kodlari
            c.execute('''CREATE TABLE IF NOT EXISTS coupons (
                id INT AUTO_INCREMENT PRIMARY KEY,
                code VARCHAR(30) UNIQUE,
                discount_pct INT DEFAULT 0,
                months_free INT DEFAULT 0,
                gwt_bonus DECIMAL(10,2) DEFAULT 0,
                max_uses INT DEFAULT 0,
                use_count INT DEFAULT 0,
                expires_at BIGINT DEFAULT NULL,
                is_active TINYINT DEFAULT 1,
                created_by BIGINT,
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 49. Kupon ishlatilish logi
            c.execute('''CREATE TABLE IF NOT EXISTS coupon_uses (
                coupon_id INT,
                user_id BIGINT,
                used_at BIGINT,
                PRIMARY KEY (coupon_id, user_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 50. Challenge (bellashuv) tizimi
            c.execute('''CREATE TABLE IF NOT EXISTS challenges (
                id INT AUTO_INCREMENT PRIMARY KEY,
                test_id VARCHAR(50),
                challenger_id BIGINT,
                challenged_id BIGINT,
                challenger_score DECIMAL(10,2) DEFAULT NULL,
                challenged_score DECIMAL(10,2) DEFAULT NULL,
                challenger_done TINYINT DEFAULT 0,
                challenged_done TINYINT DEFAULT 0,
                winner_id BIGINT DEFAULT NULL,
                status VARCHAR(20) DEFAULT 'pending',
                gwt_bet DECIMAL(10,2) DEFAULT 0,
                created_at BIGINT,
                expires_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 51. GWT Staking
            c.execute('''CREATE TABLE IF NOT EXISTS staking (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT,
                amount DECIMAL(10,2),
                apy DECIMAL(5,2) DEFAULT 12.0,
                start_at BIGINT,
                unlock_at BIGINT,
                last_reward_at BIGINT,
                total_reward DECIMAL(10,2) DEFAULT 0,
                is_active TINYINT DEFAULT 1
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 52. Bot global sozlamalari
            c.execute('''CREATE TABLE IF NOT EXISTS bot_settings (
                key_name VARCHAR(100) PRIMARY KEY,
                value_text TEXT,
                value_int BIGINT DEFAULT NULL,
                description VARCHAR(255),
                updated_by BIGINT,
                updated_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 53. Email hisobot sozlamalari
            c.execute('''CREATE TABLE IF NOT EXISTS email_reports (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT,
                email VARCHAR(255),
                frequency VARCHAR(20) DEFAULT 'weekly',
                is_active TINYINT DEFAULT 1,
                last_sent BIGINT DEFAULT NULL,
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 54. Savol report (xato bildirish)
            c.execute('''CREATE TABLE IF NOT EXISTS question_reports (
                id INT AUTO_INCREMENT PRIMARY KEY,
                test_id VARCHAR(50),
                q_index INT,
                user_id BIGINT,
                report_type VARCHAR(50),
                comment TEXT,
                status VARCHAR(20) DEFAULT 'pending',
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 55. Guruh subscriptions
            c.execute('''CREATE TABLE IF NOT EXISTS group_subscriptions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                group_id VARCHAR(50),
                plan VARCHAR(20) DEFAULT 'basic',
                max_members INT DEFAULT 10,
                months INT DEFAULT 1,
                price_paid DECIMAL(10,2) DEFAULT 0,
                starts_at BIGINT,
                expires_at BIGINT,
                is_active TINYINT DEFAULT 1,
                created_by BIGINT,
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 56. Ko'p to'g'ri javobli savollar
            c.execute('''CREATE TABLE IF NOT EXISTS multi_correct_answers (
                test_id VARCHAR(50),
                q_index INT,
                correct_indices_json TEXT,
                PRIMARY KEY (test_id, q_index)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 57. Audio/Video savollar
            c.execute('''CREATE TABLE IF NOT EXISTS question_media (
                test_id VARCHAR(50),
                q_index INT,
                media_type VARCHAR(20),
                file_id VARCHAR(255),
                file_url VARCHAR(500),
                PRIMARY KEY (test_id, q_index)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 58. Test izohlari (comments)
            c.execute('''CREATE TABLE IF NOT EXISTS test_comments (
                id INT AUTO_INCREMENT PRIMARY KEY,
                test_id VARCHAR(50),
                user_id BIGINT,
                comment TEXT,
                parent_id INT DEFAULT NULL,
                likes INT DEFAULT 0,
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # 59. Moderatsiya sozlamalari va taqiqlangan so'zlar
            c.execute('''CREATE TABLE IF NOT EXISTS moderation_rules (
                id INT AUTO_INCREMENT PRIMARY KEY,
                rule_type VARCHAR(30),
                value TEXT,
                is_active TINYINT DEFAULT 1,
                added_by BIGINT,
                created_at BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''')

            # Migratsiyalar yangi ustunlar
            try: c.execute("ALTER TABLE questions ADD COLUMN q_type VARCHAR(20) DEFAULT 'single'")
            except: pass
            try: c.execute("ALTER TABLE questions ADD COLUMN media_type VARCHAR(20) DEFAULT NULL")
            except: pass
            try: c.execute("ALTER TABLE questions ADD COLUMN media_file_id VARCHAR(255) DEFAULT NULL")
            except: pass
            try: c.execute("ALTER TABLE tests ADD COLUMN is_adaptive TINYINT DEFAULT 0")
            except: pass
            try: c.execute("ALTER TABLE tests ADD COLUMN allow_comments TINYINT DEFAULT 1")
            except: pass

            # Boshlang'ich sozlamalar
            if hasattr(self, '_seed_bot_settings'):
                try:
                    self._seed_bot_settings(c)
                except Exception as e:
                    logging.error(f"Bot sozlamalari seed xatosi: {e}")

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


            # ==============================================================
            # ⚡ PERFORMANCE INDEXLAR (tezlashtirish)
            # ==============================================================
            index_cmds = [
                "CREATE INDEX IF NOT EXISTS idx_sessions_user_state ON sessions(user_id, state)",
                "CREATE INDEX IF NOT EXISTS idx_sessions_test ON sessions(test_id, state)",
                "CREATE INDEX IF NOT EXISTS idx_answers_session ON answers(session_id)",
                "CREATE INDEX IF NOT EXISTS idx_tests_owner ON tests(owner_user_id)",
                "CREATE INDEX IF NOT EXISTS idx_tests_public ON tests(public_name)",
                "CREATE INDEX IF NOT EXISTS idx_tests_status ON tests(status)",
                "CREATE INDEX IF NOT EXISTS idx_tests_category ON tests(category_id)",
                "CREATE INDEX IF NOT EXISTS idx_ai_history_user ON ai_chat_history(user_id, session_id)",
                "CREATE INDEX IF NOT EXISTS idx_notif_user ON notifications(user_id, is_read)",
                "CREATE INDEX IF NOT EXISTS idx_qbank_owner ON question_bank(owner_id)",
                "CREATE INDEX IF NOT EXISTS idx_fc_set ON flashcards(set_id)",
                "CREATE INDEX IF NOT EXISTS idx_users_status ON users(status)",
                "CREATE INDEX IF NOT EXISTS idx_ip_tracking_ip ON ip_tracking(ip, request_time)",
                "CREATE INDEX IF NOT EXISTS idx_q_stats ON question_stats(test_id)",
                "CREATE INDEX IF NOT EXISTS idx_broadcasts_status ON broadcasts(status)",
            ]
            for cmd in index_cmds:
                try:
                    c.execute(cmd)
                except Exception:
                    pass

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
            try: c.execute("ALTER TABLE tests ADD COLUMN category_id INT DEFAULT NULL;")
            except: pass
            try: c.execute("ALTER TABLE tests ADD COLUMN tags VARCHAR(255) DEFAULT NULL;")
            except: pass
            try: c.execute("ALTER TABLE tests ADD COLUMN difficulty VARCHAR(20) DEFAULT 'medium';")
            except: pass

            # ==============================================================
            # 🌱 BOSHLANG'ICH MA'LUMOTLAR (SEED) - Kategoriyalar va Yutuqlar
            # ==============================================================
            try:
                if hasattr(self, '_seed_categories'):
                    self._seed_categories(c)
                if hasattr(self, '_seed_achievements'):
                    self._seed_achievements(c)
            except Exception as e:
                logging.error(f"Seed xatosi: {e}")

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
        # 🎮 Test yaratish statistikasi va yutuqlari (xato bo'lsa ham test yaratilishi buzilmasin)
        try:
            self.record_test_created(owner_user_id)
            self.check_and_grant_achievements(owner_user_id)
        except Exception as e:
            logging.error(f"Test yaratish statistikasi xatosi: {e}")

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


    # ================= 📢 MAJBURIY KANALLAR BOSHQARUVI =================
    def add_required_channel(self, channel_id, channel_title, added_by):
        """Yangi majburiy kanal qo'shish"""
        with self._conn() as c:
            try:
                c.execute("""
                    INSERT INTO required_channels (channel_id, channel_title, added_by, added_at, is_active)
                    VALUES (%s, %s, %s, %s, 1)
                """, (channel_id, channel_title, added_by, int(time.time())))
                return True
            except Exception as e:
                logging.error(f"Kanal qo'shishda xato: {e}")
                return False

    def get_all_required_channels(self, active_only=True):
        """Barcha majburiy kanallarni olish"""
        with self._conn() as c:
            if active_only:
                return c.execute("SELECT * FROM required_channels WHERE is_active=1 ORDER BY added_at DESC").fetchall()
            else:
                return c.execute("SELECT * FROM required_channels ORDER BY added_at DESC").fetchall()

    def get_active_channel_ids(self):
        """Faqat aktiv kanallar ID'larini olish (ro'yxat ko'rinishida)"""
        with self._conn() as c:
            rows = c.execute("SELECT channel_id FROM required_channels WHERE is_active=1").fetchall()
            return [row['channel_id'] for row in rows]

    def remove_required_channel(self, channel_id):
        """Kanalni o'chirish (yoki o'chirib qo'yish)"""
        with self._conn() as c:
            c.execute("UPDATE required_channels SET is_active=0 WHERE channel_id=%s", (channel_id,))
            return True

    def activate_required_channel(self, channel_id):
        """Kanalni qayta faollashtirish"""
        with self._conn() as c:
            c.execute("UPDATE required_channels SET is_active=1 WHERE channel_id=%s", (channel_id,))
            return True

    def delete_required_channel(self, channel_id):
        """Kanalni butunlay o'chirish (bazadan)"""
        with self._conn() as c:
            c.execute("DELETE FROM required_channels WHERE channel_id=%s", (channel_id,))
            return True


    # ================= 🌱 SEED (BOSHLANG'ICH MA'LUMOTLAR) =================
    def _seed_categories(self, c):
        """Standart kategoriyalarni qo'shadi (faqat bo'sh bo'lsa)"""
        try:
            row = c.execute("SELECT COUNT(*) as cnt FROM categories").fetchone()
            if row and row['cnt'] == 0:
                defaults = [
                    ("Matematika", "🔢"), ("Fizika", "⚛️"), ("Kimyo", "🧪"),
                    ("Biologiya", "🧬"), ("Tarix", "📜"), ("Geografiya", "🌍"),
                    ("Ona tili", "📖"), ("Ingliz tili", "🇬🇧"), ("Rus tili", "🇷🇺"),
                    ("Informatika", "💻"), ("Adabiyot", "📚"), ("Boshqa", "📌"),
                ]
                now = int(time.time())
                for name, emoji in defaults:
                    c.execute("INSERT IGNORE INTO categories (name, emoji, created_at) VALUES (%s, %s, %s)", (name, emoji, now))
        except Exception as e:
            logging.error(f"Kategoriya seed xatosi: {e}")

    def _seed_achievements(self, c):
        """Standart yutuqlarni qo'shadi (faqat bo'sh bo'lsa)"""
        try:
            row = c.execute("SELECT COUNT(*) as cnt FROM achievements").fetchone()
            if row and row['cnt'] == 0:
                defaults = [
                    ("first_test", "Birinchi qadam", "Первый шаг", "Birinchi testni yechdingiz", "Вы прошли первый тест", "🎯", 50),
                    ("ten_tests", "Faol o'quvchi", "Активный ученик", "10 ta test yechdingiz", "Вы прошли 10 тестов", "📚", 100),
                    ("fifty_tests", "Bilim izlovchi", "Искатель знаний", "50 ta test yechdingiz", "Вы прошли 50 тестов", "🎓", 300),
                    ("hundred_tests", "Bilimdon", "Эрудит", "100 ta test yechdingiz", "Вы прошли 100 тестов", "🏆", 500),
                    ("first_create", "Muallif", "Автор", "Birinchi testingizni yaratdingiz", "Вы создали первый тест", "✍️", 100),
                    ("ten_create", "Ustoz", "Наставник", "10 ta test yaratdingiz", "Вы создали 10 тестов", "👨‍🏫", 300),
                    ("perfect_score", "Mukammal", "Идеально", "100% natija oldingiz", "Вы набрали 100%", "💯", 150),
                    ("streak_7", "Bir haftalik", "Недельный", "7 kun ketma-ket faol bo'ldingiz", "7 дней подряд активны", "🔥", 200),
                    ("streak_30", "Bir oylik", "Месячный", "30 kun ketma-ket faol bo'ldingiz", "30 дней подряд активны", "⚡", 500),
                    ("level_5", "5-daraja", "Уровень 5", "5-darajaga yetdingiz", "Вы достигли 5 уровня", "⭐", 250),
                    ("level_10", "10-daraja", "Уровень 10", "10-darajaga yetdingiz", "Вы достигли 10 уровня", "🌟", 500),
                ]
                for code, t_uz, t_ru, d_uz, d_ru, emoji, xp in defaults:
                    c.execute("""INSERT IGNORE INTO achievements
                        (code, title_uz, title_ru, description_uz, description_ru, emoji, xp_reward)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (code, t_uz, t_ru, d_uz, d_ru, emoji, xp))
        except Exception as e:
            logging.error(f"Yutuq seed xatosi: {e}")

    # ================= 🎮 GAMIFICATION (XP, LEVEL, STREAK) =================
    def _ensure_user_stats(self, c, user_id):
        """user_stats qatorini yaratadi (agar yo'q bo'lsa)"""
        c.execute("INSERT IGNORE INTO user_stats (user_id, updated_at) VALUES (%s, %s)", (user_id, int(time.time())))

    def get_user_stats(self, user_id):
        """Foydalanuvchi statistikasini oladi (yo'q bo'lsa yaratadi)"""
        with self._conn() as c:
            self._ensure_user_stats(c, user_id)
            return c.execute("SELECT * FROM user_stats WHERE user_id=%s", (user_id,)).fetchone()

    @staticmethod
    def xp_for_level(level):
        """Berilgan darajaga yetish uchun kerakli umumiy XP"""
        # Har daraja uchun: 100 * level^1.5 taxminan
        return int(100 * (level ** 1.5))

    def add_xp(self, user_id, amount):
        """XP qo'shadi va kerak bo'lsa darajani oshiradi. Yangi level qaytaradi (yoki None)."""
        with self._conn() as c:
            self._ensure_user_stats(c, user_id)
            row = c.execute("SELECT xp, level FROM user_stats WHERE user_id=%s", (user_id,)).fetchone()
            xp = int(row['xp'] or 0) + int(amount)
            level = int(row['level'] or 1)
            leveled_up = False
            # Keyingi darajaga yetganini tekshirish
            while xp >= self.xp_for_level(level + 1):
                level += 1
                leveled_up = True
            c.execute("UPDATE user_stats SET xp=%s, level=%s, updated_at=%s WHERE user_id=%s",
                      (xp, level, int(time.time()), user_id))
            return level if leveled_up else None

    def update_streak(self, user_id):
        """Kunlik faollik zanjirini (streak) yangilaydi. Joriy streakni qaytaradi."""
        from datetime import datetime, timedelta
        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        with self._conn() as c:
            self._ensure_user_stats(c, user_id)
            row = c.execute("SELECT current_streak, longest_streak, last_activity_date FROM user_stats WHERE user_id=%s", (user_id,)).fetchone()
            last = row['last_activity_date']
            cur = int(row['current_streak'] or 0)
            longest = int(row['longest_streak'] or 0)
            if last == today:
                return cur  # Bugun allaqachon faol
            elif last == yesterday:
                cur += 1
            else:
                cur = 1
            longest = max(longest, cur)
            c.execute("UPDATE user_stats SET current_streak=%s, longest_streak=%s, last_activity_date=%s, updated_at=%s WHERE user_id=%s",
                      (cur, longest, today, int(time.time()), user_id))
            return cur

    def record_test_taken(self, user_id, correct, total):
        """Test yechilganini statistikaga yozadi"""
        with self._conn() as c:
            self._ensure_user_stats(c, user_id)
            c.execute("""UPDATE user_stats SET
                tests_taken = tests_taken + 1,
                total_correct = total_correct + %s,
                total_questions = total_questions + %s,
                updated_at = %s
                WHERE user_id=%s""", (int(correct), int(total), int(time.time()), user_id))

    def record_test_created(self, user_id):
        """Test yaratilganini statistikaga yozadi"""
        with self._conn() as c:
            self._ensure_user_stats(c, user_id)
            c.execute("UPDATE user_stats SET tests_created = tests_created + 1, updated_at=%s WHERE user_id=%s",
                      (int(time.time()), user_id))

    # ================= 🏅 ACHIEVEMENTS (YUTUQLAR) =================
    def grant_achievement(self, user_id, code):
        """Yutuq beradi (agar oldin berilmagan bo'lsa). True qaytarsa - yangi yutuq."""
        with self._conn() as c:
            exists = c.execute("SELECT 1 FROM user_achievements WHERE user_id=%s AND achievement_code=%s", (user_id, code)).fetchone()
            if exists:
                return False
            ach = c.execute("SELECT xp_reward FROM achievements WHERE code=%s", (code,)).fetchone()
            if not ach:
                return False
            c.execute("INSERT INTO user_achievements (user_id, achievement_code, earned_at) VALUES (%s, %s, %s)",
                      (user_id, code, int(time.time())))
        # XP mukofotini qo'shish (alohida ulanishda)
        try:
            self.add_xp(user_id, int(ach['xp_reward'] or 0))
        except Exception:
            pass
        return True

    def get_user_achievements(self, user_id):
        """Foydalanuvchi yutuqlarini ta'riflari bilan oladi"""
        with self._conn() as c:
            return c.execute("""
                SELECT a.*, ua.earned_at
                FROM user_achievements ua
                JOIN achievements a ON ua.achievement_code = a.code
                WHERE ua.user_id=%s
                ORDER BY ua.earned_at DESC
            """, (user_id,)).fetchall()

    def get_all_achievements(self):
        with self._conn() as c:
            return c.execute("SELECT * FROM achievements ORDER BY xp_reward ASC").fetchall()

    def check_and_grant_achievements(self, user_id):
        """Foydalanuvchi statistikasiga qarab yutuqlarni avtomatik beradi. Yangi berilganlar ro'yxatini qaytaradi."""
        stats = self.get_user_stats(user_id)
        if not stats:
            return []
        newly = []
        checks = [
            ("first_test", stats['tests_taken'] >= 1),
            ("ten_tests", stats['tests_taken'] >= 10),
            ("fifty_tests", stats['tests_taken'] >= 50),
            ("hundred_tests", stats['tests_taken'] >= 100),
            ("first_create", stats['tests_created'] >= 1),
            ("ten_create", stats['tests_created'] >= 10),
            ("streak_7", stats['current_streak'] >= 7),
            ("streak_30", stats['current_streak'] >= 30),
            ("level_5", stats['level'] >= 5),
            ("level_10", stats['level'] >= 10),
        ]
        for code, condition in checks:
            if condition and self.grant_achievement(user_id, code):
                newly.append(code)
        return newly

    # ================= 📊 KENGAYTIRILGAN STATISTIKA =================
    def record_question_answer(self, test_id, q_index, is_correct):
        """Har bir savol bo'yicha javob statistikasini yozadi"""
        with self._conn() as c:
            c.execute("""
                INSERT INTO question_stats (test_id, q_index, total_answers, correct_answers)
                VALUES (%s, %s, 1, %s)
                ON DUPLICATE KEY UPDATE
                    total_answers = total_answers + 1,
                    correct_answers = correct_answers + %s
            """, (test_id, q_index, 1 if is_correct else 0, 1 if is_correct else 0))

    def get_hardest_questions(self, test_id, limit=5):
        """Eng ko'p xato qilingan savollarni qaytaradi"""
        with self._conn() as c:
            return c.execute("""
                SELECT qs.q_index, qs.total_answers, qs.correct_answers, q.question,
                       ROUND(100.0 * qs.correct_answers / NULLIF(qs.total_answers,0), 1) as success_rate
                FROM question_stats qs
                JOIN questions q ON qs.test_id = q.test_id AND qs.q_index = q.q_index
                WHERE qs.test_id=%s AND qs.total_answers > 0
                ORDER BY success_rate ASC
                LIMIT %s
            """, (test_id, limit)).fetchall()

    def get_test_analytics(self, test_id):
        """Test bo'yicha to'liq analitika"""
        with self._conn() as c:
            row = c.execute("""
                SELECT
                    COUNT(*) as total_sessions,
                    AVG(score) as avg_score,
                    MAX(score) as max_score,
                    MIN(score) as min_score,
                    AVG(duration_sec) as avg_duration
                FROM sessions
                WHERE test_id=%s AND state='finished'
            """, (test_id,)).fetchone()
            return row

    def get_global_stats(self):
        """Platforma bo'yicha umumiy statistika (admin uchun)"""
        with self._conn() as c:
            users = c.execute("SELECT COUNT(*) as c FROM users").fetchone()['c']
            tests = c.execute("SELECT COUNT(*) as c FROM tests").fetchone()['c']
            sessions = c.execute("SELECT COUNT(*) as c FROM sessions WHERE state='finished'").fetchone()['c']
            premium = c.execute("SELECT COUNT(*) as c FROM users WHERE status='premium'").fetchone()['c']
            today = int(time.time()) - 86400
            new_today = c.execute("SELECT COUNT(*) as c FROM users WHERE registered_at >= %s", (today,)).fetchone()['c']
            return {
                "total_users": users,
                "total_tests": tests,
                "total_sessions": sessions,
                "premium_users": premium,
                "new_users_today": new_today,
            }

    def get_top_tests(self, limit=10):
        """Eng ko'p yechilgan testlar"""
        with self._conn() as c:
            return c.execute("""
                SELECT t.test_id, t.title, t.public_name, COUNT(s.session_id) as plays
                FROM tests t
                LEFT JOIN sessions s ON t.test_id = s.test_id AND s.state='finished'
                GROUP BY t.test_id
                HAVING plays > 0
                ORDER BY plays DESC
                LIMIT %s
            """, (limit,)).fetchall()

    def get_user_progress(self, user_id):
        """Foydalanuvchining o'sish dinamikasi (oxirgi 30 kun)"""
        with self._conn() as c:
            return c.execute("""
                SELECT DATE(FROM_UNIXTIME(finished_at)) as day, COUNT(*) as cnt, AVG(score) as avg_score
                FROM sessions
                WHERE user_id=%s AND state='finished' AND finished_at >= %s
                GROUP BY day
                ORDER BY day ASC
            """, (user_id, int(time.time()) - 30*86400)).fetchall()

    # ================= 🗂️ KATEGORIYALAR VA TEGLAR =================
    def get_categories(self):
        with self._conn() as c:
            return c.execute("SELECT * FROM categories ORDER BY name ASC").fetchall()

    def add_category(self, name, emoji="📚"):
        with self._conn() as c:
            try:
                c.execute("INSERT INTO categories (name, emoji, created_at) VALUES (%s, %s, %s)", (name, emoji, int(time.time())))
                return True
            except Exception:
                return False

    def delete_category(self, category_id):
        with self._conn() as c:
            c.execute("DELETE FROM categories WHERE id=%s", (category_id,))
            return True

    def set_test_category(self, test_id, category_id):
        with self._conn() as c:
            c.execute("UPDATE tests SET category_id=%s WHERE test_id=%s", (category_id, test_id))

    def set_test_meta(self, test_id, category_id=None, tags=None, difficulty=None):
        """Test metama'lumotlarini yangilaydi (kategoriya, teglar, qiyinlik)"""
        with self._conn() as c:
            c.execute("""UPDATE tests SET
                category_id = COALESCE(%s, category_id),
                tags = COALESCE(%s, tags),
                difficulty = COALESCE(%s, difficulty)
                WHERE test_id=%s""", (category_id, tags, difficulty, test_id))

    def get_tests_by_category(self, category_id, limit=50):
        with self._conn() as c:
            return c.execute("""
                SELECT t.*, COUNT(s.session_id) as plays
                FROM tests t
                LEFT JOIN sessions s ON t.test_id = s.test_id AND s.state='finished'
                WHERE t.category_id=%s AND t.public_name IS NOT NULL
                GROUP BY t.test_id
                ORDER BY plays DESC
                LIMIT %s
            """, (category_id, limit)).fetchall()

    def search_tests_advanced(self, query=None, category_id=None, difficulty=None, limit=50):
        """Kengaytirilgan qidiruv (nom, kategoriya, qiyinlik bo'yicha)"""
        with self._conn() as c:
            sql = """SELECT t.*, COUNT(s.session_id) as plays
                     FROM tests t
                     LEFT JOIN sessions s ON t.test_id = s.test_id AND s.state='finished'
                     WHERE t.public_name IS NOT NULL"""
            params = []
            if query:
                sql += " AND (t.public_name LIKE %s OR t.title LIKE %s OR t.tags LIKE %s)"
                q = f"%{query}%"
                params.extend([q, q, q])
            if category_id:
                sql += " AND t.category_id=%s"
                params.append(category_id)
            if difficulty:
                sql += " AND t.difficulty=%s"
                params.append(difficulty)
            sql += " GROUP BY t.test_id ORDER BY plays DESC LIMIT %s"
            params.append(limit)
            return c.execute(sql, tuple(params)).fetchall()

    # ================= 👥 O'QUV GURUHLARI (SINFLAR) =================
    def create_study_group(self, group_id, name, owner_id, join_code, description=""):
        with self._conn() as c:
            c.execute("""
                INSERT INTO study_groups (group_id, name, owner_id, join_code, description, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (group_id, name, owner_id, join_code, description, int(time.time())))
            # Yaratuvchini o'qituvchi sifatida qo'shish
            c.execute("INSERT IGNORE INTO group_members (group_id, user_id, role, joined_at) VALUES (%s, %s, 'teacher', %s)",
                      (group_id, owner_id, int(time.time())))

    def get_study_group(self, group_id):
        with self._conn() as c:
            return c.execute("SELECT * FROM study_groups WHERE group_id=%s", (group_id,)).fetchone()

    def get_group_by_code(self, join_code):
        with self._conn() as c:
            return c.execute("SELECT * FROM study_groups WHERE join_code=%s", (join_code,)).fetchone()

    def join_group(self, group_id, user_id, role='student'):
        with self._conn() as c:
            exists = c.execute("SELECT 1 FROM group_members WHERE group_id=%s AND user_id=%s", (group_id, user_id)).fetchone()
            if exists:
                return False
            c.execute("INSERT INTO group_members (group_id, user_id, role, joined_at) VALUES (%s, %s, %s, %s)",
                      (group_id, user_id, role, int(time.time())))
            return True

    def leave_group(self, group_id, user_id):
        with self._conn() as c:
            c.execute("DELETE FROM group_members WHERE group_id=%s AND user_id=%s", (group_id, user_id))
            return True

    def get_user_groups(self, user_id):
        """Foydalanuvchi a'zo bo'lgan guruhlar"""
        with self._conn() as c:
            return c.execute("""
                SELECT g.*, gm.role
                FROM study_groups g
                JOIN group_members gm ON g.group_id = gm.group_id
                WHERE gm.user_id=%s
                ORDER BY g.created_at DESC
            """, (user_id,)).fetchall()

    def get_group_members(self, group_id):
        with self._conn() as c:
            return c.execute("""
                SELECT gm.*, u.first_name, u.last_name, u.username
                FROM group_members gm
                JOIN users u ON gm.user_id = u.user_id
                WHERE gm.group_id=%s
                ORDER BY gm.role DESC, gm.joined_at ASC
            """, (group_id,)).fetchall()

    def assign_test_to_group(self, group_id, test_id, assigned_by, deadline_ts=None):
        with self._conn() as c:
            c.execute("""
                INSERT INTO group_assignments (group_id, test_id, assigned_by, deadline_ts, assigned_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (group_id, test_id, assigned_by, deadline_ts, int(time.time())))

    def get_group_assignments(self, group_id):
        with self._conn() as c:
            return c.execute("""
                SELECT ga.*, t.title, t.public_name
                FROM group_assignments ga
                JOIN tests t ON ga.test_id = t.test_id
                WHERE ga.group_id=%s
                ORDER BY ga.assigned_at DESC
            """, (group_id,)).fetchall()

    def get_group_leaderboard(self, group_id):
        """Guruh ichidagi o'quvchilar reytingi (yig'ilgan ball bo'yicha)"""
        with self._conn() as c:
            return c.execute("""
                SELECT u.user_id, u.first_name, u.username,
                       COUNT(DISTINCT s.test_id) as tests_done,
                       COALESCE(SUM(s.score),0) as total_score
                FROM group_members gm
                JOIN users u ON gm.user_id = u.user_id
                LEFT JOIN sessions s ON s.user_id = u.user_id AND s.state='finished'
                WHERE gm.group_id=%s AND gm.role='student'
                GROUP BY u.user_id
                ORDER BY total_score DESC
            """, (group_id,)).fetchall()

    # ================= 🔔 BILDIRISHNOMALAR =================
    def add_notification(self, user_id, title, body, ntype="info"):
        with self._conn() as c:
            c.execute("""
                INSERT INTO notifications (user_id, title, body, type, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (user_id, title, body, ntype, int(time.time())))

    def get_notifications(self, user_id, unread_only=False, limit=30):
        with self._conn() as c:
            if unread_only:
                return c.execute("SELECT * FROM notifications WHERE user_id=%s AND is_read=0 ORDER BY created_at DESC LIMIT %s", (user_id, limit)).fetchall()
            return c.execute("SELECT * FROM notifications WHERE user_id=%s ORDER BY created_at DESC LIMIT %s", (user_id, limit)).fetchall()

    def count_unread_notifications(self, user_id):
        with self._conn() as c:
            row = c.execute("SELECT COUNT(*) as cnt FROM notifications WHERE user_id=%s AND is_read=0", (user_id,)).fetchone()
            return row['cnt'] if row else 0

    def mark_notification_read(self, notif_id):
        with self._conn() as c:
            c.execute("UPDATE notifications SET is_read=1 WHERE id=%s", (notif_id,))

    def mark_all_read(self, user_id):
        with self._conn() as c:
            c.execute("UPDATE notifications SET is_read=1 WHERE user_id=%s", (user_id,))

    def get_due_assignments(self, hours_before=24):
        """Deadline yaqinlashgan vazifalarni qaytaradi (eslatma yuborish uchun)"""
        now = int(time.time())
        soon = now + hours_before * 3600
        with self._conn() as c:
            return c.execute("""
                SELECT ga.*, t.title, g.name as group_name
                FROM group_assignments ga
                JOIN tests t ON ga.test_id = t.test_id
                JOIN study_groups g ON ga.group_id = g.group_id
                WHERE ga.deadline_ts IS NOT NULL
                  AND ga.deadline_ts > %s AND ga.deadline_ts <= %s
            """, (now, soon)).fetchall()

    # ================= ⭐ SHARHLAR VA BAHOLAR (REVIEWS) =================
    def add_review(self, test_id, user_id, rating, comment=""):
        with self._conn() as c:
            c.execute("""
                INSERT INTO test_reviews (test_id, user_id, rating, comment, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE rating=VALUES(rating), comment=VALUES(comment), created_at=VALUES(created_at)
            """, (test_id, user_id, rating, comment, int(time.time())))

    def get_reviews(self, test_id, limit=50):
        with self._conn() as c:
            return c.execute("""
                SELECT r.*, u.first_name, u.username
                FROM test_reviews r
                JOIN users u ON r.user_id = u.user_id
                WHERE r.test_id=%s
                ORDER BY r.created_at DESC
                LIMIT %s
            """, (test_id, limit)).fetchall()

    def get_test_rating(self, test_id):
        """Test o'rtacha bahosi va sharhlar soni"""
        with self._conn() as c:
            row = c.execute("""
                SELECT AVG(rating) as avg_rating, COUNT(*) as review_count
                FROM test_reviews WHERE test_id=%s
            """, (test_id,)).fetchone()
            return {
                "avg_rating": round(float(row['avg_rating']), 1) if row and row['avg_rating'] else 0.0,
                "review_count": row['review_count'] if row else 0,
            }

    # ================= 🔖 BOOKMARKLAR (SAQLANGAN TESTLAR) =================
    def add_bookmark(self, user_id, test_id):
        with self._conn() as c:
            c.execute("INSERT IGNORE INTO test_bookmarks (user_id, test_id, created_at) VALUES (%s, %s, %s)",
                      (user_id, test_id, int(time.time())))

    def remove_bookmark(self, user_id, test_id):
        with self._conn() as c:
            c.execute("DELETE FROM test_bookmarks WHERE user_id=%s AND test_id=%s", (user_id, test_id))

    def is_bookmarked(self, user_id, test_id):
        with self._conn() as c:
            return c.execute("SELECT 1 FROM test_bookmarks WHERE user_id=%s AND test_id=%s", (user_id, test_id)).fetchone() is not None

    def get_bookmarks(self, user_id):
        with self._conn() as c:
            return c.execute("""
                SELECT t.*, b.created_at as bookmarked_at
                FROM test_bookmarks b
                JOIN tests t ON b.test_id = t.test_id
                WHERE b.user_id=%s
                ORDER BY b.created_at DESC
            """, (user_id,)).fetchall()


    # ================= 🎯 TEST YAKUNLASH POST-PROCESSING (Gamification) =================
    def process_test_completion(self, user_id, test_id, session_id, correct, total, score, is_owner=False):
        """
        Test yakunlangach barcha gamification jarayonlarini bajaradi:
        - Statistika yangilash, XP berish, streak, yutuqlar, savol statistikasi.
        Natijani dict ko'rinishida qaytaradi: {xp_gained, new_level, new_achievements, streak}
        Eslatma: o'z testini ishlaganda XP/streak berilmaydi (is_owner=True).
        """
        result = {"xp_gained": 0, "new_level": None, "new_achievements": [], "streak": 0}
        try:
            # 1. Savol statistikasini yozish (har bir savol bo'yicha)
            with self._conn() as c:
                answers = c.execute("SELECT q_index, is_correct FROM answers WHERE session_id=%s", (session_id,)).fetchall()
            for a in answers:
                self.record_question_answer(test_id, a['q_index'], bool(a['is_correct']))

            # O'z testini ishlasa - reyting/XP bermaymiz (aldovning oldini olish)
            if is_owner:
                return result

            # 2. Umumiy statistika
            self.record_test_taken(user_id, correct, total)

            # 3. Streak yangilash
            result["streak"] = self.update_streak(user_id)

            # 4. XP hisoblash: har to'g'ri javob +10 XP, tugatgani uchun +20 XP
            xp_gained = int(correct) * 10 + 20
            # Streak bonusi
            if result["streak"] >= 7:
                xp_gained = int(xp_gained * 1.5)
            result["xp_gained"] = xp_gained
            new_level = self.add_xp(user_id, xp_gained)
            result["new_level"] = new_level

            # 5. Mukammal natija yutug'i (100% to'g'ri)
            if total > 0 and correct == total:
                if self.grant_achievement(user_id, "perfect_score"):
                    result["new_achievements"].append("perfect_score")

            # 6. Boshqa yutuqlarni tekshirish
            result["new_achievements"].extend(self.check_and_grant_achievements(user_id))
        except Exception as e:
            logging.error(f"process_test_completion xatosi: {e}")
        return result


    # ================= 📢 BROADCAST =================
    def create_broadcast(self, admin_id, message, media_id=None, media_type=None, target='all'):
        with self._conn() as c:
            c.execute("""INSERT INTO broadcasts (admin_id, message, media_id, media_type, target, status, created_at)
                VALUES (%s,%s,%s,%s,%s,'pending',%s)""",
                (admin_id, message, media_id, media_type, target, int(time.time())))
            return c.lastrowid

    def update_broadcast_stats(self, broadcast_id, sent=0, fail=0, status=None):
        with self._conn() as c:
            if status:
                c.execute("UPDATE broadcasts SET sent_count=sent_count+%s, fail_count=fail_count+%s, status=%s WHERE id=%s",
                          (sent, fail, status, broadcast_id))
            else:
                c.execute("UPDATE broadcasts SET sent_count=sent_count+%s, fail_count=fail_count+%s WHERE id=%s",
                          (sent, fail, broadcast_id))

    def get_broadcasts(self, limit=20):
        with self._conn() as c:
            return c.execute("SELECT * FROM broadcasts ORDER BY created_at DESC LIMIT %s", (limit,)).fetchall()

    def get_all_user_ids(self, status_filter=None):
        with self._conn() as c:
            if status_filter:
                return [r['user_id'] for r in c.execute(
                    "SELECT user_id FROM users WHERE status=%s", (status_filter,)).fetchall()]
            return [r['user_id'] for r in c.execute("SELECT user_id FROM users").fetchall()]

    # ================= 🔗 VAQTINCHALIK HAVOLALAR =================
    def create_temp_link(self, test_id, created_by, expires_hours=24, max_uses=0):
        import secrets
        token = secrets.token_urlsafe(32)
        expires_at = int(time.time()) + expires_hours * 3600
        with self._conn() as c:
            c.execute("""INSERT INTO temp_links (token, test_id, created_by, expires_at, max_uses, use_count, created_at)
                VALUES (%s,%s,%s,%s,%s,0,%s)""",
                (token, test_id, created_by, expires_at, max_uses, int(time.time())))
        return token

    def get_temp_link(self, token):
        with self._conn() as c:
            row = c.execute("SELECT * FROM temp_links WHERE token=%s", (token,)).fetchone()
            if not row: return None
            row = dict(row)
            if row['expires_at'] < int(time.time()): return None
            if row['max_uses'] > 0 and row['use_count'] >= row['max_uses']: return None
            return row

    def use_temp_link(self, token):
        with self._conn() as c:
            c.execute("UPDATE temp_links SET use_count=use_count+1 WHERE token=%s", (token,))

    def get_test_temp_links(self, test_id):
        with self._conn() as c:
            return c.execute("SELECT * FROM temp_links WHERE test_id=%s ORDER BY created_at DESC", (test_id,)).fetchall()

    def delete_temp_link(self, token):
        with self._conn() as c:
            c.execute("DELETE FROM temp_links WHERE token=%s", (token,))

    # ================= 📦 SAVOL BANKI =================
    def add_to_question_bank(self, owner_id, question, options, correct_index, photo_id=None, category_id=None, tags=None):
        with self._conn() as c:
            c.execute("""INSERT INTO question_bank
                (owner_id, question, options_json, correct_index, photo_id, category_id, tags, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                (owner_id, question, json.dumps(options, ensure_ascii=False),
                 correct_index, photo_id, category_id, tags, int(time.time())))
            return c.lastrowid

    def get_question_bank(self, owner_id=None, category_id=None, limit=50):
        with self._conn() as c:
            sql = "SELECT * FROM question_bank WHERE 1=1"
            params = []
            if owner_id:
                sql += " AND owner_id=%s"
                params.append(owner_id)
            if category_id:
                sql += " AND category_id=%s"
                params.append(category_id)
            sql += " ORDER BY created_at DESC LIMIT %s"
            params.append(limit)
            return c.execute(sql, tuple(params)).fetchall()

    def import_test_to_bank(self, test_id, owner_id):
        """Testdagi barcha savollarni savol bankiga qo'shadi"""
        with self._conn() as c:
            qs = c.execute("SELECT * FROM questions WHERE test_id=%s", (test_id,)).fetchall()
        added = 0
        for q in qs:
            q = dict(q)
            self.add_to_question_bank(
                owner_id, q['question'],
                json.loads(q['options_json'] or '[]'),
                q['correct_index'], q.get('photo_id')
            )
            added += 1
        return added

    def create_test_from_bank(self, owner_id, question_ids, title, chat_id=None):
        """Savol bankidan test yaratish"""
        test_id = uuid.uuid4().hex[:10]
        chat_id = chat_id or owner_id
        self.create_test(test_id, owner_id, chat_id, title, 60, int(time.time()))
        with self._conn() as c:
            for i, qid in enumerate(question_ids):
                q = to_dict_safe(c.execute("SELECT * FROM question_bank WHERE id=%s", (qid,)).fetchone())
                if q:
                    c.execute("""INSERT INTO questions (test_id, q_index, question, options_json, correct_index, photo_id)
                        VALUES (%s,%s,%s,%s,%s,%s)""",
                        (test_id, i, q['question'], q['options_json'], q['correct_index'], q.get('photo_id')))
                    c.execute("UPDATE question_bank SET use_count=use_count+1 WHERE id=%s", (qid,))
        return test_id

    # ================= 📋 TEST NUSXALASH =================
    def copy_test(self, test_id, new_owner_id, new_title=None):
        """Testni nusxalash"""
        with self._conn() as c:
            test = to_dict_safe(c.execute("SELECT * FROM tests WHERE test_id=%s", (test_id,)).fetchone())
            if not test: return None
            qs = c.execute("SELECT * FROM questions WHERE test_id=%s ORDER BY q_index", (test_id,)).fetchall()

        new_id = uuid.uuid4().hex[:10]
        title = new_title or f"[Nusxa] {test['title']}"
        self.create_test(new_id, new_owner_id, new_owner_id, title,
                         test.get('per_question_sec', 60), int(time.time()),
                         scoring_type=test.get('scoring_type','standard'),
                         time_limit=test.get('time_limit', 0))
        with self._conn() as c:
            for q in qs:
                q = dict(q)
                c.execute("""INSERT INTO questions (test_id, q_index, question, options_json, correct_index, question_score, photo_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                    (new_id, q['q_index'], q['question'], q['options_json'],
                     q['correct_index'], q.get('question_score', 1.0), q.get('photo_id')))
        return new_id

    # ================= 🏆 SERTIFIKATLAR =================
    def issue_certificate(self, user_id, test_id, session_id, score):
        """Sertifikat chiqarish (faqat birinchi marta)"""
        import secrets
        with self._conn() as c:
            exists = c.execute(
                "SELECT cert_code FROM certificates WHERE user_id=%s AND test_id=%s",
                (user_id, test_id)).fetchone()
            if exists:
                return dict(exists)['cert_code']
            code = secrets.token_hex(8).upper()
            c.execute("""INSERT INTO certificates (user_id, test_id, session_id, score, cert_code, issued_at)
                VALUES (%s,%s,%s,%s,%s,%s)""",
                (user_id, test_id, session_id, score, code, int(time.time())))
            return code

    def get_certificate(self, cert_code):
        with self._conn() as c:
            row = c.execute("""SELECT c.*, t.title, u.first_name, u.username
                FROM certificates c
                JOIN tests t ON c.test_id=t.test_id
                JOIN users u ON c.user_id=u.user_id
                WHERE c.cert_code=%s""", (cert_code,)).fetchone()
            return dict(row) if row else None

    def get_user_certificates(self, user_id):
        with self._conn() as c:
            return c.execute("""SELECT c.*, t.title FROM certificates c
                JOIN tests t ON c.test_id=t.test_id
                WHERE c.user_id=%s ORDER BY c.issued_at DESC""", (user_id,)).fetchall()

    def get_test_certificate_count(self, test_id):
        with self._conn() as c:
            r = c.execute("SELECT COUNT(*) as cnt FROM certificates WHERE test_id=%s", (test_id,)).fetchone()
            return r['cnt'] if r else 0

    # ================= 📅 O'QUV REJASI =================
    def add_study_plan(self, group_id, week_start, day_of_week, test_id, note, created_by):
        with self._conn() as c:
            c.execute("""INSERT INTO study_plans (group_id, week_start, day_of_week, test_id, note, created_by, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                (group_id, week_start, day_of_week, test_id, note, created_by, int(time.time())))

    def get_study_plan(self, group_id, week_start=None):
        with self._conn() as c:
            if week_start:
                return c.execute("""SELECT sp.*, t.title FROM study_plans sp
                    JOIN tests t ON sp.test_id=t.test_id
                    WHERE sp.group_id=%s AND sp.week_start=%s ORDER BY sp.day_of_week""",
                    (group_id, week_start)).fetchall()
            return c.execute("""SELECT sp.*, t.title FROM study_plans sp
                JOIN tests t ON sp.test_id=t.test_id
                WHERE sp.group_id=%s ORDER BY sp.week_start DESC, sp.day_of_week""",
                (group_id,)).fetchall()

    def delete_study_plan_item(self, plan_id):
        with self._conn() as c:
            c.execute("DELETE FROM study_plans WHERE id=%s", (plan_id,))

    # ================= 📣 REKLAMALAR =================
    def create_ad(self, title, body, media_id, media_type, url, target, starts_at, ends_at, created_by):
        with self._conn() as c:
            c.execute("""INSERT INTO advertisements
                (title, body, media_id, media_type, url, target, starts_at, ends_at, created_by, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (title, body, media_id, media_type, url, target, starts_at, ends_at, created_by, int(time.time())))
            return c.lastrowid

    def get_active_ads(self, user_id=None):
        now = int(time.time())
        with self._conn() as c:
            ads = c.execute("""SELECT * FROM advertisements
                WHERE is_active=1 AND starts_at<=%s AND ends_at>=%s
                ORDER BY created_at DESC""", (now, now)).fetchall()
            if user_id is None:
                return ads
            seen = {r['ad_id'] for r in c.execute(
                "SELECT ad_id FROM ad_impressions WHERE user_id=%s", (user_id,)).fetchall()}
            return [a for a in ads if dict(a)['id'] not in seen]

    def record_ad_impression(self, ad_id, user_id, clicked=False):
        with self._conn() as c:
            c.execute("""INSERT INTO ad_impressions (ad_id, user_id, seen_at, clicked)
                VALUES (%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE seen_at=%s, clicked=IF(%s=1,1,clicked)""",
                (ad_id, user_id, int(time.time()), 1 if clicked else 0,
                 int(time.time()), 1 if clicked else 0))
            c.execute("UPDATE advertisements SET show_count=show_count+1 WHERE id=%s", (ad_id,))
            if clicked:
                c.execute("UPDATE advertisements SET click_count=click_count+1 WHERE id=%s", (ad_id,))

    def get_all_ads(self, active_only=False):
        with self._conn() as c:
            if active_only:
                return c.execute("SELECT * FROM advertisements WHERE is_active=1 ORDER BY created_at DESC").fetchall()
            return c.execute("SELECT * FROM advertisements ORDER BY created_at DESC").fetchall()

    def toggle_ad(self, ad_id, is_active):
        with self._conn() as c:
            c.execute("UPDATE advertisements SET is_active=%s WHERE id=%s", (is_active, ad_id))

    def delete_ad(self, ad_id):
        with self._conn() as c:
            c.execute("DELETE FROM advertisements WHERE id=%s", (ad_id,))
            c.execute("DELETE FROM ad_impressions WHERE ad_id=%s", (ad_id,))

    # ================= 🤝 AFFILIATE =================
    def get_or_create_affiliate(self, user_id):
        with self._conn() as c:
            row = c.execute("SELECT * FROM affiliates WHERE user_id=%s", (user_id,)).fetchone()
            if row: return dict(row)
            import secrets
            code = secrets.token_hex(4).upper()
            c.execute("""INSERT INTO affiliates (user_id, ref_code, created_at)
                VALUES (%s,%s,%s)""", (user_id, code, int(time.time())))
            return {"user_id": user_id, "ref_code": code, "total_referrals": 0,
                    "total_earned": 0, "balance": 0, "is_active": 1}

    def get_affiliate_stats(self, user_id):
        with self._conn() as c:
            aff = to_dict_safe(c.execute("SELECT * FROM affiliates WHERE user_id=%s", (user_id,)).fetchone())
            if not aff: return None
            refs = c.execute("SELECT COUNT(*) as cnt FROM users WHERE referrer_id=%s", (user_id,)).fetchone()
            premium_refs = c.execute("""SELECT COUNT(*) as cnt FROM users
                WHERE referrer_id=%s AND status='premium'""", (user_id,)).fetchone()
            aff['referral_count'] = refs['cnt'] if refs else 0
            aff['premium_referrals'] = premium_refs['cnt'] if premium_refs else 0
            return aff

    def add_affiliate_earning(self, user_id, amount):
        with self._conn() as c:
            c.execute("""UPDATE affiliates SET
                total_earned=total_earned+%s, balance=balance+%s, total_referrals=total_referrals+1
                WHERE user_id=%s""", (amount, amount, user_id))

    def get_top_affiliates(self, limit=20):
        with self._conn() as c:
            return c.execute("""SELECT a.*, u.first_name, u.username
                FROM affiliates a JOIN users u ON a.user_id=u.user_id
                WHERE a.is_active=1
                ORDER BY a.total_referrals DESC LIMIT %s""", (limit,)).fetchall()

    # ================= 🔐 2FA =================
    def set_2fa_code(self, user_id, phone, code, expires_minutes=10):
        expires_at = int(time.time()) + expires_minutes * 60
        with self._conn() as c:
            c.execute("""INSERT INTO twofa_codes (user_id, phone, code, expires_at, verified)
                VALUES (%s,%s,%s,%s,0)
                ON DUPLICATE KEY UPDATE phone=%s, code=%s, expires_at=%s, verified=0""",
                (user_id, phone, code, expires_at, phone, code, expires_at))

    def verify_2fa_code(self, user_id, code):
        with self._conn() as c:
            row = c.execute("""SELECT * FROM twofa_codes
                WHERE user_id=%s AND code=%s AND expires_at>%s AND verified=0""",
                (user_id, code, int(time.time()))).fetchone()
            if not row: return False
            c.execute("UPDATE twofa_codes SET verified=1 WHERE user_id=%s", (user_id,))
            c.execute("UPDATE users SET twofa_enabled=1 WHERE user_id=%s", (user_id,))
            return True

    def disable_2fa(self, user_id):
        with self._conn() as c:
            c.execute("UPDATE users SET twofa_enabled=0 WHERE user_id=%s", (user_id,))
            c.execute("DELETE FROM twofa_codes WHERE user_id=%s", (user_id,))

    # ================= 🚫 BAN SABABI =================
    def ban_user_with_reason(self, user_id, admin_id, reason):
        with self._conn() as c:
            c.execute("UPDATE users SET status='banned', ban_reason=%s WHERE user_id=%s", (reason, user_id))
            c.execute("""INSERT INTO ban_logs (user_id, admin_id, reason, action, created_at)
                VALUES (%s,%s,%s,'ban',%s)""", (user_id, admin_id, reason, int(time.time())))

    def unban_user(self, user_id, admin_id, reason="Unban"):
        with self._conn() as c:
            c.execute("UPDATE users SET status='free', ban_reason=NULL WHERE user_id=%s", (user_id,))
            c.execute("""INSERT INTO ban_logs (user_id, admin_id, reason, action, created_at)
                VALUES (%s,%s,%s,'unban',%s)""", (user_id, admin_id, reason, int(time.time())))

    def get_ban_history(self, user_id):
        with self._conn() as c:
            return c.execute("""SELECT bl.*, u.first_name as admin_name
                FROM ban_logs bl LEFT JOIN users u ON bl.admin_id=u.user_id
                WHERE bl.user_id=%s ORDER BY bl.created_at DESC""", (user_id,)).fetchall()

    # ================= 🌐 IP WHITELIST =================
    def add_ip_whitelist(self, ip, label, added_by):
        with self._conn() as c:
            c.execute("INSERT IGNORE INTO ip_whitelist (ip, label, added_by, added_at) VALUES (%s,%s,%s,%s)",
                      (ip, label, added_by, int(time.time())))

    def remove_ip_whitelist(self, ip):
        with self._conn() as c:
            c.execute("DELETE FROM ip_whitelist WHERE ip=%s", (ip,))

    def get_ip_whitelist(self):
        with self._conn() as c:
            return c.execute("SELECT * FROM ip_whitelist ORDER BY added_at DESC").fetchall()

    def is_ip_whitelisted(self, ip):
        with self._conn() as c:
            return bool(c.execute("SELECT 1 FROM ip_whitelist WHERE ip=%s", (ip,)).fetchone())

    # ================= 🔬 A/B TEST =================
    def create_ab_test(self, test_id, q_index, variant_a, variant_b,
                       options_a, options_b, correct_a, correct_b):
        with self._conn() as c:
            c.execute("""INSERT INTO ab_tests
                (test_id, q_index, variant_a, variant_b, options_a_json, options_b_json,
                 correct_a, correct_b, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                variant_a=%s, variant_b=%s, options_a_json=%s, options_b_json=%s,
                correct_a=%s, correct_b=%s""",
                (test_id, q_index, variant_a, variant_b,
                 json.dumps(options_a, ensure_ascii=False),
                 json.dumps(options_b, ensure_ascii=False),
                 correct_a, correct_b, int(time.time()),
                 variant_a, variant_b,
                 json.dumps(options_a, ensure_ascii=False),
                 json.dumps(options_b, ensure_ascii=False),
                 correct_a, correct_b))

    def record_ab_result(self, test_id, q_index, variant, is_correct):
        with self._conn() as c:
            if variant == 'a':
                c.execute("""UPDATE ab_tests SET a_count=a_count+1,
                    a_correct=a_correct+%s WHERE test_id=%s AND q_index=%s""",
                    (1 if is_correct else 0, test_id, q_index))
            else:
                c.execute("""UPDATE ab_tests SET b_count=b_count+1,
                    b_correct=b_correct+%s WHERE test_id=%s AND q_index=%s""",
                    (1 if is_correct else 0, test_id, q_index))

    def get_ab_results(self, test_id):
        with self._conn() as c:
            return c.execute("SELECT * FROM ab_tests WHERE test_id=%s", (test_id,)).fetchall()

    # ================= 🃏 FLASHCARD =================
    def create_flashcard_set(self, owner_id, title, description="", is_public=0, category_id=None):
        with self._conn() as c:
            c.execute("""INSERT INTO flashcard_sets (owner_id, title, description, category_id, is_public, created_at)
                VALUES (%s,%s,%s,%s,%s,%s)""",
                (owner_id, title, description, category_id, is_public, int(time.time())))
            return c.lastrowid

    def add_flashcard(self, set_id, front, back, hint="", photo_id=None, difficulty=1):
        with self._conn() as c:
            c.execute("""INSERT INTO flashcards (set_id, front, back, hint, photo_id, difficulty, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                (set_id, front, back, hint, photo_id, difficulty, int(time.time())))
            return c.lastrowid

    def get_flashcard_set(self, set_id):
        with self._conn() as c:
            return to_dict_safe(c.execute("SELECT * FROM flashcard_sets WHERE id=%s", (set_id,)).fetchone())

    def get_flashcards(self, set_id, user_id=None):
        with self._conn() as c:
            cards = c.execute("SELECT * FROM flashcards WHERE set_id=%s ORDER BY id", (set_id,)).fetchall()
            if not user_id:
                return cards
            # Progress bilan qaytarish
            result = []
            for card in cards:
                card = dict(card)
                prog = c.execute("SELECT * FROM flashcard_progress WHERE user_id=%s AND card_id=%s",
                                 (user_id, card['id'])).fetchone()
                card['progress'] = dict(prog) if prog else {'correct_count': 0, 'wrong_count': 0}
                result.append(card)
            return result

    def get_user_flashcard_sets(self, user_id):
        with self._conn() as c:
            return c.execute("""SELECT fs.*, COUNT(f.id) as card_count
                FROM flashcard_sets fs
                LEFT JOIN flashcards f ON fs.id=f.set_id
                WHERE fs.owner_id=%s
                GROUP BY fs.id
                ORDER BY fs.created_at DESC""", (user_id,)).fetchall()

    def get_public_flashcard_sets(self, limit=50):
        with self._conn() as c:
            return c.execute("""SELECT fs.*, COUNT(f.id) as card_count, u.first_name
                FROM flashcard_sets fs
                LEFT JOIN flashcards f ON fs.id=f.set_id
                JOIN users u ON fs.owner_id=u.user_id
                WHERE fs.is_public=1
                GROUP BY fs.id
                ORDER BY fs.created_at DESC LIMIT %s""", (limit,)).fetchall()

    def record_flashcard_answer(self, user_id, card_id, is_correct):
        with self._conn() as c:
            if is_correct:
                c.execute("""INSERT INTO flashcard_progress (user_id, card_id, correct_count, wrong_count, last_seen)
                    VALUES (%s,%s,1,0,%s)
                    ON DUPLICATE KEY UPDATE correct_count=correct_count+1, last_seen=%s""",
                    (user_id, card_id, int(time.time()), int(time.time())))
            else:
                c.execute("""INSERT INTO flashcard_progress (user_id, card_id, correct_count, wrong_count, last_seen)
                    VALUES (%s,%s,0,1,%s)
                    ON DUPLICATE KEY UPDATE wrong_count=wrong_count+1, last_seen=%s""",
                    (user_id, card_id, int(time.time()), int(time.time())))

    # ================= 🤖 AI IZOHLAR =================
    def get_ai_explanation(self, test_id, q_index, lang='uz'):
        with self._conn() as c:
            row = c.execute("SELECT explanation FROM ai_explanations WHERE test_id=%s AND q_index=%s AND lang=%s",
                            (test_id, q_index, lang)).fetchone()
            return row['explanation'] if row else None

    def save_ai_explanation(self, test_id, q_index, explanation, lang='uz'):
        with self._conn() as c:
            c.execute("""INSERT INTO ai_explanations (test_id, q_index, explanation, lang, created_at)
                VALUES (%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE explanation=%s, created_at=%s""",
                (test_id, q_index, explanation, lang, int(time.time()),
                 explanation, int(time.time())))

    # ================= 📊 FOYDALANUVCHI XARITASI (Soatlik faollik) =================
    def get_user_activity_heatmap(self, user_id):
        """Foydalanuvchi qaysi soatlarda faolroq (0-23 soat bo'yicha)"""
        with self._conn() as c:
            rows = c.execute("""
                SELECT HOUR(FROM_UNIXTIME(finished_at)) as hour_val,
                       COUNT(*) as cnt
                FROM sessions
                WHERE user_id=%s AND state='finished'
                GROUP BY hour_val ORDER BY hour_val
            """, (user_id,)).fetchall()
            result = {i: 0 for i in range(24)}
            for r in rows:
                result[int(r['hour_val'])] = int(r['cnt'])
            return result

    # ================= 🔧 YORDAMCHI =================

def to_dict_safe(row):
    """PyMySQL Row yoki None ni xavfsiz dict ga aylantiradi"""
    if row is None: return None
    return dict(row)


    # ================= 📄 PAGINATION =================
    def get_tests_paginated(self, owner_id, page=1, limit=10):
        """Sahifalash bilan testlarni olish"""
        offset = (page - 1) * limit
        with self._conn() as c:
            rows = c.execute("""
                SELECT t.*, COUNT(DISTINCT s.session_id) as finished_count,
                       COUNT(DISTINCT q.q_index) as q_count
                FROM tests t
                LEFT JOIN sessions s ON t.test_id=s.test_id AND s.state='finished'
                LEFT JOIN questions q ON t.test_id=q.test_id
                WHERE t.owner_user_id=%s
                GROUP BY t.test_id
                ORDER BY t.created_at DESC
                LIMIT %s OFFSET %s
            """, (owner_id, limit, offset)).fetchall()
            total = c.execute(
                "SELECT COUNT(*) as cnt FROM tests WHERE owner_user_id=%s", (owner_id,)
            ).fetchone()
            total_count = total['cnt'] if total else 0
        return [dict(r) for r in rows], total_count

    def get_public_tests_paginated(self, query=None, category_id=None, page=1, limit=12):
        """Ommaviy testlar — sahifalash"""
        offset = (page - 1) * limit
        with self._conn() as c:
            sql = """SELECT t.*, COUNT(DISTINCT s.session_id) as plays,
                            AVG(r.rating) as avg_rating
                     FROM tests t
                     LEFT JOIN sessions s ON t.test_id=s.test_id AND s.state='finished'
                     LEFT JOIN test_reviews r ON t.test_id=r.test_id
                     WHERE t.public_name IS NOT NULL AND t.status='open'"""
            params = []
            if query:
                sql += " AND (t.title LIKE %s OR t.public_name LIKE %s)"
                params.extend([f"%{query}%", f"%{query}%"])
            if category_id:
                sql += " AND t.category_id=%s"
                params.append(category_id)
            count_sql = sql.replace(
                "SELECT t.*, COUNT(DISTINCT s.session_id) as plays,\n                            AVG(r.rating) as avg_rating",
                "SELECT COUNT(DISTINCT t.test_id) as cnt"
            ).split("GROUP BY")[0]
            sql += " GROUP BY t.test_id ORDER BY plays DESC LIMIT %s OFFSET %s"
            rows = c.execute(sql, tuple(params + [limit, offset])).fetchall()
            total = c.execute(count_sql, tuple(params)).fetchone()
            total_count = total['cnt'] if total else 0
        return [dict(r) for r in rows], total_count

    # ================= 📅 SCHEDULED TESTS =================
    def schedule_test_open(self, test_id, open_at_ts):
        """Testni ma'lum vaqtda ochish uchun rejalashtirish"""
        with self._conn() as c:
            c.execute("UPDATE tests SET status='scheduled', deadline_ts=%s WHERE test_id=%s",
                      (open_at_ts, test_id))

    def get_scheduled_tests_due(self):
        """Hozir ochilishi kerak bo'lgan rejalashtirilgan testlar"""
        now = int(time.time())
        with self._conn() as c:
            return c.execute("""
                SELECT * FROM tests
                WHERE status='scheduled' AND deadline_ts IS NOT NULL AND deadline_ts <= %s
            """, (now,)).fetchall()

    def get_upcoming_tests(self, hours=24):
        """Kelgusi N soat ichida ochilishi rejalashtirilgan testlar"""
        now = int(time.time())
        soon = now + hours * 3600
        with self._conn() as c:
            return c.execute("""
                SELECT t.*, u.first_name, u.username
                FROM tests t
                JOIN users u ON t.owner_user_id=u.user_id
                WHERE t.status='scheduled' AND t.deadline_ts BETWEEN %s AND %s
            """, (now, soon)).fetchall()


    # ================= ⚙️ BOT SOZLAMALARI =================
    def _seed_bot_settings(self, c):
        """Boshlang'ich bot sozlamalarini yaratish"""
        defaults = [
            ("maintenance_mode",    "0",   0,   "Texnik ishlar rejimi (0/1)"),
            ("ai_daily_limit",      "10",  10,  "Har foydalanuvchi uchun kunlik AI chaqiruvi"),
            ("max_q_per_test",      "100", 100, "Testdagi maksimal savollar soni"),
            ("min_q_per_test",      "1",   1,   "Testdagi minimal savollar soni"),
            ("premium_price_1",     "29000", None, "1 oylik premium narxi (so'm)"),
            ("premium_price_3",     "79000", None, "3 oylik premium narxi (so'm)"),
            ("premium_price_6",     "149000", None, "6 oylik premium narxi (so'm)"),
            ("premium_price_12",    "279000", None, "12 oylik premium narxi (so'm)"),
            ("gwt_price_usd",       "10",   None, "1 GWT = ? USD"),
            ("staking_apy",         "12",   12,   "Staking foizi (yillik %)"),
            ("staking_lock_days",   "30",   30,   "Staking qulflash muddati (kun)"),
            ("challenge_expire_h",  "24",   24,   "Challenge muddati (soat)"),
            ("auto_moderation",     "1",    1,    "Avtomatik moderatsiya (0/1)"),
            ("registration_bonus",  "1",    1,    "Yangi foydalanuvchi uchun GWT bonus"),
            ("referral_premium_n",  "10",   10,   "Nechta referal = 1 oy premium"),
            ("max_broadcast_delay", "5",    5,    "Broadcast orasidagi interval (ms*10)"),
            ("site_name",           "Geo Ustoz", None, "Sayt nomi"),
            ("support_username",    "@support",  None, "Support username"),
        ]
        for key, val_text, val_int, desc in defaults:
            try:
                c.execute("""INSERT IGNORE INTO bot_settings
                    (key_name, value_text, value_int, description, updated_at)
                    VALUES (%s, %s, %s, %s, %s)""",
                    (key, val_text, val_int, desc, int(time.time())))
            except Exception:
                pass

    def get_setting(self, key, default=None):
        """Bot sozlamasini olish"""
        with self._conn() as c:
            row = c.execute("SELECT value_text, value_int FROM bot_settings WHERE key_name=%s", (key,)).fetchone()
            if not row:
                return default
            row = dict(row)
            if row.get('value_int') is not None:
                return row['value_int']
            return row.get('value_text') or default

    def set_setting(self, key, value, admin_id=0):
        """Bot sozlamasini yangilash"""
        with self._conn() as c:
            val_int = None
            try:
                val_int = int(value)
            except (ValueError, TypeError):
                pass
            c.execute("""INSERT INTO bot_settings (key_name, value_text, value_int, updated_by, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    value_text=%s, value_int=%s, updated_by=%s, updated_at=%s""",
                (key, str(value), val_int, admin_id, int(time.time()),
                 str(value), val_int, admin_id, int(time.time())))

    def get_all_settings(self):
        """Barcha sozlamalarni olish"""
        with self._conn() as c:
            return c.execute("SELECT * FROM bot_settings ORDER BY key_name").fetchall()

    # ================= 🎯 ADAPTIVE TESTING =================
    def get_adaptive_level(self, user_id, q_bank_id):
        with self._conn() as c:
            row = c.execute("SELECT difficulty_level FROM adaptive_progress WHERE user_id=%s AND q_bank_id=%s",
                            (user_id, q_bank_id)).fetchone()
            return int(row['difficulty_level']) if row else 1

    def update_adaptive_progress(self, user_id, q_bank_id, is_correct):
        with self._conn() as c:
            c.execute("""INSERT INTO adaptive_progress (user_id, q_bank_id, correct_streak, wrong_streak, difficulty_level, last_seen)
                VALUES (%s, %s, %s, %s, 1, %s)
                ON DUPLICATE KEY UPDATE
                    correct_streak = IF(%s=1, correct_streak+1, 0),
                    wrong_streak   = IF(%s=0, wrong_streak+1, 0),
                    difficulty_level = GREATEST(1, LEAST(5,
                        difficulty_level + IF(%s=1 AND correct_streak>=2, 1, IF(%s=0 AND wrong_streak>=2, -1, 0))
                    )),
                    last_seen = %s""",
                (user_id, q_bank_id, 1 if is_correct else 0, 0 if is_correct else 1, int(time.time()),
                 1 if is_correct else 0, 1 if is_correct else 0,
                 1 if is_correct else 0, 1 if is_correct else 0,
                 int(time.time())))

    def get_adaptive_questions(self, user_id, category_id=None, limit=10):
        """Foydalanuvchi darajasiga mos savollarni olish"""
        with self._conn() as c:
            level = 3  # default o'rta daraja
            if category_id:
                rows = c.execute("""SELECT AVG(difficulty_level) as avg_lvl
                    FROM adaptive_progress ap
                    JOIN question_bank qb ON ap.q_bank_id=qb.id
                    WHERE ap.user_id=%s AND qb.category_id=%s""",
                    (user_id, category_id)).fetchone()
                if rows and rows['avg_lvl']:
                    level = int(rows['avg_lvl'])
            # O'sha darajadagi savollarni olish
            return c.execute("""
                SELECT qb.* FROM question_bank qb
                LEFT JOIN adaptive_progress ap ON qb.id=ap.q_bank_id AND ap.user_id=%s
                WHERE (%s IS NULL OR qb.category_id=%s)
                ORDER BY ABS(COALESCE(ap.difficulty_level, 3) - %s) ASC, RAND()
                LIMIT %s
            """, (user_id, category_id, category_id, level, limit)).fetchall()

    # ================= 📝 FREE-TEXT SAVOLLAR =================
    def save_freetext_answer(self, session_id, q_index, user_answer):
        with self._conn() as c:
            c.execute("""INSERT INTO freetext_answers (session_id, q_index, user_answer)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE user_answer=%s""",
                (session_id, q_index, user_answer, user_answer))

    def get_freetext_answers(self, session_id):
        with self._conn() as c:
            return c.execute("SELECT * FROM freetext_answers WHERE session_id=%s", (session_id,)).fetchall()

    def save_freetext_ai_score(self, freetext_id, score, feedback):
        with self._conn() as c:
            c.execute("UPDATE freetext_answers SET ai_score=%s, ai_feedback=%s, checked_at=%s WHERE id=%s",
                      (score, feedback, int(time.time()), freetext_id))

    def get_pending_freetext(self, limit=20):
        """AI tomonidan hali baholanmagan free-text javoblar"""
        with self._conn() as c:
            return c.execute("""SELECT fa.*, s.test_id, s.user_id
                FROM freetext_answers fa
                JOIN sessions s ON fa.session_id=s.session_id
                WHERE fa.ai_score IS NULL
                ORDER BY fa.id ASC LIMIT %s""", (limit,)).fetchall()

    # ================= 🎟️ KUPON KODLARI =================
    def create_coupon(self, code, discount_pct=0, months_free=0, gwt_bonus=0,
                      max_uses=0, expires_hours=None, created_by=0):
        expires_at = int(time.time()) + expires_hours * 3600 if expires_hours else None
        with self._conn() as c:
            c.execute("""INSERT INTO coupons
                (code, discount_pct, months_free, gwt_bonus, max_uses, expires_at, created_by, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (code.upper(), discount_pct, months_free, float(gwt_bonus),
                 max_uses, expires_at, created_by, int(time.time())))
            return c.lastrowid

    def get_coupon(self, code):
        with self._conn() as c:
            row = c.execute("SELECT * FROM coupons WHERE code=%s AND is_active=1", (code.upper(),)).fetchone()
            if not row:
                return None, "Kupon topilmadi"
            row = dict(row)
            if row.get('expires_at') and row['expires_at'] < int(time.time()):
                return None, "Kupon muddati tugagan"
            if row['max_uses'] > 0 and row['use_count'] >= row['max_uses']:
                return None, "Kupon limiti tugagan"
            return row, None

    def use_coupon(self, coupon_id, user_id):
        with self._conn() as c:
            exists = c.execute("SELECT 1 FROM coupon_uses WHERE coupon_id=%s AND user_id=%s",
                               (coupon_id, user_id)).fetchone()
            if exists:
                return False, "Siz bu kuponni allaqachon ishlatgansiz"
            c.execute("INSERT INTO coupon_uses (coupon_id, user_id, used_at) VALUES (%s, %s, %s)",
                      (coupon_id, user_id, int(time.time())))
            c.execute("UPDATE coupons SET use_count=use_count+1 WHERE id=%s", (coupon_id,))
            return True, None

    def get_all_coupons(self, active_only=False):
        with self._conn() as c:
            if active_only:
                return c.execute("SELECT * FROM coupons WHERE is_active=1 ORDER BY created_at DESC").fetchall()
            return c.execute("SELECT * FROM coupons ORDER BY created_at DESC").fetchall()

    def toggle_coupon(self, coupon_id, is_active):
        with self._conn() as c:
            c.execute("UPDATE coupons SET is_active=%s WHERE id=%s", (is_active, coupon_id))

    def delete_coupon(self, coupon_id):
        with self._conn() as c:
            c.execute("DELETE FROM coupon_uses WHERE coupon_id=%s", (coupon_id,))
            c.execute("DELETE FROM coupons WHERE id=%s", (coupon_id,))

    # ================= ⚔️ CHALLENGE (BELLASHUV) =================
    def create_challenge(self, test_id, challenger_id, challenged_id, gwt_bet=0, expire_hours=24):
        expires_at = int(time.time()) + expire_hours * 3600
        with self._conn() as c:
            c.execute("""INSERT INTO challenges
                (test_id, challenger_id, challenged_id, gwt_bet, created_at, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s)""",
                (test_id, challenger_id, challenged_id, float(gwt_bet), int(time.time()), expires_at))
            return c.lastrowid

    def get_challenge(self, challenge_id):
        with self._conn() as c:
            return c.execute("SELECT * FROM challenges WHERE id=%s", (challenge_id,)).fetchone()

    def get_user_challenges(self, user_id):
        with self._conn() as c:
            return c.execute("""
                SELECT c.*, t.title,
                    u1.first_name as challenger_name, u2.first_name as challenged_name
                FROM challenges c
                JOIN tests t ON c.test_id=t.test_id
                JOIN users u1 ON c.challenger_id=u1.user_id
                JOIN users u2 ON c.challenged_id=u2.user_id
                WHERE (c.challenger_id=%s OR c.challenged_id=%s)
                    AND c.status != 'expired'
                ORDER BY c.created_at DESC
            """, (user_id, user_id)).fetchall()

    def submit_challenge_score(self, challenge_id, user_id, score):
        with self._conn() as c:
            ch = to_dict_safe(c.execute("SELECT * FROM challenges WHERE id=%s", (challenge_id,)).fetchone())
            if not ch:
                return False, "Challenge topilmadi"
            if ch['expires_at'] < int(time.time()):
                c.execute("UPDATE challenges SET status='expired' WHERE id=%s", (challenge_id,))
                return False, "Challenge muddati tugagan"
            if user_id == ch['challenger_id']:
                c.execute("UPDATE challenges SET challenger_score=%s, challenger_done=1 WHERE id=%s",
                          (score, challenge_id))
            elif user_id == ch['challenged_id']:
                c.execute("UPDATE challenges SET challenged_score=%s, challenged_done=1 WHERE id=%s",
                          (score, challenge_id))
            else:
                return False, "Bu challenge sizga tegishli emas"
            # Ikkisi ham yakunlagan bo'lsa, g'olibni aniqlash
            ch = to_dict_safe(c.execute("SELECT * FROM challenges WHERE id=%s", (challenge_id,)).fetchone())
            if ch and ch.get('challenger_done') and ch.get('challenged_done'):
                c_score = float(ch.get('challenger_score') or 0)
                d_score = float(ch.get('challenged_score') or 0)
                winner_id = ch['challenger_id'] if c_score >= d_score else ch['challenged_id']
                c.execute("UPDATE challenges SET status='finished', winner_id=%s WHERE id=%s",
                          (winner_id, challenge_id))
                return True, winner_id
            return True, None

    # ================= 💎 STAKING =================
    def start_staking(self, user_id, amount, lock_days=30):
        # get_setting xavfsiz chaqiruv
        try:
            apy = float(self.get_setting('staking_apy', 12) or 12)
        except Exception:
            apy = 12.0
        unlock_at = int(time.time()) + int(lock_days) * 86400
        try:
            with self._conn() as c:
                # Staking jadvali borligini tekshirish
                try:
                    c.execute('''CREATE TABLE IF NOT EXISTS staking (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id BIGINT,
                        amount DECIMAL(10,2),
                        apy DECIMAL(5,2) DEFAULT 12.0,
                        start_at BIGINT,
                        unlock_at BIGINT,
                        last_reward_at BIGINT,
                        total_reward DECIMAL(10,2) DEFAULT 0,
                        is_active TINYINT DEFAULT 1
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4''')
                except Exception:
                    pass
                # Balans tekshirish
                wallet = c.execute("SELECT balance FROM wallets WHERE user_id=%s", (user_id,)).fetchone()
                if not wallet or float(dict(wallet)['balance']) < float(amount):
                    return False, "Yetarli GWT yo'q"
                # Walletdan yechib olish
                c.execute("UPDATE wallets SET balance=balance-%s WHERE user_id=%s", (float(amount), user_id))
                c.execute("""INSERT INTO staking (user_id, amount, apy, start_at, unlock_at, last_reward_at)
                    VALUES (%s, %s, %s, %s, %s, %s)""",
                    (user_id, float(amount), apy, int(time.time()), unlock_at, int(time.time())))
                return True, c.lastrowid
        except Exception as e:
            logging.error(f"start_staking xato: {e}")
            return False, f"Xato: {e}"

    def get_user_staking(self, user_id):
        try:
            with self._conn() as c:
                return c.execute("SELECT * FROM staking WHERE user_id=%s AND is_active=1 ORDER BY start_at DESC",
                                 (user_id,)).fetchall()
        except Exception as e:
            logging.error(f"get_user_staking xato: {e}")
            return []

    def unstake(self, staking_id, user_id):
        try:
            with self._conn() as c:
                stake = to_dict_safe(c.execute("SELECT * FROM staking WHERE id=%s AND user_id=%s AND is_active=1",
                                               (staking_id, user_id)).fetchone())
                if not stake:
                    return False, "Staking topilmadi"
                now = int(time.time())
                unlock_at = int(stake.get('unlock_at') or 0)
                early = now < unlock_at
                amount = float(stake.get('amount') or 0)
                reward = float(stake.get('total_reward') or 0)
                if early:
                    penalty = amount * 0.1
                    amount = amount - penalty
                c.execute("UPDATE staking SET is_active=0 WHERE id=%s", (staking_id,))
                c.execute("UPDATE wallets SET balance=balance+%s WHERE user_id=%s", (amount + reward, user_id))
                return True, {"amount": amount, "reward": reward, "early": early}
        except Exception as e:
            logging.error(f"unstake xato: {e}")
            return False, f"Xato: {e}"

    def process_staking_rewards(self):
        """Kunlik staking mukofotlarini hisoblash (scheduler uchun)"""
        now = int(time.time())
        day_ago = now - 86400
        with self._conn() as c:
            stakes = c.execute("""SELECT * FROM staking
                WHERE is_active=1 AND last_reward_at < %s""", (day_ago,)).fetchall()
            for s in stakes:
                s = dict(s)
                daily_rate = float(s.get('apy', 12)) / 365 / 100
                reward = float(s.get('amount', 0)) * daily_rate
                c.execute("""UPDATE staking SET
                    total_reward=total_reward+%s,
                    last_reward_at=%s
                    WHERE id=%s""", (reward, now, s['id']))
                c.execute("UPDATE wallets SET balance=balance+%s WHERE user_id=%s",
                          (reward, s['user_id']))

    def get_all_staking_stats(self):
        with self._conn() as c:
            return c.execute("""SELECT u.first_name, u.username, s.*
                FROM staking s JOIN users u ON s.user_id=u.user_id
                WHERE s.is_active=1 ORDER BY s.amount DESC""").fetchall()

    # ================= 📊 SAVOL REPORT =================
    def report_question(self, test_id, q_index, user_id, report_type, comment=""):
        with self._conn() as c:
            c.execute("""INSERT INTO question_reports
                (test_id, q_index, user_id, report_type, comment, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)""",
                (test_id, q_index, user_id, report_type, comment, int(time.time())))

    def get_question_reports(self, status='pending', limit=50):
        with self._conn() as c:
            return c.execute("""SELECT qr.*, t.title as test_title, q.question
                FROM question_reports qr
                JOIN tests t ON qr.test_id=t.test_id
                JOIN questions q ON qr.test_id=q.test_id AND qr.q_index=q.q_index
                WHERE qr.status=%s
                ORDER BY qr.created_at DESC LIMIT %s""", (status, limit)).fetchall()

    def resolve_report(self, report_id, status='resolved'):
        with self._conn() as c:
            c.execute("UPDATE question_reports SET status=%s WHERE id=%s", (status, report_id))

    # ================= 💬 TEST IZOHLARI =================
    def add_test_comment(self, test_id, user_id, comment, parent_id=None):
        with self._conn() as c:
            c.execute("""INSERT INTO test_comments (test_id, user_id, comment, parent_id, created_at)
                VALUES (%s, %s, %s, %s, %s)""",
                (test_id, user_id, comment, parent_id, int(time.time())))
            return c.lastrowid

    def get_test_comments(self, test_id, limit=50):
        with self._conn() as c:
            return c.execute("""SELECT tc.*, u.first_name, u.username
                FROM test_comments tc JOIN users u ON tc.user_id=u.user_id
                WHERE tc.test_id=%s AND tc.parent_id IS NULL
                ORDER BY tc.created_at DESC LIMIT %s""", (test_id, limit)).fetchall()

    def like_comment(self, comment_id):
        with self._conn() as c:
            c.execute("UPDATE test_comments SET likes=likes+1 WHERE id=%s", (comment_id,))

    def delete_comment(self, comment_id, user_id=None, is_admin=False):
        with self._conn() as c:
            if is_admin:
                c.execute("DELETE FROM test_comments WHERE id=%s OR parent_id=%s",
                          (comment_id, comment_id))
            else:
                c.execute("DELETE FROM test_comments WHERE id=%s AND user_id=%s",
                          (comment_id, user_id))

    # ================= 📧 EMAIL HISOBOT =================
    def subscribe_email_report(self, user_id, email, frequency='weekly'):
        with self._conn() as c:
            c.execute("""INSERT INTO email_reports (user_id, email, frequency, created_at)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE email=%s, frequency=%s, is_active=1""",
                (user_id, email, frequency, int(time.time()), email, frequency))

    def unsubscribe_email_report(self, user_id):
        with self._conn() as c:
            c.execute("UPDATE email_reports SET is_active=0 WHERE user_id=%s", (user_id,))

    def get_email_report_subscribers(self, frequency='weekly'):
        now = int(time.time())
        if frequency == 'weekly':
            last_sent_before = now - 7 * 86400
        else:
            last_sent_before = now - 30 * 86400
        with self._conn() as c:
            return c.execute("""SELECT er.*, u.first_name, u.username
                FROM email_reports er JOIN users u ON er.user_id=u.user_id
                WHERE er.is_active=1 AND er.frequency=%s
                AND (er.last_sent IS NULL OR er.last_sent < %s)""",
                (frequency, last_sent_before)).fetchall()

    def mark_email_sent(self, report_id):
        with self._conn() as c:
            c.execute("UPDATE email_reports SET last_sent=%s WHERE id=%s",
                      (int(time.time()), report_id))

    # ================= 🔧 MODERATSIYA QOIDALARI =================
    def add_moderation_rule(self, rule_type, value, added_by=0):
        with self._conn() as c:
            c.execute("""INSERT INTO moderation_rules (rule_type, value, added_by, created_at)
                VALUES (%s, %s, %s, %s)""",
                (rule_type, value, added_by, int(time.time())))

    def get_moderation_rules(self, rule_type=None):
        with self._conn() as c:
            if rule_type:
                return c.execute("SELECT * FROM moderation_rules WHERE rule_type=%s AND is_active=1",
                                 (rule_type,)).fetchall()
            return c.execute("SELECT * FROM moderation_rules WHERE is_active=1 ORDER BY rule_type").fetchall()

    def toggle_moderation_rule(self, rule_id, is_active):
        with self._conn() as c:
            c.execute("UPDATE moderation_rules SET is_active=%s WHERE id=%s", (is_active, rule_id))

    def delete_moderation_rule(self, rule_id):
        with self._conn() as c:
            c.execute("DELETE FROM moderation_rules WHERE id=%s", (rule_id,))

    def check_custom_moderation(self, text):
        """Maxsus taqiqlangan so'zlarni tekshirish"""
        if not text:
            return True
        rules = self.get_moderation_rules('banned_word')
        text_lower = text.lower()
        for r in rules:
            if dict(r).get('value', '').lower() in text_lower:
                return False
        return True

    # ================= 🎓 GURUH SUBSCRIPTION =================
    def create_group_subscription(self, group_id, plan, max_members, months, price, created_by):
        starts_at = int(time.time())
        expires_at = starts_at + months * 30 * 86400
        with self._conn() as c:
            c.execute("""INSERT INTO group_subscriptions
                (group_id, plan, max_members, months, price_paid, starts_at, expires_at, created_by, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (group_id, plan, max_members, months, float(price),
                 starts_at, expires_at, created_by, starts_at))

    def get_group_subscription(self, group_id):
        now = int(time.time())
        with self._conn() as c:
            return c.execute("""SELECT * FROM group_subscriptions
                WHERE group_id=%s AND is_active=1 AND expires_at > %s
                ORDER BY created_at DESC LIMIT 1""", (group_id, now)).fetchone()

    def get_all_group_subscriptions(self):
        with self._conn() as c:
            return c.execute("""SELECT gs.*, g.name as group_name
                FROM group_subscriptions gs
                JOIN study_groups g ON gs.group_id=g.group_id
                ORDER BY gs.created_at DESC""").fetchall()

    # ================= 📊 BULK OPERATIONS =================
    def bulk_give_premium(self, user_ids, months, admin_id):
        """Bir nechta foydalanuvchiga birdan premium berish"""
        count = 0
        for uid in user_ids:
            try:
                self.add_premium_months(uid, months)
                count += 1
            except Exception as e:
                logging.error(f"Bulk premium xato uid={uid}: {e}")
        return count

    def bulk_ban_users(self, user_ids, admin_id, reason="Bulk ban"):
        """Bir nechta foydalanuvchini birdan ban qilish"""
        count = 0
        for uid in user_ids:
            try:
                self.ban_user_with_reason(uid, admin_id, reason)
                count += 1
            except Exception as e:
                logging.error(f"Bulk ban xato uid={uid}: {e}")
        return count

    def bulk_archive_tests(self, test_ids, user_id):
        """Bir nechta testni birdan arxivlash"""
        with self._conn() as c:
            for tid in test_ids:
                c.execute("UPDATE tests SET status='archived' WHERE test_id=%s AND owner_user_id=%s",
                          (tid, user_id))
        return len(test_ids)

    # ================= 🎲 KO'P TO'G'RI JAVOBLAR =================
    def set_multi_correct(self, test_id, q_index, correct_indices):
        """Bir savolda bir nechta to'g'ri javob"""
        with self._conn() as c:
            c.execute("""INSERT INTO multi_correct_answers (test_id, q_index, correct_indices_json)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE correct_indices_json=%s""",
                (test_id, q_index, json.dumps(correct_indices), json.dumps(correct_indices)))

    def get_multi_correct(self, test_id, q_index):
        with self._conn() as c:
            row = c.execute("SELECT correct_indices_json FROM multi_correct_answers WHERE test_id=%s AND q_index=%s",
                            (test_id, q_index)).fetchone()
            if row:
                return json.loads(dict(row)['correct_indices_json'])
            return None

    # ================= 📊 GLOBAL STATISTIKA (KENGAYTIRILGAN) =================
    def get_extended_global_stats(self):
        """Admin uchun kengaytirilgan statistika"""
        with self._conn() as c:
            base = self.get_global_stats()
            # Bugungi aktiv foydalanuvchilar
            today = int(time.time()) - 86400
            active_today = c.execute("""SELECT COUNT(DISTINCT user_id) as cnt
                FROM sessions WHERE started_at >= %s""", (today,)).fetchone()
            # Jami token aylanmasi
            circulation = c.execute("""SELECT SUM(balance) as total
                FROM wallets WHERE user_id != 0""").fetchone()
            # Staking statistikasi
            staking_stats = c.execute("""SELECT COUNT(*) as cnt, SUM(amount) as total_staked
                FROM staking WHERE is_active=1""").fetchone()
            # Kupon statistikasi
            coupon_stats = c.execute("SELECT COUNT(*) as cnt, SUM(use_count) as uses FROM coupons").fetchone()
            # Challenge statistikasi
            challenge_stats = c.execute("""SELECT COUNT(*) as cnt,
                COUNT(CASE WHEN status='finished' THEN 1 END) as finished
                FROM challenges""").fetchone()
            base.update({
                "active_today": dict(active_today)['cnt'] if active_today else 0,
                "token_circulation": float(dict(circulation)['total'] or 0) if circulation else 0,
                "total_staked": float(dict(staking_stats)['total_staked'] or 0) if staking_stats else 0,
                "staking_count": dict(staking_stats)['cnt'] if staking_stats else 0,
                "coupon_count": dict(coupon_stats)['cnt'] if coupon_stats else 0,
                "coupon_uses": dict(coupon_stats)['uses'] if coupon_stats else 0,
                "challenge_count": dict(challenge_stats)['cnt'] if challenge_stats else 0,
                "challenge_finished": dict(challenge_stats)['finished'] if challenge_stats else 0,
            })
            return base



# ==============================================================
# 🛡️ STUB METODLAR — Eski versiyalar uchun xavfsizlik
# Bu metodlar yangi versiyada to'liq metodlar bilan almashtiriladi
# PythonAnywhere'da eski db.py bo'lsa ham bot ishlaydi
# ==============================================================

def _ensure_stubs(cls):
    """Klasga yo'q metodlarni stub sifatida qo'shadi"""

    stubs = {
        # Bot sozlamalari
        '_seed_bot_settings':       lambda self, c: None,
        'get_setting':              lambda self, key, default=None: default,
        'set_setting':              lambda self, key, value, admin_id=0: None,
        'get_all_settings':         lambda self: [],

        # Scheduled tests
        'get_scheduled_tests_due':  lambda self: [],
        'get_upcoming_tests':       lambda self, hours=24: [],
        'schedule_test_open':       lambda self, test_id, open_at_ts: None,

        # Pagination
        'get_tests_paginated':      lambda self, owner_id, page=1, limit=10: ([], 0),
        'get_public_tests_paginated': lambda self, query=None, category_id=None, page=1, limit=12: ([], 0),

        # Staking — haqiqiy metodlar yuqorida mavjud, stub kerak emas
        # 'start_staking', 'unstake', 'get_user_staking', 'get_all_staking_stats'
        # 'process_staking_rewards' — bular DB klassida to'liq yozilgan

        # Kuponlar — haqiqiy metodlar yuqorida mavjud
        # 'create_coupon', 'get_coupon', 'use_coupon', 'get_all_coupons'
        # 'toggle_coupon', 'delete_coupon' — DB klassida to'liq yozilgan

        # Challenge — haqiqiy metodlar yuqorida mavjud
        # 'create_challenge', 'get_challenge', 'get_user_challenges'
        # 'submit_challenge_score' — DB klassida to'liq yozilgan

        # Savol report
        'report_question':          lambda self, test_id, q_index, user_id, report_type, comment="": None,
        'get_question_reports':     lambda self, status='pending', limit=50: [],
        'resolve_report':           lambda self, report_id, status='resolved': None,

        # Test izohlari
        'add_test_comment':         lambda self, test_id, user_id, comment, parent_id=None: 0,
        'get_test_comments':        lambda self, test_id, limit=50: [],
        'like_comment':             lambda self, comment_id: None,
        'delete_comment':           lambda self, comment_id, user_id=None, is_admin=False: None,

        # Email hisobot
        'subscribe_email_report':   lambda self, user_id, email, frequency='weekly': None,
        'unsubscribe_email_report': lambda self, user_id: None,
        'get_email_report_subscribers': lambda self, frequency='weekly': [],
        'mark_email_sent':          lambda self, report_id: None,

        # Moderatsiya
        'add_moderation_rule':      lambda self, rule_type, value, added_by=0: None,
        'get_moderation_rules':     lambda self, rule_type=None: [],
        'toggle_moderation_rule':   lambda self, rule_id, is_active: None,
        'delete_moderation_rule':   lambda self, rule_id: None,
        'check_custom_moderation':  lambda self, text: True,

        # Bulk
        'bulk_give_premium':        lambda self, user_ids, months, admin_id: 0,
        'bulk_ban_users':           lambda self, user_ids, admin_id, reason="Bulk ban": 0,
        'bulk_archive_tests':       lambda self, test_ids, user_id: 0,

        # Adaptive
        'get_adaptive_level':       lambda self, user_id, q_bank_id: 1,
        'update_adaptive_progress': lambda self, user_id, q_bank_id, is_correct: None,
        'get_adaptive_questions':   lambda self, user_id, category_id=None, limit=10: [],

        # Free-text
        'save_freetext_answer':     lambda self, session_id, q_index, user_answer: None,
        'get_freetext_answers':     lambda self, session_id: [],
        'save_freetext_ai_score':   lambda self, freetext_id, score, feedback: None,
        'get_pending_freetext':     lambda self, limit=20: [],

        # Ko'p to'g'ri javob
        'set_multi_correct':        lambda self, test_id, q_index, correct_indices: None,
        'get_multi_correct':        lambda self, test_id, q_index: None,

        # Guruh subscription
        'create_group_subscription': lambda self, group_id, plan, max_members, months, price, created_by: None,
        'get_group_subscription':   lambda self, group_id: None,
        'get_all_group_subscriptions': lambda self: [],

        # Extended stats
        'get_extended_global_stats': lambda self: {
            "total_users": 0, "total_tests": 0, "total_sessions": 0,
            "premium_users": 0, "new_users_today": 0, "active_today": 0,
            "token_circulation": 0.0, "total_staked": 0.0, "staking_count": 0,
            "coupon_count": 0, "coupon_uses": 0, "challenge_count": 0, "challenge_finished": 0
        },
    }

    for method_name, stub_func in stubs.items():
        # MUHIM: faqat metod YO'Q bo'lsagina stub qo'yamiz
        # Haqiqiy metod bo'lsa uni EZmAYMIZ
        if not hasattr(cls, method_name):
            setattr(cls, method_name, stub_func)

    return cls


# Stub metodlarni DB klassiga qo'shamiz (faqat yo'q metodlar uchun)
_ensure_stubs(DB)
