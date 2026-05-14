# 🎓 Geo Ustoz - Telegram Quiz & Web App Bot

Bu loyiha **Geo Ustoz** (Testchi) avtomatlashtirilgan test tizimining Telegram bot va Web App (veb-ilova) qismlaridan iborat kompleks dasturdir. U orqali foydalanuvchilar test yechishlari, AI bilan suhbatlashishlari, o'z testlarini yaratishlari va ichki token (GWT) orqali savdo qilishlari mumkin.

## ✨ Asosiy Imkoniyatlar
* **Telegram Web App Integratsiyasi:** Barcha asosiy jarayonlar (test yechish, natijalar, hisobni boshqarish) bot ichidagi qulay veb-oynada ishlaydi.
* **Sun'iy Intellekt (AI):** Groq va OpenRouter API'lari orqali testlarni avtomatik tuzish, savollarga javob topish va foydalanuvchilar bilan suhbatlashish.
* **Token va Hamyon Tizimi:** Foydalanuvchilar uchun avtomatik kriptografik hamyonlar yaratiladi. GWT tokeni yordamida testlarni sotish va sotib olish mumkin.
* **Xavfsizlik va Anti-DDoS:** IP blokka tushirish, reCAPTCHA orqali botlardan himoya va PIN-kod (Lock) tizimi.
* **To'lov Tizimlari:** Telegram Stars va karta orqali Premium obunalar xaridi.

---

## 🛠 O'rnatish uchun talablar
Serverda (VPS, VDS, PythonAnywhere) quyidagilar o'rnatilgan bo'lishi shart:
* **Python 3.9** yoki undan yuqori versiya.
* **MySQL 8.0+** ma'lumotlar bazasi.
* SSL sertifikatiga ega domen (Telegram Web App faqat HTTPS orqali ishlaydi).

---

## 🚀 O'rnatish bo'yicha qadam-ba-qadam qo'llanma

### 1-qadam: Loyihani yuklab olish
Terminal orqali loyihani serverga yuklab oling va papkaga kiring:
```bash
git clone [https://github.com/SizningProfilingiz/LoyihangizNomi.git](https://github.com/SizningProfilingiz/LoyihangizNomi.git)
cd LoyihangizNomi
2-qadam: Kutubxonalarni o'rnatish
Loyiha ishlashi uchun kerakli barcha Python kutubxonalarini o'rnating. Buning uchun Virtual Environment (venv) ishlatish tavsiya etiladi:

Bash
python3 -m venv venv
source venv/bin/activate  # Linux/Mac uchun
# Windows uchun: venv\Scripts\activate

pip install -r requirements.txt
3-qadam: Ma'lumotlar bazasini tayyorlash
MySQL'da yangi ma'lumotlar bazasi yarating (masalan, quizbot_db). Jadvallarni (tables) o'zingiz qo'lda yaratishingiz shart emas — bot birinchi marta ishga tushganda db.py barcha kerakli jadvallarni va "Genesis Block"ni avtomatik yaratib oladi.

4-qadam: Muhit o'zgaruvchilarini (.env) sozlash
Loyihada maxfiy parollar ochiq yozilmagan. Yangi .env faylini yarating va ichidagi ma'lumotlarni o'zingiznikiga almashtiring:

Kod parchasi
# Bot sozlamalari
BOT_TOKEN=123456789:ABCDefghIJKLmnopQRSTuvwxYZ
WEB_BASE_URL=[https://sizning-saytingiz.com](https://sizning-saytingiz.com)

# Ma'lumotlar bazasi (MySQL)
DB_HOST=localhost
DB_USER=root
DB_PASS=mysql_paroli_shu_yerga
DB_NAME=quizbot_db
DB_PORT=3306

# Adminlar (ID larni vergul bilan ajratib yozing)
SUPERADMINS=111111111,222222222
LOWER_ADMINS=333333333
MAIN_ADMIN_ID=111111111
ADMIN_CARD=8600 0000 0000 0000 (Ism Familiya)

# Majburiy kanallar
REQUIRED_CHANNEL=@majburiy_kanal_nomi
PUBLIC_TEST_CHANNEL=@ommaviy_testlar_nomi

# Tashqi API Kalitlar
GROQ_API_KEY=gsk_shu_yerga_groq_kalit
OPENROUTER_API_KEY=sk-or-v1-shu_yerga_openrouter_kalit
RESEND_API_KEY=re_shu_yerga_resend_kalit
RESEND_FROM_EMAIL=bot@sizning-domeningiz.com
RECAPTCHA_SECRET_KEY=6Ld_shu_yerga_recaptcha_kalit
5-qadam: Dasturni ishga tushirish
Ushbu loyiha 2 ta qismdan iborat: Telegram Bot (main.py) va Web App darchasi (web.py). Ikkalasini ham serverda doimiy ishlashini ta'minlash kerak.

Buning uchun screen, tmux, pm2 yoki systemd dan foydalanishingiz mumkin.

Oddiy usulda ishga tushirish (Test uchun):
1-terminalda botni yoqing:

Bash
python main.py
2-terminalda veb-serverni yoqing (standart 5000-portda ishga tushadi):

Bash
python web.py
Eslatma: Veb qismi Telegramda Web App sifatida ochilishi uchun web.py ishlayotgan portni Nginx (Reverse Proxy) orqali HTTPS domeniga ulashingiz kerak bo'ladi.

📂 Fayllar strukturasi qisqacha
main.py — Telegram botning asosiy mantig'i, xabarlarni qabul qilish va qayta ishlash.

web.py — Flask yordamida yozilgan Web App (backend) qismi.

db.py — MySQL bilan ishlash, tranzaksiyalar, foydalanuvchilar va testlarni bazada saqlash uchun qobiq klass.

templates/ — Web App uchun HTML frontend fayllar (index, solve_test, account va h.k).

static/ — CSS, JS va yuklangan rasmlar saqlanadigan papka.