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

# Majburiy kanallar (Yangi versiyada bot ichida boshqariladi)
# Eski ENV usuli ham ishlaydi, lekin tavsiya etilmaydi
# REQUIRED_CHANNELS=@kanal1,@kanal2 (eskirgan)
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



---

## 📢 Majburiy Kanallarni Boshqarish (Yangi!)

Yangi versiyada majburiy kanallarni ENV faylida emas, **bot ichida** boshqarishingiz mumkin!

### Admin Komandalar:

**1. Barcha kanallarni ko'rish:**
```
/managechannels
```

**2. Yangi kanal qo'shish:**
```
/addchannel @kanal_nomi
```
> ⚠️ **Muhim:** Bot o'sha kanalga admin sifatida qo'shilgan bo'lishi kerak!

**3. Kanalni o'chirish (deaktivatsiya):**
```
/removechannel @kanal_nomi
```
> ℹ️ Kanal bazada qoladi, lekin faol emas. Keyin qayta yoqish mumkin.

**4. Kanalni butunlay o'chirish (bazadan):**
```
/deletechannel @kanal_nomi
```
> ⚠️ **Diqqat:** Kanal butunlay bazadan o'chiriladi! Tasdiqlash kerak bo'ladi.

**5. Kanalni qayta faollashtirish:**
```
/activatechannel @kanal_nomi
```

### Xususiyatlar:
- ✅ Real-time yangilanish (botni qayta ishga tushirish shart emas)
- ✅ Har bir kanal alohida tekshiriladi
- ✅ Foydalanuvchiga obuna bo'lmagan kanallar ro'yxati ko'rsatiladi
- ✅ Har bir kanal uchun dinamik tugmalar
- ✅ 3 tilda qo'llab-quvvatlash (O'zbek Lotin, Kirill, Rus)
- ✅ Adminlar uchun majburiy obuna o'chirilgan
- ✅ MySQL database'da saqlanadi

### Ishlatish Misoli:

```bash
# 1. Kanal qo'shish
/addchannel @geo_ustoz

# 2. Yana bir kanal qo'shish
/addchannel @testchi_uz

# 3. Barcha kanallarni ko'rish
/managechannels

# 4. Bitta kanalni o'chirish (deaktivatsiya)
/removechannel @geo_ustoz

# 5. Qayta yoqish
/activatechannel @geo_ustoz

# 6. Butunlay o'chirish (bazadan)
/deletechannel @testchi_uz
```

### O'chirish usullari:

1. **Deaktivatsiya** (`/removechannel`) - Kanal bazada qoladi, keyin qayta yoqish mumkin
2. **Butunlay o'chirish** (`/deletechannel`) - Kanal butunlay bazadan o'chiriladi, tasdiqlash kerak

---

## 🔄 Yangilanishlar

**v2.0 - Majburiy Kanallar Tiziми**
- Bot ichida kanallarni boshqarish
- Real-time yangilanish
- Ko'p kanallar qo'llab-quvvatlash
- Database'da saqlash



---

## 🚀 v3.0 - Yangi Imkoniyatlar (Katta Yangilanish)

Ushbu versiyada botga juda ko'plab yangi funksiyalar qo'shildi. Barcha funksiyalar 3 tilda (O'zbek lotin, O'zbek kirill, Rus) ishlaydi.

### 📊 1. Statistika va Analitika
- **Foydalanuvchi statistikasi:** Daraja, XP, aniqlik foizi, ishlangan/yaratilgan testlar.
- **Test analitikasi:** O'rtacha ball, eng yuqori/past natija, eng ko'p xato qilingan savollar.
- **Global statistika (admin):** Umumiy foydalanuvchilar, testlar, premium, kunlik o'sish.
- Bot: `/stats` | Web: `/stats`, `/admin/stats`, `/test-analytics/<test_id>`

### 🎮 2. Gamification (O'yin elementi)
- **XP va Daraja tizimi:** Har bir to'g'ri javob uchun XP, darajalar avtomatik oshadi.
- **Streak (kunlik faollik):** Ketma-ket faol kunlar uchun bonus XP (7+ kun = 1.5x).
- **Yutuqlar (Achievements):** 11 ta turli yutuq (birinchi test, 100 test, mukammal natija va h.k.).
- Bot: `/achievements`, `/top` | Web: `/achievements`, `/leaderboard`

### 👥 3. O'quv Guruhlari (Sinflar)
- O'qituvchilar guruh yaratadi, o'quvchilar maxsus kod orqali qo'shiladi.
- Guruhga test (uy vazifasi) biriktirish, guruh ichidagi reyting.
- Bot: `/groups`, `/creategroup Nomi`, `/joingroup KOD` | Web: `/my-groups`, `/group/<id>`

### 🤖 4. AI Test Generator
- Mavzu kiritsangiz, sun'iy intellekt (Groq) avtomatik test tuzib beradi.
- Bot: `/aitest <mavzu>` (masalan: `/aitest O'zbekiston tarixi`)

### 🔔 5. Bildirishnomalar
- Yangi yutuq, daraja, deadline yaqinlashganda avtomatik xabar.
- Guruh vazifalari uchun avtomatik eslatma (har 6 soatda tekshiriladi).
- Bot: `/notifications` | Web: `/notifications`

### 🗂️ 6. Kategoriyalar va Kutubxona
- Testlar kategoriyalar bo'yicha ajratiladi (Matematika, Fizika, Tarix va h.k.).
- Ommaviy testlar kutubxonasi, kategoriya va nom bo'yicha qidiruv.
- Web: `/library`

### 📥 7. Kengaytirilgan Export
- Ko'p varaqli Excel hisobot: Natijalar + Umumiy tahlil + Savollar tahlili.
- Web: `/export-excel/<test_id>`

### ⭐ 8. Sharhlar va Saqlanganlar
- Testlarga baho (1-5 yulduz) va sharh qoldirish.
- Testlarni saqlash (bookmark).
- API: `/api/review/add`, `/api/bookmark/toggle`

### 🗄️ Yangi Database Jadvallari
`categories`, `user_stats`, `achievements`, `user_achievements`, `study_groups`,
`group_members`, `group_assignments`, `notifications`, `test_reviews`,
`test_bookmarks`, `question_stats`. Barchasi bot birinchi ishga tushganda
avtomatik yaratiladi (qo'lda hech narsa qilish shart emas).

### 📋 Barcha Bot Komandalari (Yangi)
```
/stats          - Shaxsiy statistika (daraja, XP, streak)
/achievements   - Yutuqlar ro'yxati
/top            - Oylik reyting
/notifications  - Bildirishnomalar
/groups         - Mening guruhlarim
/creategroup    - Yangi guruh yaratish
/joingroup      - Kod orqali guruhga qo'shilish
/aitest         - AI orqali test yaratish
```
