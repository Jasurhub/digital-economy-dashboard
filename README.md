# 🇺🇿 Raqamli Iqtisodiyot — O'zbekiston Statistika Dashboardi

**Real-vaqtli interaktiv dashboard** — O'zbekiston Statistika Agentligining rasmiy
raqamli iqtisodiyot ma'lumotlari asosida qurilgan.

> Manba: https://stat.uz/uz/rasmiy-statistika/raqamli-iqtisodiyot

---

## Arxitektura

```
digital-economy-dashboard/
├── app/
│   ├── __init__.py
│   ├── api.py          ← FastAPI backend (REST endpoints)
│   ├── database.py     ← SQLite schema + CRUD helpers
│   ├── scraper.py      ← stat.uz web scraper (BeautifulSoup)
│   ├── seed_data.py    ← Namuna ma'lumotlar (offline rejim)
│   └── scheduler.py    ← Fon rejimida avtomatik yangilash (6 soatda bir)
├── frontend/
│   └── index.html      ← Interaktiv dashboard (Chart.js, 3 til)
├── main.py             ← Dasturni ishga tushirish nuqtasi
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── README.md
```

---

## Tezkor ishga tushirish

### 1. Python (mahalliy)

```bash
# Virtual muhit yarating
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate

# Kutubxonalarni o'rnating
pip install -r requirements.txt

# Dashboardni ishga tushiring
python main.py
# → http://localhost:8000
```

### 2. Docker (tavsiya etiladi)

```bash
docker-compose up --build
# → http://localhost:8000
```

---

## API Endpointlari

| Endpoint | Tavsif |
|----------|--------|
| `GET /` | Dashboard (frontend) |
| `GET /api/categories?lang=uz` | Barcha kategoriyalar |
| `GET /api/indicators?category=internet` | Ko'rsatkichlar ro'yxati |
| `GET /api/data/{slug}?year_from=2020&year_to=2023` | Vaqt qatorlari ma'lumotlari |
| `GET /api/kpi?lang=uz` | KPI bloklari (so'nggi qiymat + o'sish) |
| `GET /api/compare?slugs=a,b,c` | Solishtiruv uchun ko'p qatorli ma'lumot |
| `GET /api/growth?lang=en` | CAGR va jami o'sish tahlili |
| `GET /api/scrape/status` | So'nggi scrape holati |
| `POST /api/scrape/trigger` | Qo'lda yangilashni boshlash |
| `GET /docs` | Swagger UI (interaktiv API hujjati) |

### Til parametrlari
Barcha endpointlar `?lang=uz` (standart), `?lang=en`, `?lang=ru` ni qo'llab-quvvatlaydi.

---

## Dashboard imkoniyatlari

- **KPI bloklari** — har bir ko'rsatkich uchun so'nggi qiymat + yillik o'sish %
- **Chiziq/ustun/maydon grafiklar** — vaqt qatorlari vizualizatsiyasi
- **CAGR tahlili** — barcha ko'rsatkichlar bo'yicha o'sish sur'ati
- **Solishtirma grafik** — 5 ta asosiy ko'rsatkichni bir vaqtda ko'rish
- **Filtrlash** — kategoriya va yil bo'yicha
- **3 til** — O'zbek / English / Русский
- **Avtomatik yangilanish** — fon rejimida har 6 soatda
- **Qo'lda yangilash** — "Yangilash" tugmasi orqali

---

## Ko'rsatkichlar kategoriyalari

| Kategoriya | Ko'rsatkichlar |
|-----------|----------------|
| 🌐 Internet va ulanish | Internet foydalanuvchilari, Mobil abonentlar, Kengzoqli internet, Optik tolali tarmoq |
| 💻 AKT sektori | YaIM ulushi, Korxonalar soni, Bandlik, Dasturiy ta'minot eksporti, Investitsiyalar |
| 🛒 Elektron tijorat | Tijorat hajmi, Onlayn to'lovlar, E-tijorat korxonalari, Raqamli to'lov foydalanuvchilari |
| 🏛️ Elektron hukumat | Portal foydalanuvchilari, Onlayn xizmatlar soni, BMT indeksi |
| 📡 Telekommunikatsiya | Daromadlar, 4G qamrov, Mobil internet foydalanuvchilari, Internet tezligi |
| 🎓 Raqamli ta'lim | IT bitiruvchilar, IT-Park rezidentlari, Raqamli savodxonlik |

---

## Texnik tafsilotlar

- **Scraper strategiyalari**: HTML jadvallar → stat blocks → subpage follow
- **Scheduler**: APScheduler (Asia/Tashkent timezone, har 6 soatda)
- **DB**: SQLite (WAL rejimi, concurrent reads)
- **Standalone rejim**: Agar backend mavjud bo'lmasa, frontend demo ma'lumotlarni ko'rsatadi
- **CORS**: Barcha originlar uchun ochiq (ishlab chiqish uchun)

---

## Litsenziya
Ma'lumotlar manbai: stat.uz (O'zbekiston Statistika Agentligi)
