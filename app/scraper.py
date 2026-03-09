"""
Scraper for https://stat.uz/uz/rasmiy-statistika/raqamli-iqtisodiyot

Strategy (priority order):
  1. Discover dataset IDs by parsing JSON(API)/CSV href attributes from the live page
  2. Call stat.uz JSON API for each known dataset
  3. Fallback to CSV download
  4. Parse pivoted or row-per-year tables into {year: value}

All 38 datasets are catalogued from the official page screenshots.
"""
import csv
import io
import re
import time
import logging
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from app.database import insert_category, insert_indicator, upsert_data_point, log_scrape

logger = logging.getLogger("scraper")

BASE_URL   = "https://stat.uz"
TARGET_URL = "https://stat.uz/uz/rasmiy-statistika/raqamli-iqtisodiyot"
HEADERS    = {
    "User-Agent": "Mozilla/5.0 (compatible; UzDigitalEconomyDashboard/1.0)",
    "Accept-Language": "uz,ru;q=0.9,en;q=0.8",
}

# ─────────────────────────────────────────────────────────────────────────────
#  FULL DATASET CATALOGUE  (derived from official page screenshots)
# ─────────────────────────────────────────────────────────────────────────────
CATEGORIES_META = {
    "korxonalar": {
        "name_uz": "Iqtisodiy faoliyat turlari bo'yicha korxona va tashkilotlar",
        "name_en": "Companies by economic activity type",
        "name_ru": "Предприятия по видам экономической деятельности",
        "icon": "🏢",
    },
    "xizmat": {
        "name_uz": "Xizmat ko'rsatish",
        "name_en": "Services",
        "name_ru": "Услуги",
        "icon": "🖥️",
    },
    "aloqa": {
        "name_uz": "Aloqa",
        "name_en": "Telecommunications",
        "name_ru": "Связь",
        "icon": "📡",
    },
    "turmush": {
        "name_uz": "Turmush darajasi bo'yicha ko'rsatkichlar",
        "name_en": "Living standards indicators",
        "name_ru": "Показатели уровня жизни",
        "icon": "🏠",
    },
    "mehnat": {
        "name_uz": "Mehnat resurslari",
        "name_en": "Labour resources",
        "name_ru": "Трудовые ресурсы",
        "icon": "👷",
    },
    "ish-haqi": {
        "name_uz": "Ish haqi",
        "name_en": "Wages",
        "name_ru": "Заработная плата",
        "icon": "💰",
    },
    "axborot-iqtisodiyoti": {
        "name_uz": "Axborot iqtisodiyoti va elektron tijorat",
        "name_en": "Information economy & e-commerce",
        "name_ru": "Информационная экономика и электронная торговля",
        "icon": "🛒",
    },
}

DATASETS = [
    # ── Iqtisodiy faoliyat turlari bo'yicha korxona va tashkilotlar ──────────
    {"cat": "korxonalar", "unit_uz": "dona", "unit_en": "units", "updated": "01/01/2025",
     "slug": "tugatilgan-kichik-tadbirkorlik",
     "name_uz": "Tugatilgan kichik tadbirkorlik subyektlari soni (iqtisodiy faoliyat turlari kesimida, yillik)",
     "name_en": "Closed small business entities by economic activity type (annual)",
     "name_ru": "Ликвидированные МСП по видам деятельности (годовые)"},
    {"cat": "korxonalar", "unit_uz": "dona", "unit_en": "units", "updated": "01/01/2025",
     "slug": "axborot-aloqa-korxonalar",
     "name_uz": "Axborot va aloqa sohasida faoliyat ko'rsatayotgan korxona va tashkilotlar soni (iqtisodiy faoliyat turi bo'yicha, yillik)",
     "name_en": "IT & telecom companies (by activity type, annual)",
     "name_ru": "ИКТ-компании по видам деятельности (годовые)"},

    # ── Xizmat ko'rsatish ─────────────────────────────────────────────────────
    {"cat": "xizmat", "unit_uz": "%", "unit_en": "%", "updated": "03/07/2025",
     "slug": "dasturiy-xarajat-yaim",
     "name_uz": "Dasturiy ta'minotga qilingan xarajatlarning yalpi ichki mahsulotdagi ulushi (yillik)",
     "name_en": "Software expenditure as % of GDP (annual)",
     "name_ru": "Расходы на ПО в % от ВВП (годовые)"},

    # ── Aloqa ─────────────────────────────────────────────────────────────────
    {"cat": "aloqa", "unit_uz": "mlrd. so'm", "unit_en": "billion UZS", "updated": "26/06/2025",
     "slug": "aloqa-xizmatlar-hajmi",
     "name_uz": "Ko'rsatilgan aloqa va axborotlashtirish xizmatlarining hajmi",
     "name_en": "Volume of telecom & IT services rendered",
     "name_ru": "Объём услуг связи и информатизации"},
    {"cat": "aloqa", "unit_uz": "ming dona", "unit_en": "thousands", "updated": "20/06/2025",
     "slug": "internet-abonentlar-jismoniy-hudud",
     "name_uz": "Internet tarmog'iga ulangan abonentlar soni, jismoniy shaxslar (hududlar kesimida)",
     "name_en": "Internet subscribers – individuals (by region)",
     "name_ru": "Абоненты интернета – физлица (по регионам)"},
    {"cat": "aloqa", "unit_uz": "mlrd. so'm", "unit_en": "billion UZS", "updated": "26/06/2025",
     "slug": "noshirlik-xizmatlari",
     "name_uz": "Noshirlik xizmatlari hajmi",
     "name_en": "Publishing services volume",
     "name_ru": "Объём издательских услуг"},
    {"cat": "aloqa", "unit_uz": "mlrd. so'm", "unit_en": "billion UZS", "updated": "26/06/2025",
     "slug": "televideniye-xizmatlari",
     "name_uz": "Televideniye va dasturlari kino-videofilmlar ishlab chiqarish bo'yicha xizmatlar, ovoz yozish va musiqa asarlarini nashr qilish bo'yicha xizmatlar hajmi",
     "name_en": "TV, film production, audio recording & publishing services volume",
     "name_ru": "Объём услуг ТВ, кинопроизводства, звукозаписи"},
    {"cat": "aloqa", "unit_uz": "mlrd. so'm", "unit_en": "billion UZS", "updated": "26/06/2025",
     "slug": "teleradio-xizmatlari",
     "name_uz": "Teleradioeshittirish va dasturlar tuzish bo'yicha xizmatlar hajmi",
     "name_en": "Broadcasting & programming services volume",
     "name_ru": "Объём услуг телерадиовещания и программирования"},
    {"cat": "aloqa", "unit_uz": "mlrd. so'm", "unit_en": "billion UZS", "updated": "26/06/2025",
     "slug": "telekommunikatsiya-xizmatlari",
     "name_uz": "Telekommunikatsiya xizmatlari hajmi",
     "name_en": "Telecommunications services volume",
     "name_ru": "Объём услуг телекоммуникаций"},
    {"cat": "aloqa", "unit_uz": "mlrd. so'm", "unit_en": "billion UZS", "updated": "26/06/2025",
     "slug": "kompyuter-xizmatlar-hajmi",
     "name_uz": "Kompyuter dasturlashtirish, maslahat berish xizmatlari va boshqa yordamchi xizmatlar hajmi",
     "name_en": "IT programming, consulting & support services volume",
     "name_ru": "Объём услуг программирования, консультирования и сопутствующих ИТ"},
    {"cat": "aloqa", "unit_uz": "ming dona", "unit_en": "thousands", "updated": "23/06/2025",
     "slug": "mobil-internet-abonentlar",
     "name_uz": "Internet tarmog'iga mobil aloqa orqali ulangan abonentlar soni",
     "name_en": "Mobile internet subscribers",
     "name_ru": "Абоненты мобильного интернета"},
    {"cat": "aloqa", "unit_uz": "ming dona", "unit_en": "thousands", "updated": "23/06/2025",
     "slug": "keng-polosali-abonentlar",
     "name_uz": "Internet tarmog'iga keng polosali ulanish bo'yicha abonentlar soni",
     "name_en": "Broadband internet subscribers",
     "name_ru": "Абоненты широкополосного интернета"},
    {"cat": "aloqa", "unit_uz": "ming dona", "unit_en": "thousands", "updated": "23/06/2025",
     "slug": "malumot-uzatuvchi-abonentlar",
     "name_uz": "Ma'lumot uzatuvchi tarmog'iga ulangan, Internetni qo'shgan holda abonentlar soni",
     "name_en": "Data network subscribers (incl. Internet)",
     "name_ru": "Абоненты сети передачи данных (вкл. интернет)"},
    {"cat": "aloqa", "unit_uz": "ming dona", "unit_en": "thousands", "updated": "23/06/2025",
     "slug": "internet-abonentlar-yuridik",
     "name_uz": "Internet tarmog'iga ulangan abonentlar soni, yuridik shaxslar",
     "name_en": "Internet subscribers – legal entities",
     "name_ru": "Абоненты интернета – юридические лица"},
    {"cat": "aloqa", "unit_uz": "ming dona", "unit_en": "thousands", "updated": "20/06/2025",
     "slug": "internet-abonentlar-jami-hudud",
     "name_uz": "Internet tarmog'iga ulangan abonentlar soni, jami (hududlar kesimida)",
     "name_en": "Total internet subscribers (by region)",
     "name_ru": "Всего абонентов интернета (по регионам)"},
    {"cat": "aloqa", "unit_uz": "dona/100 kishi", "unit_en": "per 100 population", "updated": "20/06/2025",
     "slug": "internet-abonentlar-100-kishi",
     "name_uz": "Internet tarmog'iga ulangan abonentlar soni, 100 ta aholiga nisbatan (hududlar kesimida)",
     "name_en": "Internet subscribers per 100 population (by region)",
     "name_ru": "Абоненты интернета на 100 чел. (по регионам)"},
    {"cat": "aloqa", "unit_uz": "ming dona", "unit_en": "thousands", "updated": "20/06/2025",
     "slug": "mobil-radiostantsiyalar-jami",
     "name_uz": "Mobil aloqa tizimiga ulangan abonent radiostantsiyalari soni (jami)",
     "name_en": "Mobile subscriber stations – total",
     "name_ru": "Абонентские радиостанции мобильной связи – всего"},
    {"cat": "aloqa", "unit_uz": "ming dona", "unit_en": "thousands", "updated": "20/06/2025",
     "slug": "mobil-radiostantsiyalar-hudud",
     "name_uz": "Mobil aloqa tizimiga ulangan abonent radiostantsiyalari soni (hududlar kesimida)",
     "name_en": "Mobile subscriber stations (by region)",
     "name_ru": "Абонентские радиостанции мобильной связи (по регионам)"},
    {"cat": "aloqa", "unit_uz": "ming dona", "unit_en": "thousands", "updated": "20/06/2025",
     "slug": "mahalliy-telefon-sigimi",
     "name_uz": "Mahalliy telefon tarmog'ining umumiy montaj qilingan sig'imi",
     "name_en": "Local telephone network installed capacity",
     "name_ru": "Установленная ёмкость местных телефонных сетей"},

    # ── Turmush darajasi ──────────────────────────────────────────────────────
    {"cat": "turmush", "unit_uz": "%", "unit_en": "%", "updated": "31/10/2024",
     "slug": "uy-xojalik-tor-yolakli",
     "name_uz": "Qayd qilingan (simli) tor yo'lakli tarmoq bo'yicha internetga kirish imkoniyatiga ega bo'lgan uy xo'jaliklari ulushi",
     "name_en": "Households with fixed narrowband internet (%)",
     "name_ru": "Домохозяйства с фиксированным узкополосным интернетом (%)"},
    {"cat": "turmush", "unit_uz": "%", "unit_en": "%", "updated": "31/10/2024",
     "slug": "uy-xojalik-keng-yolakli-simli",
     "name_uz": "Qayd qilingan (simli) keng yo'lakli tarmoq bo'yicha internetga kirish imkoniyatiga ega bo'lgan uy xo'jaliklari ulushi",
     "name_en": "Households with fixed broadband internet (%)",
     "name_ru": "Домохозяйства с фиксированным ШПД (%)"},
    {"cat": "turmush", "unit_uz": "%", "unit_en": "%", "updated": "31/10/2024",
     "slug": "uy-xojalik-simsiz-keng",
     "name_uz": "Qayd qilingan yer ustidagi (simsiz) keng yo'lakli tarmoq bo'yicha internetga kirish imkoniyatiga ega bo'lgan uy xo'jaliklari ulushi",
     "name_en": "Households with wireless broadband internet (%)",
     "name_ru": "Домохозяйства с беспроводным ШПД (%)"},
    {"cat": "turmush", "unit_uz": "%", "unit_en": "%", "updated": "31/10/2024",
     "slug": "uy-xojalik-suniy-yuldosh",
     "name_uz": "Sun'iy yo'ldoshli keng yo'lakli tarmoq (sun'iy yo'ldosh aloqasi orqali) bo'yicha internetga kirish imkoniyatiga ega bo'lgan uy xo'jaliklari ulushi",
     "name_en": "Households with satellite broadband internet (%)",
     "name_ru": "Домохозяйства со спутниковым ШПД (%)"},
    {"cat": "turmush", "unit_uz": "%", "unit_en": "%", "updated": "31/10/2024",
     "slug": "uy-xojalik-portativ-mobil",
     "name_uz": "Portativ qurilmadan foydalaniladigan keng yo'lakli mobil tarmoq bo'yicha internetga kirish imkoniyatiga ega bo'lgan uy xo'jaliklari ulushi",
     "name_en": "Households with mobile broadband (portable device) (%)",
     "name_ru": "Домохозяйства с мобильным ШПД через портативное устройство (%)"},
    {"cat": "turmush", "unit_uz": "%", "unit_en": "%", "updated": "31/10/2024",
     "slug": "uy-xojalik-sim-usb-modem",
     "name_uz": "Kompyuterga moslashtirilgan SIM-karta yoki USB modemdan foydalaniladigan keng yo'lakli mobil tarmoq bo'yicha internetga kirish imkoniyatiga ega bo'lgan uy xo'jaliklari ulushi",
     "name_en": "Households with mobile broadband via SIM/USB modem (%)",
     "name_ru": "Домохозяйства с мобильным ШПД через SIM/USB модем (%)"},

    # ── Mehnat resurslari ─────────────────────────────────────────────────────
    {"cat": "mehnat", "unit_uz": "ming kishi", "unit_en": "thousands", "updated": "25/06/2025",
     "slug": "akt-xodimlar-soni",
     "name_uz": "AKT sohasida faoliyat yuritayotgan yuridik shaxslarda ishlovchi xodimlar soni",
     "name_en": "Employees at ICT legal entities",
     "name_ru": "Работники ИКТ-компаний"},

    # ── Ish haqi ──────────────────────────────────────────────────────────────
    {"cat": "ish-haqi", "unit_uz": "ming so'm", "unit_en": "thousand UZS", "updated": "12/06/2025",
     "slug": "axborot-aloqa-ish-haqi",
     "name_uz": "Axborot va aloqa sohasidagi ish haqi",
     "name_en": "ICT sector wages",
     "name_ru": "Заработная плата в сфере ИКТ"},

    # ── Axborot iqtisodiyoti va elektron tijorat ──────────────────────────────
    {"cat": "axborot-iqtisodiyoti", "unit_uz": "mlrd. so'm", "unit_en": "billion UZS", "updated": "21/01/2025",
     "slug": "axborot-iqtisodiyot-yqq-jami",
     "name_uz": "Axborot iqtisodiyoti va elektron tijorat sektorida yaratilgan yalpi qo'shilgan qiymat hajmi (yillik)",
     "name_en": "GVA in information economy & e-commerce (annual)",
     "name_ru": "ВДС в инф. экономике и э-торговле (годовые)"},
    {"cat": "axborot-iqtisodiyoti", "unit_uz": "mlrd. so'm", "unit_en": "billion UZS", "updated": "21/01/2025",
     "slug": "akt-sektor-yqq-hajmi",
     "name_uz": "Axborot kommunikatsiya texnologiyalari (AKT) sektorida yaratilgan yalpi qo'shilgan qiymat hajmi (yillik)",
     "name_en": "GVA in ICT sector (annual)",
     "name_ru": "ВДС в секторе ИКТ (годовые)"},
    {"cat": "axborot-iqtisodiyoti", "unit_uz": "mlrd. so'm", "unit_en": "billion UZS", "updated": "21/01/2025",
     "slug": "akt-ishlab-chikarish-yqq",
     "name_uz": "AKT ishlab chiqarishda yaratilgan yalpi qo'shilgan qiymat hajmi (yillik)",
     "name_en": "GVA in ICT manufacturing (annual)",
     "name_ru": "ВДС в производстве ИКТ (годовые)"},
    {"cat": "axborot-iqtisodiyoti", "unit_uz": "mlrd. so'm", "unit_en": "billion UZS", "updated": "21/01/2025",
     "slug": "akt-savdo-yqq",
     "name_uz": "AKT savdosida yaratilgan yalpi qo'shilgan qiymat hajmi (yillik)",
     "name_en": "GVA in ICT trade (annual)",
     "name_ru": "ВДС в торговле ИКТ (годовые)"},
    {"cat": "axborot-iqtisodiyoti", "unit_uz": "mlrd. so'm", "unit_en": "billion UZS", "updated": "21/01/2025",
     "slug": "akt-xizmatlar-yqq",
     "name_uz": "AKT xizmatlarida yaratilgan yalpi qo'shilgan qiymat hajmi (yillik)",
     "name_en": "GVA in ICT services (annual)",
     "name_ru": "ВДС в услугах ИКТ (годовые)"},
    {"cat": "axborot-iqtisodiyoti", "unit_uz": "mlrd. so'm", "unit_en": "billion UZS", "updated": "21/01/2025",
     "slug": "kontent-sektor-yqq",
     "name_uz": "Kontent sektori va ommaviy axborot vositalarida yaratilgan yalpi qo'shilgan qiymat hajmi (yillik)",
     "name_en": "GVA in content sector & media (annual)",
     "name_ru": "ВДС в контент-секторе и СМИ (годовые)"},
    {"cat": "axborot-iqtisodiyoti", "unit_uz": "mlrd. so'm", "unit_en": "billion UZS", "updated": "21/01/2025",
     "slug": "elektron-tijorat-yqq",
     "name_uz": "Elektron tijoratda yaratilgan yalpi qo'shilgan qiymat hajmi (yillik)",
     "name_en": "GVA in e-commerce (annual)",
     "name_ru": "ВДС в электронной торговле (годовые)"},
    {"cat": "axborot-iqtisodiyoti", "unit_uz": "%", "unit_en": "%", "updated": "21/01/2025",
     "slug": "axborot-iqtisodiyot-yaim-ulushi",
     "name_uz": "Axborot iqtisodiyoti va elektron tijorat sektorida yaratilgan yalpi qo'shilgan qiymatning YalMdagi ulushi (yillik)",
     "name_en": "Information economy & e-commerce GVA share in GDP (annual)",
     "name_ru": "Доля ВДС инф. экономики и э-торговли в ВВП (годовые)"},
    {"cat": "axborot-iqtisodiyoti", "unit_uz": "%", "unit_en": "%", "updated": "21/01/2025",
     "slug": "akt-yaim-ulushi",
     "name_uz": "Axborot kommunikatsiya texnologiyalari (AKT) sektorida yaratilgan yalpi qo'shilgan qiymatning YalMdagi ulushi (yillik)",
     "name_en": "ICT sector GVA share in GDP (annual)",
     "name_ru": "Доля ВДС сектора ИКТ в ВВП (годовые)"},
    {"cat": "axborot-iqtisodiyoti", "unit_uz": "%", "unit_en": "%", "updated": "21/01/2025",
     "slug": "akt-ishlab-chikarish-yaim-ulushi",
     "name_uz": "AKT ishlab chiqarishda yaratilgan yalpi qo'shilgan qiymatning YalMdagi ulushi (yillik)",
     "name_en": "ICT manufacturing GVA share in GDP (annual)",
     "name_ru": "Доля ВДС производства ИКТ в ВВП (годовые)"},
    {"cat": "axborot-iqtisodiyoti", "unit_uz": "%", "unit_en": "%", "updated": "21/01/2025",
     "slug": "akt-savdo-yaim-ulushi",
     "name_uz": "AKT savdosida yaratilgan yalpi qo'shilgan qiymatning YalMdagi ulushi (yillik)",
     "name_en": "ICT trade GVA share in GDP (annual)",
     "name_ru": "Доля ВДС торговли ИКТ в ВВП (годовые)"},
    {"cat": "axborot-iqtisodiyoti", "unit_uz": "%", "unit_en": "%", "updated": "21/01/2025",
     "slug": "akt-xizmatlar-yaim-ulushi",
     "name_uz": "AKT xizmatlarida yaratilgan yalpi qo'shilgan qiymatning YalMdagi ulushi (yillik)",
     "name_en": "ICT services GVA share in GDP (annual)",
     "name_ru": "Доля ВДС услуг ИКТ в ВВП (годовые)"},
    {"cat": "axborot-iqtisodiyoti", "unit_uz": "%", "unit_en": "%", "updated": "21/01/2025",
     "slug": "kontent-sektor-yaim-ulushi",
     "name_uz": "Kontent sektori va ommaviy axborot vositalarida yaratilgan yalpi qo'shilgan qiymatning YalMdagi ulushi (yillik)",
     "name_en": "Content sector & media GVA share in GDP (annual)",
     "name_ru": "Доля ВДС контент-сектора и СМИ в ВВП (годовые)"},
    {"cat": "axborot-iqtisodiyoti", "unit_uz": "%", "unit_en": "%", "updated": "21/01/2025",
     "slug": "elektron-tijorat-yaim-ulushi",
     "name_uz": "Elektron tijoratda yaratilgan yalpi qo'shilgan qiymatning YalMdagi ulushi (yillik)",
     "name_en": "E-commerce GVA share in GDP (annual)",
     "name_ru": "Доля ВДС электронной торговли в ВВП (годовые)"},
]


# ─────────────────────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _session():
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def _clean_number(text: str):
    if not text:
        return None
    text = re.sub(r"\s", "", str(text).strip())
    text = text.replace(",", ".")
    text = re.sub(r"[^\d.\-]", "", text)
    if not text or text in (".", "-", ""):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _extract_year(text):
    m = re.search(r"\b(20\d{2})\b", str(text))
    return int(m.group(1)) if m else None


# ─────────────────────────────────────────────────────────────────────────────
#  Step 1 – discover dataset IDs from live page
# ─────────────────────────────────────────────────────────────────────────────

def discover_ids(session) -> dict[str, str]:
    """Return {row_title[:80] → dataset_id} by parsing href attributes."""
    try:
        resp = session.get(TARGET_URL, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Main page unreachable: {e}")
        return {}

    soup   = BeautifulSoup(resp.text, "lxml")
    id_map = {}
    for a in soup.find_all("a", href=True):
        m = re.search(r"[?&]id=(\d+)", a["href"])
        if not m:
            continue
        did  = m.group(1)
        row  = a.find_parent("tr") or a.find_parent("li") or a.find_parent("div")
        title = row.get_text(" ", strip=True)[:80] if row else ""
        if title:
            id_map[title] = did
    logger.info(f"Discovered {len(id_map)} dataset IDs")
    return id_map


# ─────────────────────────────────────────────────────────────────────────────
#  Step 2 – fetch JSON or CSV for a given ID
# ─────────────────────────────────────────────────────────────────────────────

def fetch_json(session, did: str) -> list[dict]:
    url = f"{BASE_URL}/uz/ofitsialnaya-statistika/raqamli-iqtisodiyot?task=json&id={did}"
    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        for key in ("data", "rows", "result", "items"):
            if isinstance(data.get(key), list):
                return data[key]
    except Exception as e:
        logger.debug(f"JSON id={did}: {e}")
    return []


def fetch_csv(session, did: str) -> list[dict]:
    url = f"{BASE_URL}/uz/ofitsialnaya-statistika/raqamli-iqtisodiyot?task=download&id={did}&format=csv"
    try:
        resp    = session.get(url, timeout=20)
        resp.raise_for_status()
        content = resp.content.decode("utf-8-sig", errors="replace")
        return list(csv.DictReader(io.StringIO(content)))
    except Exception as e:
        logger.debug(f"CSV id={did}: {e}")
    return []


# ─────────────────────────────────────────────────────────────────────────────
#  Step 3 – parse rows → {year: value}
# ─────────────────────────────────────────────────────────────────────────────

def parse_timeseries(rows: list[dict]) -> dict[int, float]:
    result: dict[int, float] = {}
    for row in rows:
        keys      = list(row.keys())
        year_key  = next((k for k in keys if re.search(r"yil|year|год", k, re.I)), None)
        value_key = next((k for k in keys if re.search(r"qiymat|value|значение|miqdor|amount", k, re.I)), None)
        if year_key and value_key:
            y = _extract_year(row[year_key])
            v = _clean_number(row[value_key])
            if y and v is not None:
                result[y] = v
            continue
        # Pivot table – year in column header
        for k, v in row.items():
            y = _extract_year(k)
            if y:
                val = _clean_number(v)
                if val is not None:
                    result[y] = val
    return result


# ─────────────────────────────────────────────────────────────────────────────
#  Main pipeline
# ─────────────────────────────────────────────────────────────────────────────

def run_scraper() -> int:
    started = datetime.now().isoformat()
    total   = 0
    session = _session()

    try:
        id_map = discover_ids(session)

        # Register all categories
        cat_ids: dict[str, int] = {}
        for slug, meta in CATEGORIES_META.items():
            cat_ids[slug] = insert_category(
                name_uz=meta["name_uz"],
                name_en=meta["name_en"],
                name_ru=meta["name_ru"],
                slug=slug,
                icon=meta["icon"],
            )

        for ds in DATASETS:
            # Match dataset to discovered ID via fuzzy title search
            did = None
            needle = ds["name_uz"][:40].lower()
            for title, candidate_id in id_map.items():
                if needle in title.lower():
                    did = candidate_id
                    break

            rows: list[dict] = []
            if did:
                rows = fetch_json(session, did)
                if not rows:
                    rows = fetch_csv(session, did)
                time.sleep(0.3)

            ts = parse_timeseries(rows) if rows else {}
            if not ts:
                logger.info(f"  {ds['slug']}: no live data — seed will cover it")
                continue

            cat_meta = CATEGORIES_META[ds["cat"]]
            ind_id = insert_indicator(
                category_id=cat_ids[ds["cat"]],
                name_uz=ds["name_uz"],
                name_en=ds["name_en"],
                name_ru=ds.get("name_ru", ds["name_uz"]),
                unit_uz=ds["unit_uz"],
                unit_en=ds["unit_en"],
                slug=ds["slug"],
                source_url=TARGET_URL,
            )
            for year, value in ts.items():
                upsert_data_point(ind_id, year, value, raw_value=str(value))
                total += 1
            logger.info(f"  {ds['slug']}: {len(ts)} years stored from live data")

        log_scrape(started, datetime.now().isoformat(), "success", total)
        logger.info(f"Scrape finished – {total} rows upserted.")

    except Exception as e:
        log_scrape(started, datetime.now().isoformat(), "error", total, str(e))
        logger.error(f"Scrape failed: {e}", exc_info=True)

    return total


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from app.database import init_db
    init_db()
    run_scraper()
