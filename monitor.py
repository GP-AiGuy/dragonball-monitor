"""
Dragon Ball TCG Booster Box Pre-Order Monitor

Focused tracker for Dragon Ball Super Card Game:
  - Masters series (current target: B31)
  - Fusion World series (current target: FB11)

Watches NL/EU/UK shops + Dragon Ball news sources.
PRIORITY alerts when B31 or FB11 booster box pre-orders go live.
Also detects: restocks, price drops, sold-out -> available transitions.

Usage:
    python3 execution/tcg_preorder_monitor.py              # Run once
    python3 execution/tcg_preorder_monitor.py --dry-run    # Check without alerts
    python3 execution/tcg_preorder_monitor.py --reset      # Reset DB
    python3 execution/tcg_preorder_monitor.py --list       # Show tracked products
    python3 execution/tcg_preorder_monitor.py --priority   # Show only priority watchlist matches
"""

import os
import re
import json
import time
import hashlib
import logging
import argparse
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
# Override via TCG_STATE_DIR env var (used by GitHub Actions to persist state in repo)
DATA_DIR = Path(os.getenv("TCG_STATE_DIR", str(PROJECT_ROOT / ".tmp" / "tcg_monitor")))
DATA_DIR.mkdir(parents=True, exist_ok=True)
SEEN_PRODUCTS_FILE = DATA_DIR / "seen_products.json"
SEEN_NEWS_FILE = DATA_DIR / "seen_news.json"
PRICE_HISTORY_FILE = DATA_DIR / "price_history.json"
# Self-healing: per-shop + per-priority-URL health stats. Auto-disables dead shops.
HEALTH_FILE = DATA_DIR / "health.json"
MAX_CONSECUTIVE_FAILURES = 5
DASHBOARD_FEED_FILE = Path(
    os.getenv("TCG_DASHBOARD_FEED", str(PROJECT_ROOT / "dragonball-tracker" / "data.json"))
)

# ─── Watchlist ───────────────────────────────────────────────────────────
# These are the booster boxes Gianni actively wants to pre-order.
# Hits trigger HIGH-PRIORITY Telegram alert (separate from regular alerts).

PRIORITY_WATCHLIST = [
    {
        "id": "BT31",
        "series": "Masters Ultra-Bout Set 4 (Battles Beyond Dimensions)",
        "name": "Dragon Ball Super Card Game Masters BT31 Booster Box",
        # Spelling variants seen in the wild:
        #   B31, BT31, B-31, BT-31, B 31, BT 31, B_31, B31E, BT31EN, B-31-EN
        # Boundaries: not preceded by alphanumeric (avoid e.g. "ZB31" or "FB31"),
        # not FOLLOWED by a digit (avoid e.g. "B315"). Letters after are OK
        # (suffixes like E, EN, JP).
        "patterns": [
            r"(?<![a-z0-9])bt?[\s\-_]?31(?!\d)",
            r"impact[\s\-_]*beyond[\s\-_]*dimensions",
            r"battles[\s\-_]*beyond[\s\-_]*dimensions",
        ],
    },
    {
        "id": "FB10",
        "series": "Fusion World Cross Force",
        "name": "Dragon Ball Super Card Game Fusion World FB10 Booster Box (Cross Force)",
        # Variants: FB10, FB-10, FB_10, FB 10, FB10E, FB10EN.
        # Right-guard prevents matching FB100/FB101/FB108.
        "patterns": [
            r"(?<![a-z0-9])fb[\s\-_]?10(?!\d)",
            r"fusion\s*world[\s\-_]*10(?!\d)",
            r"cross[\s\-_]*force",
        ],
    },
]

# Known direct product URLs. Always deep-checked every run, regardless of whether
# the shop's category page surfaces them. Add new ones as they're discovered.
PRIORITY_PRODUCT_URLS = [
    # ── BT31 (Masters Ultra-Bout Set 4) ──
    {
        "id": "BT31",
        "shop": "Gamerz Paradize",
        "country": "NL",
        "url": "https://gamerzparadize.nl/products/dragon-ball-scg-masters-ultra-bout-series-set-4-battles-beyond-dimensions-booster-box-b31",
    },
    {
        "id": "BT31",
        "shop": "AlpsTCG",
        "country": "NL",
        "url": "https://alpstcg.nl/products/dragon-ball-super-card-game-masters-ultra-bout-series-b-31-booster-display-24-packs-en-pre-order",
    },
    {
        "id": "BT31",
        "shop": "Arly Trading",
        "country": "NL",
        "url": "https://arlytrading.nl/winkel/preorder/dragon-ball-super-card-game-masters-ultra-bout-series-b-31-boosterbox/",
    },
    # ── FB10 (Fusion World Cross Force) ──
    {
        "id": "FB10",
        "shop": "Dracoon",
        "country": "NL",
        "url": "https://dracoon.nl/product/fusion-world-cross-force-fb10-boosterbox/",
    },
    {
        "id": "FB10",
        "shop": "TCGHaven",
        "country": "NL",
        "url": "https://tcghaven.nl/products/pre-order-dragon-ball-super-card-game-fusion-world-fb10",
    },
    {
        "id": "FB10",
        "shop": "Gamerz Paradize",
        "country": "NL",
        "url": "https://gamerzparadize.nl/en/products/dragon-ball-scg-fusion-world-10-booster-box",
    },
]

# ─── Filters ─────────────────────────────────────────────────────────────

# Title MUST contain one of these to be considered a booster box.
BOOSTER_BOX_KEYWORDS = [
    "booster box", "booster display", "display box", "boosterbox",
    "boosterdisplay", "display 24", "display 36", "24 boosters",
    "display van 24", "box of 24",
]

# Title MUST contain Dragon Ball branding.
DRAGONBALL_KEYWORDS = [
    "dragon ball", "dragonball", "dbs ", "dbsccg", "dbs-ccg",
    "dragon-ball",
]

# We ONLY care about Masters series + Fusion World. All older DB series ignored.
ALLOWED_SERIES_KEYWORDS = [
    "masters",
    "fusion world", "fusionworld", "fusion-world",
]
# Also allow if set code matches Masters (B25+) or Fusion World (FB##)
ALLOWED_SERIES_REGEX = re.compile(
    r"(?<![a-z0-9])("
    r"fb[\s\-]?\d{1,2}"           # FB01..FB99
    r"|b[\s\-]?(2[5-9]|[3-9]\d)"  # B25..B99 = Masters era
    r")(?![a-z0-9])"
)

# Old/blocked DB series we explicitly do NOT want.
BLOCKED_SERIES_KEYWORDS = [
    "zenkai", "unison warrior", "vermilion bloodline", "battle evolution",
    "world champion", "supreme rivalry", "miraculous revival",
    "dawn of the z-legends", "rise of the unison", "saiyan showdown",
    "ultimate squad", "cross spirits", "ultimate deck", "destroyer kings",
    "expert deck", "themed booster", "perfect combination", "fighter's ambition",
    "absolute approach", "magnificent collection", "tournament of power",
    "colossal warfare", "union force", "draft box", "special anniversary",
]

# Hard exclude (case, sleeves, single packs, ETB, etc).
EXCLUDE_KEYWORDS = [
    "acryl", "acrylic", "case voor", "display case", "opbergdoos",
    "beschermhoes", "sleeves", "protector", "magnetische", "playmat",
    "boosterpack", "booster pack", "1 pakje", "single pack",
    "elite trainer", "etb", "tin box", "collection box", "premium collection",
    "starter deck", "starter box", "deck box", "promo pack", "promo card",
    "bundle starter", "binder", "portfolio", "gift set", "gift box",
    "deck case", "card sleeve", "playmat",
]

PREORDER_KEYWORDS = [
    "pre-order", "preorder", "pre order", "voorbestelling", "presale",
    "coming soon", "verwacht", "binnenkort beschikbaar", "verschijnt",
    "release date", "to be released", "available from",
]

OUT_OF_STOCK_KEYWORDS = [
    "uitverkocht", "niet op voorraad", "out of stock", "sold out",
    "currently unavailable", "niet leverbaar", "tijdelijk uitverkocht",
    "wachtlijst", "notify me", "back in stock",
    # Dutch shop variants seen on Arly Trading, AlpsTCG, etc
    "momenteel niet beschikbaar", "niet beschikbaar", "tijdelijk niet beschikbaar",
    "voorraad: 0", "geen voorraad", "stock: 0", "out-of-stock",
    "nicht verfügbar", "epuisé",  # DE/FR for international shops
    "houd mij op de hoogte", "laat het mij weten", "email me",
]

IN_STOCK_KEYWORDS = [
    "op voorraad", "in stock", "direct leverbaar", "vandaag verzonden",
    "morgen in huis", "available now",
]

# ─── Shop Configurations (NL first, then EU/UK) ──────────────────────────

SHOP_SEARCHES = [
    # ── Nederlandse shops ──
    {
        "name": "Bol.com",
        "country": "NL",
        "url": "https://www.bol.com/nl/nl/s/?searchtext=dragon+ball+booster+box",
        "extractor": "bol",
    },
    {
        "name": "Bol.com",
        "country": "NL",
        "url": "https://www.bol.com/nl/nl/s/?searchtext=dragon+ball+fusion+world+booster",
        "extractor": "bol",
    },
    {
        "name": "Bol.com",
        "country": "NL",
        "url": "https://www.bol.com/nl/nl/s/?searchtext=dragon+ball+masters+booster",
        "extractor": "bol",
    },
    {
        "name": "Ludofy",
        "country": "NL",
        "url": "https://ludofy.com/search?q=dragon+ball+fusion+world",
        "extractor": "generic_shop",
    },
    {
        "name": "TBH Store",
        "country": "NL",
        "url": "https://www.tbhstore.nl/zoeken/?q=dragon+ball+booster+box",
        "extractor": "generic_shop",
    },
    {
        "name": "TBH Store",
        "country": "NL",
        "url": "https://www.tbhstore.nl/c-7253195/dragon-ball-tcg/",
        "extractor": "generic_shop",
    },
    {
        "name": "Pokeca",
        "country": "NL",
        "url": "https://pokeca.nl/collections/dragon-ball-super",
        "extractor": "generic_shop",
    },
    {
        "name": "Spellenvariant",
        "country": "NL",
        "url": "https://www.spellenvariant.nl/trading-card-games/dragon-ball-tcg",
        "extractor": "generic_shop",
    },
    {
        "name": "Gamerz Paradize",
        "country": "NL",
        "url": "https://gamerzparadize.nl/collections/brand-dragon-ball-super-card-game",
        "extractor": "generic_shop",
    },
    {
        "name": "Oppacards",
        "country": "NL",
        "url": "https://oppacards.com/product-category/dragon-ball-super/",
        "extractor": "generic_shop",
    },
    {
        "name": "TF-Robots",
        "country": "NL",
        "url": "https://www.tf-robots.nl/?s=dragon+ball+booster",
        "extractor": "generic_shop",
    },
    {
        "name": "Dracoon",
        "country": "NL",
        "url": "https://dracoon.nl/product-category/trading-card-games/dragon-ball-super-card-game/",
        "extractor": "generic_shop",
    },
    {
        "name": "TCGHaven",
        "country": "NL",
        "url": "https://tcghaven.nl/collections/dragon-ball-super",
        "extractor": "generic_shop",
    },
    {
        "name": "AlpsTCG",
        "country": "NL",
        "url": "https://alpstcg.nl/collections/dragon-ball-super-card-game",
        "extractor": "generic_shop",
    },
    {
        "name": "TcgReus",
        "country": "NL",
        "url": "https://www.tcgreus.nl/en/categories/dragon-ball-super-card-game",
        "extractor": "generic_shop",
    },
    {
        "name": "Arly Trading",
        "country": "NL",
        "url": "https://arlytrading.nl/productcategorie/dragon-ball-super-card-game/",
        "extractor": "generic_shop",
    },
    {
        "name": "Gamerz Paradize (booster boxen)",
        "country": "NL",
        "url": "https://gamerzparadize.nl/en/collections/dragon-ball-booster-boxen",
        "extractor": "generic_shop",
    },
    {
        "name": "Card Game Shop BE",
        "country": "BE",
        "url": "https://www.cardgameshop.be/en/categories/dragon-ball",
        "extractor": "generic_shop",
    },
    # ── EU marketplace ──
    {
        "name": "Cardmarket",
        "country": "EU",
        "url": "https://www.cardmarket.com/en/DragonBallSuperFusionWorld/Products/Booster-Boxes",
        "extractor": "cardmarket",
    },
    {
        "name": "Cardmarket",
        "country": "EU",
        "url": "https://www.cardmarket.com/en/DragonBallSuper/Products/Booster-Boxes",
        "extractor": "cardmarket",
    },
    # ── UK shops ──
    {
        "name": "Magic Madhouse",
        "country": "UK",
        "url": "https://www.magicmadhouse.co.uk/search?w=dragon+ball+booster+box",
        "extractor": "generic_shop",
    },
    {
        "name": "Chaos Cards",
        "country": "UK",
        "url": "https://www.chaoscards.co.uk/cards/dragon-ball-super/sealed-product",
        "extractor": "generic_shop",
    },
    {
        "name": "Total Cards",
        "country": "UK",
        "url": "https://www.totalcards.net/dragon-ball-super-card-game?p=1&search=booster+box",
        "extractor": "generic_shop",
    },
    # ── DE / BE ──
    {
        "name": "Fantasywelt",
        "country": "DE",
        "url": "https://www.fantasywelt.de/Suche?q=dragon+ball+booster+display",
        "extractor": "generic_shop",
    },
    # ── Amazon (NL + UK) ──
    {
        "name": "Amazon.nl",
        "country": "NL",
        "url": "https://www.amazon.nl/s?k=dragon+ball+super+card+game+booster+box&i=toys",
        "extractor": "amazon",
    },
    {
        "name": "Amazon.nl",
        "country": "NL",
        "url": "https://www.amazon.nl/s?k=dragon+ball+fusion+world+booster+display&i=toys",
        "extractor": "amazon",
    },
    {
        "name": "Amazon.co.uk",
        "country": "UK",
        "url": "https://www.amazon.co.uk/s?k=dragon+ball+super+card+game+booster+box",
        "extractor": "amazon",
    },
]

# ─── News Sources ────────────────────────────────────────────────────────
# Dragon Ball specific. PokeBeach / PokeGuardian removed.

NEWS_SOURCES = [
    {
        "name": "DBS Card Game (Official)",
        "url": "https://www.dbs-cardgame.com/us/news/",
    },
    {
        "name": "Fusion World (Official)",
        "url": "https://www.dbs-cardgame.com/fw/en/news/",
    },
    {
        "name": "Reddit r/DragonballTCG",
        "url": "https://old.reddit.com/r/DragonballTCG/new/",
    },
    {
        "name": "Reddit r/DragonballSuperTCG",
        "url": "https://old.reddit.com/r/DragonballSuperTCG/new/",
    },
]

NEWS_KEYWORDS = [
    "booster box", "booster display", "release date", "releasing", "release in",
    "pre-order", "preorder", "announced", "reveal", "spoilers", "card list",
    "set list", "expansion", "next set", "upcoming", "b31", "fb11",
    "fb12", "b32", "masters", "fusion world",
]

# ─── Extractors ──────────────────────────────────────────────────────────

EXTRACTOR_JS = {
    "bol": """() => {
        const results = [];
        const links = document.querySelectorAll('a[href*="/p/"]');
        const seen = new Set();
        for (const a of links) {
            const href = a.href || '';
            const text = a.textContent.trim();
            if (text.length < 15 || text.length > 300) continue;
            if (text === 'Bekijk en bestel') continue;
            if (seen.has(href)) continue;
            seen.add(href);
            let container = a.parentElement;
            for (let i = 0; i < 8 && container; i++) {
                if ((container.textContent || '').match(/\\d+[.,]\\d{2}/)) break;
                container = container.parentElement;
            }
            const allText = container ? (container.textContent || '') : text;
            const priceMatch = allText.match(/(\\d+)[.,](\\d{2})/);
            const price = priceMatch ? '€' + priceMatch[1] + ',' + priceMatch[2] : '';
            results.push({
                title: text.substring(0, 200),
                url: href.substring(0, 300),
                price: price || 'Prijs onbekend',
                fullText: allText.toLowerCase().substring(0, 800)
            });
        }
        return results;
    }""",

    "amazon": """() => {
        const results = [];
        const items = document.querySelectorAll('[data-component-type="s-search-result"]');
        for (const el of items) {
            const titleEl = el.querySelector('h2 a span, h2 span');
            const linkEl = el.querySelector('h2 a');
            const priceEl = el.querySelector('.a-price .a-offscreen');
            if (!titleEl) continue;
            const title = titleEl.textContent.trim();
            const href = linkEl ? linkEl.href : '';
            const price = priceEl ? priceEl.textContent.trim() : 'Prijs onbekend';
            results.push({
                title: title.substring(0, 200),
                url: href.substring(0, 300),
                price: price,
                fullText: el.textContent.toLowerCase().substring(0, 800)
            });
        }
        return results;
    }""",

    "cardmarket": """() => {
        const results = [];
        const links = document.querySelectorAll('a[href*="/Products/"]');
        const seen = new Set();
        for (const a of links) {
            const href = a.href || '';
            let text = a.textContent.trim();
            if (text.length < 10 || text.length > 300) continue;
            if (seen.has(href) || !href.includes('Booster-Box')) continue;
            seen.add(href);
            text = text.replace(/From\\s*[\\d.,]+\\s*€/gi, '').trim();
            const parent = a.closest('tr, .row, div') || a.parentElement;
            const pText = parent ? parent.textContent : text;
            const priceMatch = pText.match(/(\\d+)[.,](\\d{2})\\s*€/);
            const price = priceMatch ? '€' + priceMatch[1] + ',' + priceMatch[2] : '';
            results.push({
                title: text.substring(0, 200),
                url: href.startsWith('http') ? href.substring(0, 300) : 'https://www.cardmarket.com' + href,
                price: price || 'Prijs onbekend',
                fullText: pText.toLowerCase().substring(0, 800)
            });
        }
        return results;
    }""",

    # Generic extractor: works on most Shopify/WooCommerce/Magento shops.
    # Looks for product links with prices nearby.
    "generic_shop": """() => {
        const results = [];
        // Common product link selectors across NL/EU TCG shops
        const linkSelectors = [
            'a.product-item-link',
            'a.product-link',
            'a.product-title-link',
            'a.product-title',
            'a.product-name',
            '.product a[href]',
            '.product-item a[href]',
            '.product-card a[href]',
            'article a[href]',
            'h2 a[href]',
            'h3 a[href]',
            'a[href*="/product"]',
            'a[href*="/products/"]',
            'a[href*="/p/"]',
        ];
        const links = document.querySelectorAll(linkSelectors.join(', '));
        const seen = new Set();
        for (const a of links) {
            const href = a.href || '';
            // Prefer aria-label or alt text from img inside (cleanest for Shopify).
            // Fall back to textContent stripped of script/style/HTML noise.
            let text = (a.getAttribute('aria-label') || '').trim();
            if (!text) {
                const img = a.querySelector('img[alt]');
                if (img) text = (img.getAttribute('alt') || '').trim();
            }
            if (!text) {
                text = a.textContent.replace(/<[^>]+>/g, ' ').replace(/\\s+/g, ' ').trim();
            }
            if (text.length < 10 || text.length > 300) continue;
            if (seen.has(href)) continue;
            // Skip nav/category/login links
            if (/\\b(login|account|cart|winkelwagen|menu|home)\\b/i.test(text)) continue;
            // Skip embedded HTML/JS leakage
            if (/^<|noscript|cdn\\.shop/i.test(text)) continue;
            seen.add(href);
            const parent = a.closest('article, .product, .product-item, .product-card, li, div');
            const pText = parent ? parent.textContent : text;
            const priceMatch = pText.match(/[€£]\\s*(\\d+)[.,](\\d{2})|(\\d+)[.,](\\d{2})\\s*[€£]/);
            let price = 'Prijs onbekend';
            if (priceMatch) {
                const m = priceMatch[0];
                price = m.replace(/\\s+/g, '');
                if (!price.includes('€') && !price.includes('£')) price = '€' + price;
            }
            results.push({
                title: text.substring(0, 200),
                url: href.substring(0, 300),
                price: price,
                fullText: pText.toLowerCase().substring(0, 800)
            });
        }
        return results;
    }""",
}

# Deep product-page check: opens a single product page and reads the
# add-to-cart button + stock indicators directly. Far more accurate than
# the listing-page heuristic. Used for B31/FB11 hits only.
DEEP_CHECK_JS = """() => {
    const bodyText = (document.body.innerText || '').toLowerCase();
    // Refined price: collect ALL prices on page, skip 0/sub-min, prefer highest
    // (booster box prices €40-€300 range; lower values are typically strikethroughs
    // for accessories or zero-fields).
    const priceRegex = /[€£]\\s*(\\d+)[.,](\\d{2})|(\\d+)[.,](\\d{2})\\s*[€£]/g;
    const allPrices = [];
    let m;
    while ((m = priceRegex.exec(bodyText)) !== null) {
        const euros = parseInt(m[1] || m[3]);
        const cents = parseInt(m[2] || m[4]);
        const num = euros + cents / 100;
        if (num >= 25 && num <= 500) allPrices.push({num, raw: m[0]});
    }
    let price = null;
    if (allPrices.length) {
        // Use the most common price if multiple, else max
        allPrices.sort((a, b) => b.num - a.num);
        price = allPrices[0].raw.replace(/\\s+/g, '');
        if (!price.includes('€') && !price.includes('£')) price = '€' + price;
    }
    // Add-to-cart button presence (and not disabled)
    const cartSelectors = [
        'button[name="add"]', 'button.add-to-cart', 'button.product-add',
        'form[action*="cart"] button[type="submit"]', '[class*="AddToCart"]',
        '#add-to-cart', 'button[data-action="add-to-cart"]',
        'button:not([disabled]).btn-cart', 'button.add_to_cart_button',
    ];
    let cartBtn = null;
    for (const sel of cartSelectors) {
        const b = document.querySelector(sel);
        if (b) { cartBtn = b; break; }
    }
    const cartEnabled = !!cartBtn && !cartBtn.disabled && !cartBtn.classList.contains('disabled');
    // Notify-me / waitlist signals
    const notifySignals = [
        'notify me', 'op de hoogte', 'mail mij', 'wachtlijst',
        'sign up to be notified', 'back in stock', 'meld u aan',
    ];
    const hasNotify = notifySignals.some(s => bodyText.includes(s));
    return {
        price: price,
        cart_enabled: cartEnabled,
        has_notify_signup: hasNotify,
        body_excerpt: bodyText.substring(0, 2000),
    };
}"""


NEWS_EXTRACTOR_JS = """() => {
    const results = [];
    const els = document.querySelectorAll(
        'h1 a, h2 a, h3 a, article a, .post-title a, .entry-title a, ' +
        '.news-item a, [class*="headline"] a, [class*="title"] a, ' +
        'a.title, .Post a[data-event-action="title"]'
    );
    const seen = new Set();
    for (const el of els) {
        const text = el.textContent.trim();
        const href = el.href || '';
        if (text.length > 10 && text.length < 300 && href && !seen.has(href)) {
            seen.add(href);
            results.push({ title: text.substring(0, 250), url: href.substring(0, 300) });
        }
    }
    return results.slice(0, 30);
}"""


# ─── Helpers ─────────────────────────────────────────────────────────────

def load_json(path):
    if path.exists():
        return json.loads(path.read_text())
    return {}


def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False))


def make_hash(key):
    return hashlib.md5(key.encode()).hexdigest()


def canonical_url(url):
    """Normalize URL for dedup: lowercase, strip query/fragment, drop www, strip trailing /."""
    if not url:
        return ""
    u = url.lower().split("?")[0].split("#")[0].rstrip("/")
    u = u.replace("://www.", "://")
    return u


# ─── Self-healing: shop health tracking ─────────────────────────────────

def load_health():
    return load_json(HEALTH_FILE) or {"shops": {}, "priority_urls": {}}


def record_shop_result(health, shop_key, success, error=None, products_found=0):
    """Track per-shop success/failure. Returns True if shop should be auto-disabled now."""
    s = health["shops"].setdefault(shop_key, {
        "consecutive_failures": 0,
        "consecutive_zero_results": 0,
        "last_success": None,
        "last_error": None,
        "disabled": False,
        "alerted_disabled": False,
    })
    if success:
        s["consecutive_failures"] = 0
        s["last_success"] = datetime.now().isoformat()
        s["last_error"] = None
        if products_found == 0:
            s["consecutive_zero_results"] += 1
        else:
            s["consecutive_zero_results"] = 0
    else:
        s["consecutive_failures"] += 1
        s["last_error"] = str(error)[:200] if error else "unknown"

    # Auto-disable after MAX_CONSECUTIVE_FAILURES hard errors (DNS, timeouts, etc).
    # Zero-results doesn't auto-disable but flags warning.
    just_disabled = False
    if s["consecutive_failures"] >= MAX_CONSECUTIVE_FAILURES and not s["disabled"]:
        s["disabled"] = True
        just_disabled = True
        log.warning(f"AUTO-DISABLED shop '{shop_key}' after {s['consecutive_failures']} failures: {s['last_error']}")
    return just_disabled


def is_shop_disabled(health, shop_key):
    s = health["shops"].get(shop_key)
    return bool(s and s.get("disabled"))


def record_priority_url_result(health, key, status, error=None):
    """Track health of priority URL deep checks."""
    p = health["priority_urls"].setdefault(key, {
        "consecutive_unknown": 0,
        "consecutive_failures": 0,
        "last_buyable": None,
        "last_error": None,
    })
    if error:
        p["consecutive_failures"] += 1
        p["last_error"] = str(error)[:200]
    elif status == "unknown":
        p["consecutive_unknown"] += 1
    else:
        p["consecutive_unknown"] = 0
        p["consecutive_failures"] = 0
        p["last_error"] = None
        if status in ("in_stock", "preorder"):
            p["last_buyable"] = datetime.now().isoformat()


def parse_price(price_str):
    """Extract numeric price for comparison. Returns float or None."""
    if not price_str or price_str == "Prijs onbekend":
        return None
    m = re.search(r"(\d+)[.,](\d{2})", price_str)
    if not m:
        return None
    try:
        return float(f"{m.group(1)}.{m.group(2)}")
    except ValueError:
        return None


def detect_stock_status(full_text):
    """Returns: 'preorder' | 'in_stock' | 'out_of_stock' | 'unknown'.

    OOS wins over preorder: a 'pre-order' product can be sold out (no more
    pre-orders accepted). Don't alert in that case.
    """
    text = full_text.lower()
    if any(kw in text for kw in OUT_OF_STOCK_KEYWORDS):
        return "out_of_stock"
    if any(kw in text for kw in PREORDER_KEYWORDS):
        return "preorder"
    if any(kw in text for kw in IN_STOCK_KEYWORDS):
        return "in_stock"
    return "unknown"


def deep_check_product(context, url):
    """Open a product detail page and verify stock via add-to-cart + body text.

    Returns dict with: stock_status, price (if found), cart_enabled, raw signals.
    Returns None on error.
    """
    page = context.new_page()
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=20000)
        page.wait_for_timeout(2000)
        accept_cookies(page)
        page.wait_for_timeout(1000)
        result = page.evaluate(DEEP_CHECK_JS)
        body = result.get("body_excerpt", "")
        cart_enabled = bool(result.get("cart_enabled"))
        has_oos = any(kw in body for kw in OUT_OF_STOCK_KEYWORDS) or result.get("has_notify_signup")
        has_preorder = any(kw in body for kw in PREORDER_KEYWORDS)
        has_instock = any(kw in body for kw in IN_STOCK_KEYWORDS)

        # OOS signals win over preorder wording: a "pre-order" product can still be sold out.
        # Notify-me/wachtlijst widgets are a strong OOS signal even if the page also says
        # "pre-order" elsewhere.
        if has_oos and not cart_enabled:
            status = "out_of_stock"
        elif cart_enabled and has_preorder:
            status = "preorder"
        elif cart_enabled:
            status = "in_stock"
        elif has_preorder:
            status = "preorder"
        elif has_instock:
            status = "in_stock"
        else:
            status = "unknown"
        return {
            "stock_status": status,
            "price": result.get("price"),
            "cart_enabled": result.get("cart_enabled", False),
            "deep_checked_at": datetime.now().isoformat(),
        }
    except Exception as e:
        log.warning(f"  Deep check failed for {url}: {e}")
        return None
    finally:
        page.close()


def is_dragonball_booster_box(title):
    """Strict: title must be a Dragon Ball Masters or Fusion World booster BOX.

    Rules (all on title only, fullText is too noisy on grid pages):
    1. Must contain a booster-box keyword
    2. Must NOT contain a hard-exclude keyword (sleeves, ETB, single pack, etc)
    3. Must NOT contain a blocked old-series keyword (Zenkai, Unison, etc)
    4. Must mention Masters / Fusion World OR a Masters/FW set code (B25+ or FB##)
       OR explicit Dragon Ball branding. Series code alone is enough since FB##
       and B25+ are unique to Dragon Ball Super CCG.
    """
    title_lower = title.lower()
    if not any(kw in title_lower for kw in BOOSTER_BOX_KEYWORDS):
        return False
    if any(kw in title_lower for kw in EXCLUDE_KEYWORDS):
        return False
    if any(kw in title_lower for kw in BLOCKED_SERIES_KEYWORDS):
        return False
    has_dragonball = any(kw in title_lower for kw in DRAGONBALL_KEYWORDS)
    has_fw_or_code = (
        "fusion world" in title_lower
        or "fusionworld" in title_lower
        or ALLOWED_SERIES_REGEX.search(title_lower) is not None
    )
    has_masters_with_code = "masters" in title_lower and ALLOWED_SERIES_REGEX.search(title_lower) is not None
    if not (has_dragonball or has_fw_or_code or has_masters_with_code):
        return False
    return True


# Booster box minimum price (€). Anything below this is almost certainly an
# accessory / bundle / single pack misidentified as a box.
MIN_BOOSTER_BOX_PRICE = 25.0


EBAY_URL_RE = re.compile(r"(^|//)(www\.|m\.)?ebay\.", re.IGNORECASE)


def is_shop_url(url):
    """Block eBay (marketplace, not shop). Only real shops allowed."""
    if not url:
        return False
    return not EBAY_URL_RE.search(url)


def detect_priority_match(title):
    """Returns watchlist entry dict if title matches B31 or FB11 exactly, else None.

    Title-only (no fullText): set codes are short, fullText leakage from
    neighbouring products causes false positives.
    """
    title_lower = title.lower()
    for entry in PRIORITY_WATCHLIST:
        for pattern in entry["patterns"]:
            if re.search(pattern, title_lower):
                return entry
    return None


def is_relevant_news(title):
    """News must mention Dragon Ball + Masters/FW + a news keyword. No Pokemon, no other DB series, no eBay listings."""
    title_lower = title.lower()
    has_dragonball = any(kw in title_lower for kw in ["dragon ball", "dragonball", "dbs", "dbsccg"])
    if not has_dragonball:
        return False
    if "ebay" in title_lower:
        return False
    if any(kw in title_lower for kw in BLOCKED_SERIES_KEYWORDS):
        return False
    has_target_series = (
        any(kw in title_lower for kw in ALLOWED_SERIES_KEYWORDS)
        or ALLOWED_SERIES_REGEX.search(title_lower)
    )
    has_news_kw = any(kw in title_lower for kw in NEWS_KEYWORDS)
    return has_target_series and has_news_kw


# ─── Browser ─────────────────────────────────────────────────────────────

def accept_cookies(page):
    for btn in [
        "#sp-cc", "#onetrust-accept-btn-handler",
        "button:has-text('Accepteren')", "button:has-text('Accept All')",
        "button:has-text('Allow All')", "button:has-text('Alle cookies accepteren')",
        "button:has-text('Alles accepteren')", "#js-first-screen-accept",
        "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
        "button:has-text('Agree')", "button:has-text('I Accept')",
        "button:has-text('Akkoord')",
    ]:
        try:
            page.click(btn, timeout=1500)
            page.wait_for_timeout(500)
            return True
        except Exception:
            continue
    return False


# ─── Scraping ────────────────────────────────────────────────────────────

def scrape_shops(context):
    """Scrape all shop searches. Returns (new_products, status_changes, price_drops)."""
    seen = load_json(SEEN_PRODUCTS_FILE)
    price_history = load_json(PRICE_HISTORY_FILE)
    health = load_health()

    new_products = []
    status_changes = []  # (product_dict, old_status, new_status)
    price_drops = []     # (product_dict, old_price, new_price)
    auto_disabled_now = []  # for Telegram heads-up

    for search in SHOP_SEARCHES:
        shop_key = f"{search['name']}|{search['url']}"
        if is_shop_disabled(health, shop_key):
            log.info(f"SKIP (auto-disabled): {search['name']} ({search['country']})")
            continue

        page = context.new_page()
        try:
            log.info(f"Checking {search['name']} ({search['country']})...")
            page.goto(search["url"], wait_until="domcontentloaded", timeout=20000)
            page.wait_for_timeout(2000)
            accept_cookies(page)
            page.wait_for_timeout(1500)

            if search["extractor"] in ("cardmarket", "generic_shop"):
                for _ in range(4):
                    page.evaluate("window.scrollBy(0, 800)")
                    page.wait_for_timeout(400)

            js_code = EXTRACTOR_JS.get(search["extractor"])
            if not js_code:
                continue

            try:
                raw_products = page.evaluate(js_code)
            except Exception as e:
                log.warning(f"  Extraction failed: {e}")
                continue

            def _passes(p):
                if not is_dragonball_booster_box(p["title"]):
                    return False
                if not is_shop_url(p.get("url", "")):
                    return False
                # Filter accessories/bundles misidentified as boxes by checking price.
                # Allow products without a price (could be pre-order with hidden price).
                price_num = parse_price(p.get("price", ""))
                if price_num is not None and price_num < MIN_BOOSTER_BOX_PRICE:
                    return False
                return True

            relevant = [p for p in raw_products if _passes(p)]
            log.info(f"  Found {len(relevant)} Dragon Ball booster box products")
            if record_shop_result(health, shop_key, success=True, products_found=len(relevant)):
                auto_disabled_now.append(search["name"])

            for p in relevant:
                priority = detect_priority_match(p["title"])
                stock_status = detect_stock_status(p.get("fullText", ""))
                price = p["price"]
                price_num = parse_price(price)

                # Priority hits (B31/FB11) get a deep check on the product detail page.
                # Far more accurate stock detection (real add-to-cart button check).
                deep_checked = False
                if priority and p.get("url"):
                    log.info(f"  Deep check {priority['id']}: {p['url'][:80]}")
                    deep = deep_check_product(context, p["url"])
                    if deep:
                        stock_status = deep["stock_status"]
                        if deep.get("price"):
                            price = deep["price"]
                            price_num = parse_price(price) or price_num
                        deep_checked = True

                # Canonical key: normalized URL (dedupe across shop-scrape + priority-URL)
                h = make_hash(canonical_url(p["url"])) if p.get("url") else make_hash(f"{search['name']}|{p['title']}")

                product_record = {
                    "title": p["title"],
                    "shop": search["name"],
                    "country": search["country"],
                    "price": price,
                    "price_num": price_num,
                    "stock_status": stock_status,
                    "url": p["url"],
                    "priority": priority["id"] if priority else None,
                    "priority_series": priority["series"] if priority else None,
                    "deep_checked": deep_checked,
                    "last_seen": datetime.now().isoformat(),
                }

                if h not in seen:
                    # Brand new
                    product_record["first_seen"] = datetime.now().isoformat()
                    seen[h] = product_record
                    new_products.append(product_record)
                    tag = ""
                    if priority:
                        tag = f" [PRIORITY: {priority['id']}]"
                    elif stock_status == "preorder":
                        tag = " [PRE-ORDER]"
                    log.info(f"  NEW{tag}: {p['title'][:80]} | {p['price']}")
                else:
                    # Existing - check for changes
                    old = seen[h]
                    old_status = old.get("stock_status", "unknown")
                    old_price = old.get("price_num")

                    # Status transition (e.g. out_of_stock -> in_stock = restock!)
                    if old_status != stock_status and stock_status != "unknown":
                        status_changes.append((product_record, old_status, stock_status))
                        log.info(f"  STATUS: {p['title'][:60]} | {old_status} -> {stock_status}")

                    # Price drop (>5% lower)
                    if old_price and price_num and price_num < old_price * 0.95:
                        price_drops.append((product_record, old_price, price_num))
                        log.info(f"  PRICE DROP: {p['title'][:60]} | €{old_price} -> €{price_num}")

                    # Update record (preserve first_seen)
                    product_record["first_seen"] = old.get("first_seen", datetime.now().isoformat())
                    seen[h] = product_record

                # Append to price history
                if price_num is not None:
                    price_history.setdefault(h, []).append({
                        "ts": datetime.now().isoformat(),
                        "price": price_num,
                        "stock_status": stock_status,
                    })
                    # Cap history at 200 points per product
                    price_history[h] = price_history[h][-200:]

        except Exception as e:
            log.warning(f"  Error scraping {search['name']}: {e}")
            if record_shop_result(health, shop_key, success=False, error=e):
                auto_disabled_now.append(search["name"])
        finally:
            page.close()

    save_json(SEEN_PRODUCTS_FILE, seen)
    save_json(PRICE_HISTORY_FILE, price_history)
    save_json(HEALTH_FILE, health)

    # One-time Telegram heads-up for shops that just got auto-disabled
    if auto_disabled_now and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        send_telegram(
            "<b>Monitor self-heal:</b>\n"
            f"Auto-disabled {len(auto_disabled_now)} broken shop(s) "
            f"after {MAX_CONSECUTIVE_FAILURES} consecutive failures:\n\n"
            + "\n".join(f"- {name}" for name in auto_disabled_now)
            + "\n\nUpdate URLs in monitor.py and re-enable in health.json."
        )
        # Mark as alerted so we don't re-ping
        for search in SHOP_SEARCHES:
            if search["name"] in auto_disabled_now:
                k = f"{search['name']}|{search['url']}"
                if k in health["shops"]:
                    health["shops"][k]["alerted_disabled"] = True
        save_json(HEALTH_FILE, health)

    return new_products, status_changes, price_drops


def scrape_news(context):
    seen = load_json(SEEN_NEWS_FILE)
    new_news = []

    for source in NEWS_SOURCES:
        page = context.new_page()
        try:
            log.info(f"Checking news: {source['name']}...")
            page.goto(source["url"], wait_until="domcontentloaded", timeout=15000)
            page.wait_for_timeout(2000)
            accept_cookies(page)
            page.wait_for_timeout(1000)

            headlines = page.evaluate(NEWS_EXTRACTOR_JS)
            relevant = [
                h for h in headlines
                if is_relevant_news(h["title"]) and is_shop_url(h.get("url", ""))
            ]
            log.info(f"  {len(relevant)} relevant (of {len(headlines)} total)")

            for article in relevant:
                h = make_hash(article["url"])
                if h not in seen:
                    # Detect priority mention in headline
                    priority = detect_priority_match(article["title"])
                    record = {
                        "title": article["title"],
                        "source": source["name"],
                        "url": article["url"],
                        "priority": priority["id"] if priority else None,
                        "first_seen": datetime.now().isoformat(),
                    }
                    seen[h] = record
                    new_news.append(record)
                    tag = f" [PRIORITY: {priority['id']}]" if priority else ""
                    log.info(f"  NEW{tag}: {article['title'][:90]}")

        except Exception as e:
            log.warning(f"  Error: {e}")
        finally:
            page.close()

    save_json(SEEN_NEWS_FILE, seen)
    return new_news


def scrape_priority_urls(context):
    """Always deep-check known direct B31/FB11 product URLs.

    These URLs may not be discoverable via shop search/category pages (they may
    be hidden, require category filters, or the shop's search may be broken).
    Returns same tuple shape as scrape_shops.
    """
    seen = load_json(SEEN_PRODUCTS_FILE)
    price_history = load_json(PRICE_HISTORY_FILE)
    health = load_health()
    new_products = []
    status_changes = []
    price_drops = []
    broken_priority = []

    for entry in PRIORITY_PRODUCT_URLS:
        url_key = f"{entry['shop']}|{entry['id']}|{entry['url']}"
        log.info(f"Priority URL check: {entry['shop']} {entry['id']} -> {entry['url'][:80]}")
        deep = deep_check_product(context, entry["url"])
        if not deep:
            record_priority_url_result(health, url_key, status=None, error="deep check failed")
            p = health["priority_urls"].get(url_key, {})
            if p.get("consecutive_failures", 0) == MAX_CONSECUTIVE_FAILURES:
                broken_priority.append(f"{entry['shop']} {entry['id']}")
            continue
        record_priority_url_result(health, url_key, status=deep["stock_status"])
        # Get title from the page itself
        page = context.new_page()
        title = entry["id"]
        try:
            page.goto(entry["url"], wait_until="domcontentloaded", timeout=15000)
            page.wait_for_timeout(1000)
            title = page.evaluate("() => (document.querySelector('h1, .product-title, .product-name')?.textContent || document.title || '').trim().substring(0, 200)") or entry["id"]
        except Exception:
            pass
        finally:
            page.close()

        watchlist = next((w for w in PRIORITY_WATCHLIST if w["id"] == entry["id"]), None)
        price = deep.get("price") or "Prijs onbekend"
        price_num = parse_price(price)
        # Canonical key: normalized URL (matches scrape_shops, prevents dupes)
        h = make_hash(canonical_url(entry["url"]))

        product_record = {
            "title": title,
            "shop": entry["shop"],
            "country": entry["country"],
            "price": price,
            "price_num": price_num,
            "stock_status": deep["stock_status"],
            "url": entry["url"],
            "priority": entry["id"],
            "priority_series": watchlist["series"] if watchlist else "",
            "deep_checked": True,
            "last_seen": datetime.now().isoformat(),
        }

        if h not in seen:
            product_record["first_seen"] = datetime.now().isoformat()
            seen[h] = product_record
            new_products.append(product_record)
            log.info(f"  NEW [{entry['id']}]: {title[:80]} | {price} | {deep['stock_status']}")
        else:
            old = seen[h]
            old_status = old.get("stock_status", "unknown")
            old_price = old.get("price_num")
            if old_status != deep["stock_status"] and deep["stock_status"] != "unknown":
                status_changes.append((product_record, old_status, deep["stock_status"]))
                log.info(f"  STATUS [{entry['id']}]: {old_status} -> {deep['stock_status']}")
            if old_price and price_num and price_num < old_price * 0.95:
                price_drops.append((product_record, old_price, price_num))
            product_record["first_seen"] = old.get("first_seen", datetime.now().isoformat())
            seen[h] = product_record

        if price_num is not None:
            price_history.setdefault(h, []).append({
                "ts": datetime.now().isoformat(),
                "price": price_num,
                "stock_status": deep["stock_status"],
            })
            price_history[h] = price_history[h][-200:]

    save_json(SEEN_PRODUCTS_FILE, seen)
    save_json(PRICE_HISTORY_FILE, price_history)
    save_json(HEALTH_FILE, health)

    if broken_priority and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        send_telegram(
            "<b>Monitor self-heal: broken priority URL(s)</b>\n\n"
            f"{MAX_CONSECUTIVE_FAILURES} consecutive failures on:\n"
            + "\n".join(f"- {x}" for x in broken_priority)
            + "\n\nLikely the shop changed the URL. Update PRIORITY_PRODUCT_URLS in monitor.py."
        )

    return new_products, status_changes, price_drops


def scrape_all(priority_only=False):
    """Run scrapes. priority_only=True skips slow shop searches + news,
    only deep-checks PRIORITY_PRODUCT_URLS. Use for fast polling (every 15 min).
    """
    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            locale="nl-NL",
            viewport={"width": 1920, "height": 1080},
        )
        # Priority URLs always run (the highest-value check)
        prio_new, prio_status, prio_prices = scrape_priority_urls(context)
        if priority_only:
            new_products, status_changes, price_drops, new_news = prio_new, prio_status, prio_prices, []
        else:
            new_products, status_changes, price_drops = scrape_shops(context)
            new_products = prio_new + new_products
            status_changes = prio_status + status_changes
            price_drops = prio_prices + price_drops
            new_news = scrape_news(context)
        browser.close()

    return new_products, status_changes, price_drops, new_news


# ─── Telegram ────────────────────────────────────────────────────────────

def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("Telegram credentials not configured")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message[:4096],
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return True
    except requests.RequestException as e:
        log.error(f"Telegram send failed: {e}")
        return False


def send_priority_alert(product):
    """Loud alert for B31/FB11 hits."""
    series = product.get("priority_series", "")
    pid = product.get("priority", "")
    msg = (
        f"<b>!!! {pid} BOOSTER BOX GEVONDEN !!!</b>\n"
        f"<b>Serie:</b> Dragon Ball {series}\n\n"
        f"<b>Shop:</b> {product['shop']} ({product['country']})\n"
        f"<b>Product:</b> {product['title'][:120]}\n"
        f"<b>Prijs:</b> {product['price']}\n"
        f"<b>Status:</b> {product['stock_status']}\n\n"
        f"<a href=\"{product['url']}\">DIRECT BESTELLEN</a>"
    )
    send_telegram(msg)


def send_preorder_alert(product):
    msg = (
        f"<b>Nieuwe Dragon Ball Pre-Order</b>\n\n"
        f"<b>Shop:</b> {product['shop']} ({product['country']})\n"
        f"<b>Product:</b> {product['title'][:120]}\n"
        f"<b>Prijs:</b> {product['price']}\n\n"
        f"<a href=\"{product['url']}\">Pre-orderen</a>"
    )
    send_telegram(msg)


def send_restock_alert(product, old_status):
    msg = (
        f"<b>RESTOCK!</b>\n\n"
        f"<b>Shop:</b> {product['shop']} ({product['country']})\n"
        f"<b>Product:</b> {product['title'][:120]}\n"
        f"<b>Was:</b> {old_status} -> nu {product['stock_status']}\n"
        f"<b>Prijs:</b> {product['price']}\n\n"
        f"<a href=\"{product['url']}\">Bekijk</a>"
    )
    send_telegram(msg)


def send_price_drop_alert(product, old_price, new_price):
    pct = round((1 - new_price / old_price) * 100)
    msg = (
        f"<b>Prijs gedaald ({pct}%)</b>\n\n"
        f"<b>Shop:</b> {product['shop']} ({product['country']})\n"
        f"<b>Product:</b> {product['title'][:120]}\n"
        f"<b>Was:</b> €{old_price:.2f} -> nu €{new_price:.2f}\n\n"
        f"<a href=\"{product['url']}\">Bekijk</a>"
    )
    send_telegram(msg)


def send_news_digest(new_news):
    if not new_news:
        return
    priority_news = [n for n in new_news if n.get("priority")]
    regular_news = [n for n in new_news if not n.get("priority")]

    # Priority news = individual loud messages
    for article in priority_news:
        msg = (
            f"<b>!!! {article['priority']} NIEUWS !!!</b>\n\n"
            f"<b>Bron:</b> {article['source']}\n"
            f"<b>{article['title'][:200]}</b>\n\n"
            f"<a href=\"{article['url']}\">Lees meer</a>"
        )
        send_telegram(msg)
        time.sleep(0.5)

    # Regular news = digest
    if regular_news:
        lines = [f"<b>Dragon Ball TCG Nieuws ({len(regular_news)})</b>\n"]
        for article in regular_news[:10]:
            lines.append(
                f"- <b>{article['title'][:120]}</b>\n"
                f"  {article['source']} - <a href=\"{article['url']}\">link</a>\n"
            )
        if len(regular_news) > 10:
            lines.append(f"\n... en {len(regular_news) - 10} meer")
        send_telegram("\n".join(lines))


# ─── Dashboard Feed ──────────────────────────────────────────────────────

def write_dashboard_feed():
    """Write data.json for the dragonball-tracker frontend to consume."""
    seen_products = load_json(SEEN_PRODUCTS_FILE)
    seen_news = load_json(SEEN_NEWS_FILE)
    price_history = load_json(PRICE_HISTORY_FILE)
    health = load_health()

    products = list(seen_products.values())
    products.sort(key=lambda x: (x.get("priority") is None, -ord(x.get("last_seen", "")[0]) if x.get("last_seen") else 0, x.get("last_seen", "")), reverse=False)

    # Health summary for dashboard transparency: which shops are working, dead, or silent.
    shop_health = []
    for shop_key, s in (health.get("shops") or {}).items():
        name = shop_key.split("|", 1)[0]
        shop_health.append({
            "shop": name,
            "disabled": s.get("disabled", False),
            "consecutive_failures": s.get("consecutive_failures", 0),
            "consecutive_zero_results": s.get("consecutive_zero_results", 0),
            "last_success": s.get("last_success"),
            "last_error": s.get("last_error"),
        })
    priority_health = []
    for url_key, p in (health.get("priority_urls") or {}).items():
        parts = url_key.split("|")
        priority_health.append({
            "shop": parts[0] if len(parts) > 0 else "?",
            "id": parts[1] if len(parts) > 1 else "?",
            "consecutive_unknown": p.get("consecutive_unknown", 0),
            "consecutive_failures": p.get("consecutive_failures", 0),
            "last_buyable": p.get("last_buyable"),
            "last_error": p.get("last_error"),
        })

    feed = {
        "generated_at": datetime.now().isoformat(),
        "watchlist": [
            {"id": w["id"], "series": w["series"], "name": w["name"]}
            for w in PRIORITY_WATCHLIST
        ],
        "products": products,
        "news": list(seen_news.values()),
        "price_history": price_history,
        "stats": {
            "total_products": len(products),
            "preorders": sum(1 for p in products if p.get("stock_status") == "preorder"),
            "in_stock": sum(1 for p in products if p.get("stock_status") == "in_stock"),
            "out_of_stock": sum(1 for p in products if p.get("stock_status") == "out_of_stock"),
            "priority_hits": sum(1 for p in products if p.get("priority")),
            "news_articles": len(seen_news),
            "shops_total": len(shop_health),
            "shops_healthy": sum(1 for s in shop_health if not s["disabled"] and s["consecutive_failures"] == 0),
            "shops_disabled": sum(1 for s in shop_health if s["disabled"]),
        },
        "health": {
            "shops": shop_health,
            "priority_urls": priority_health,
        },
    }
    save_json(DASHBOARD_FEED_FILE, feed)
    log.info(f"Dashboard feed written: {DASHBOARD_FEED_FILE}")


# ─── Commands ────────────────────────────────────────────────────────────

BUYABLE_STATUSES = ("in_stock", "preorder")


def cmd_run(dry_run=False, priority_only=False):
    mode = "priority-only (fast)" if priority_only else "full"
    log.info(f"Starting Dragon Ball TCG monitor ({mode})...")
    new_products, status_changes, price_drops, new_news = scrape_all(priority_only=priority_only)

    # ALERT POLICY: only ping Telegram for products that are actually buyable
    # (in_stock or preorder). OOS / unknown are tracked silently.
    priority_hits = [
        p for p in new_products
        if p.get("priority") and p.get("stock_status") in BUYABLE_STATUSES
    ]
    new_preorders = [
        p for p in new_products
        if p.get("stock_status") == "preorder" and not p.get("priority")
    ]
    restocks = [
        (p, old) for p, old, new in status_changes
        if new in BUYABLE_STATUSES and old not in BUYABLE_STATUSES
    ]

    log.info(
        f"Results: {len(new_products)} new ({len(priority_hits)} PRIORITY, {len(new_preorders)} pre-orders), "
        f"{len(restocks)} restocks, {len(price_drops)} price drops, {len(new_news)} news"
    )

    if dry_run:
        for p in new_products:
            tag = f" [{p['priority']}]" if p.get("priority") else f" [{p['stock_status']}]"
            log.info(f"[DRY] {p['shop']} ({p['country']}){tag} | {p['title'][:80]} | {p['price']}")
        for p, old, new in status_changes:
            log.info(f"[DRY] STATUS: {p['title'][:60]} | {old} -> {new}")
        for p, old, new in price_drops:
            log.info(f"[DRY] PRICE: {p['title'][:60]} | €{old} -> €{new}")
        for n in new_news:
            tag = f" [{n['priority']}]" if n.get("priority") else ""
            log.info(f"[DRY] NEWS{tag} | {n['source']} | {n['title'][:90]}")
    else:
        # Alert-time dedup: never ping twice for the same (priority_id, shop) or URL
        sent_priority = set()
        sent_preorder = set()
        for product in priority_hits:
            key = (product.get("priority"), product["shop"])
            if key in sent_priority:
                continue
            sent_priority.add(key)
            send_priority_alert(product)
            time.sleep(0.5)
        for product in new_preorders:
            key = canonical_url(product.get("url", "")) or product["title"]
            if key in sent_preorder:
                continue
            sent_preorder.add(key)
            send_preorder_alert(product)
            time.sleep(0.5)
        for product, old in restocks:
            send_restock_alert(product, old)
            time.sleep(0.5)
        for product, old, new in price_drops:
            send_price_drop_alert(product, old, new)
            time.sleep(0.5)
        send_news_digest(new_news)

    write_dashboard_feed()
    return new_products, status_changes, price_drops, new_news


def cmd_list(priority_only=False):
    seen_products = load_json(SEEN_PRODUCTS_FILE)
    seen_news = load_json(SEEN_NEWS_FILE)

    products = list(seen_products.values())
    if priority_only:
        products = [p for p in products if p.get("priority")]

    if not products and not seen_news:
        print("Geen data in database.")
        return

    if products:
        priority = [p for p in products if p.get("priority")]
        regular = [p for p in products if not p.get("priority")]

        if priority:
            print(f"\n{'='*70}")
            print(f"  PRIORITY WATCHLIST HITS ({len(priority)})")
            print(f"{'='*70}")
            for p in sorted(priority, key=lambda x: x.get("first_seen", ""), reverse=True):
                print(f"  [{p['priority']}] {p['shop']:18s} ({p['country']}) | {p['price']:12s} | {p.get('stock_status','?'):12s} | {p['title'][:55]}")

        if regular and not priority_only:
            print(f"\n{'='*70}")
            print(f"  Alle Dragon Ball Booster Boxes ({len(regular)})")
            print(f"{'='*70}")
            for p in sorted(regular, key=lambda x: x.get("first_seen", ""), reverse=True):
                print(f"  {p['shop']:18s} ({p['country']}) | {p['price']:12s} | {p.get('stock_status','?'):12s} | {p['title'][:55]}")

    if seen_news and not priority_only:
        print(f"\n{'='*70}")
        print(f"  Nieuws ({len(seen_news)})")
        print(f"{'='*70}")
        for h, info in sorted(seen_news.items(), key=lambda x: x[1].get("first_seen", ""), reverse=True)[:30]:
            tag = f"[{info['priority']}] " if info.get("priority") else ""
            print(f"  {info['source']:25s} | {tag}{info['title'][:75]}")


# ─── Main ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Dragon Ball TCG Booster Box Pre-Order Monitor")
    parser.add_argument("--reset", action="store_true", help="Reset all databases")
    parser.add_argument("--dry-run", action="store_true", help="Check without sending alerts")
    parser.add_argument("--list", action="store_true", help="Show all tracked products & news")
    parser.add_argument("--priority", action="store_true", help="Show only priority watchlist matches")
    parser.add_argument("--priority-only", action="store_true", help="Fast mode: only deep-check priority URLs (skip slow shop searches)")
    args = parser.parse_args()

    if args.reset:
        for f in [SEEN_PRODUCTS_FILE, SEEN_NEWS_FILE, PRICE_HISTORY_FILE]:
            if f.exists():
                f.unlink()
        log.info("Databases gereset.")

    if args.list or args.priority:
        cmd_list(priority_only=args.priority)
        return

    new_products, status_changes, price_drops, new_news = cmd_run(dry_run=args.dry_run, priority_only=args.priority_only)
    priority_hits = sum(1 for p in new_products if p.get("priority"))
    preorders = sum(1 for p in new_products if p.get("stock_status") == "preorder")
    print(
        f"\n{len(new_products)} nieuwe producten "
        f"({priority_hits} PRIORITY hits, {preorders} pre-orders), "
        f"{len(status_changes)} status changes, {len(price_drops)} price drops, "
        f"{len(new_news)} nieuws."
    )


if __name__ == "__main__":
    main()
