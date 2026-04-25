#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Поиск смысловых дубликатов: локальные статьи (articles_markdown/) vs база данных Notion.

Алгоритм:
  1. Интерактивно спрашивает базу Notion и период.
  2. Загружает статьи из Notion (заголовок + текстовые свойства + первые блоки страницы).
  3. Генерирует LaBSE-эмбеддинги для локальных и Notion-статей.
  4. Находит пары с косинусным сходством >= порога кандидатов.
  5. Опционально проверяет кандидатов через DeepSeek Chat API.
  6. Выводит отчёт: какие локальные статьи уже есть в Notion.

Зависимости (уже в requirements.txt):
    pip install notion-client sentence-transformers openai

Использование:
    python3 1.6_find_notion_duplicates.py

Переменные окружения (опционально, чтобы не вводить каждый раз):
    NOTION_API_KEY=secret_xxx
    DEEPSEEK_API_KEY=sk-xxx
"""

import os
import re
import sys
import glob
import json
import asyncio
from dotenv import load_dotenv

load_dotenv()
from datetime import datetime, timedelta, timezone

# --- notion-client ---
try:
    from notion_client import Client as NotionClient
except ImportError:
    print("Ошибка: библиотека notion-client не установлена.")
    print("Установите командой:  pip install notion-client")
    sys.exit(1)

# --- sentence-transformers ---
try:
    from sentence_transformers import SentenceTransformer
    import torch
    import numpy as np
except ImportError:
    print("Ошибка: библиотека sentence-transformers не установлена.")
    print("Установите командой:  pip install sentence-transformers")
    sys.exit(1)

# --- openai (для DeepSeek, опционально) ---
try:
    from openai import AsyncOpenAI
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False


# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------

SCRIPT_DIR     = os.path.dirname(os.path.abspath(__file__))
ARTICLES_DIR   = os.path.join(SCRIPT_DIR, "articles_markdown")
REPORT_FILE    = os.path.join(SCRIPT_DIR, "notion_duplicates_report.txt")

DEEPSEEK_BASE_URL   = "https://api.deepseek.com"
CHAT_MODEL          = "deepseek-v4-flash"
LABSE_MODEL         = "sentence-transformers/LaBSE"

DEFAULT_EMBED_THRESHOLD   = 0.65   # порог кандидата
DEFAULT_CONFIRM_THRESHOLD = 0.90   # авто-дубликат без LLM
DEFAULT_MAX_CONCURRENT    = 5
BODY_EXCERPT_CHARS        = 1500
MAX_BLOCKS_PER_PAGE       = 15     # блоков на страницу Notion


# ---------------------------------------------------------------------------
# Работа с текстом локальных файлов
# ---------------------------------------------------------------------------

def is_russian(text: str) -> bool:
    if not text:
        return False
    letters = [c for c in text if c.isalpha()]
    if not letters:
        return False
    cyrillic = sum(1 for c in letters if '\u0400' <= c <= '\u04FF')
    return cyrillic / len(letters) > 0.3


def extract_title(content: str) -> str:
    if content.startswith("# "):
        end = content.index("\n") if "\n" in content else len(content)
        return content[2:end].strip()
    return ""


def extract_body_excerpt(content: str) -> str:
    sep = re.search(r"^---\s*$", content, re.MULTILINE)
    body = content[sep.end():].strip() if sep else content
    body = re.sub(r"!\[.*?\]\(.*?\)", "", body)
    body = re.sub(r"\*\*.+?\*\*:.*\n?", "", body)
    body = re.sub(r"\n{2,}", "\n", body).strip()
    return body[:BODY_EXCERPT_CHARS]


def extract_text_for_embedding(content: str) -> str:
    title = extract_title(content)
    body  = extract_body_excerpt(content)
    return f"{title}\n\n{body}".strip()


# ---------------------------------------------------------------------------
# Утилиты Notion
# ---------------------------------------------------------------------------

def rich_text_to_str(rt_list: list) -> str:
    return "".join(item.get("plain_text", "") for item in rt_list)


def get_page_title(page: dict) -> str:
    for prop in page.get("properties", {}).values():
        if prop.get("type") == "title":
            return rich_text_to_str(prop.get("title", []))
    return "(без заголовка)"


def get_page_text_props(page: dict) -> str:
    """Извлекает все rich_text-свойства страницы в одну строку."""
    parts = []
    for prop in page.get("properties", {}).values():
        if prop.get("type") == "rich_text":
            text = rich_text_to_str(prop.get("rich_text", []))
            if text:
                parts.append(text)
    return " ".join(parts)


def blocks_to_text(blocks: list) -> str:
    lines = []
    for block in blocks:
        btype = block.get("type", "")
        data  = block.get(btype, {})
        rt    = data.get("rich_text", [])
        if rt:
            lines.append(rich_text_to_str(rt))
    return "\n".join(lines)


def get_page_url(page: dict) -> str:
    # Сначала ищем свойство типа url
    for prop in page.get("properties", {}).values():
        if prop.get("type") == "url" and prop.get("url"):
            return prop["url"]
    # Иначе — permalink самой страницы в Notion
    return page.get("url", "")


def parse_created_date(page: dict) -> str:
    return page.get("created_time", "")[:10]


def notion_embed_text(title: str, props_text: str, blocks_text: str) -> str:
    parts = [p for p in [title, props_text, blocks_text] if p]
    combined = "\n\n".join(parts)
    return combined[:BODY_EXCERPT_CHARS * 2]


def extract_db_id(raw: str) -> str:
    """Извлекает ID базы данных из URL Notion или из сырой строки."""
    m = re.search(
        r"([0-9a-f]{32}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
        raw,
    )
    if m:
        h = m.group(1).replace("-", "")
        return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"
    return raw.strip()


# ---------------------------------------------------------------------------
# Загрузка данных из Notion
# ---------------------------------------------------------------------------

def list_notion_databases(notion: NotionClient) -> list:
    results, cursor = [], None
    while True:
        kwargs = {"filter": {"property": "object", "value": "database"}}
        if cursor:
            kwargs["start_cursor"] = cursor
        resp = notion.search(**kwargs)
        results.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return results


def get_data_source_id(notion: NotionClient, db_id: str) -> str:
    """В notion-client 2.x databases.query заменён на data_sources.query.
    Получаем data_source_id из объекта базы данных."""
    db = notion.databases.retrieve(db_id)
    data_sources = db.get("data_sources", []) if isinstance(db, dict) else getattr(db, "data_sources", [])
    if not data_sources:
        raise ValueError(
            "У базы данных нет data_sources. "
            "Убедитесь, что интеграция имеет доступ к этой базе."
        )
    ds = data_sources[0]
    return ds.get("id") if isinstance(ds, dict) else ds.id


def fetch_notion_pages(notion: NotionClient, db_id: str,
                       start_dt: datetime, end_dt: datetime) -> list:
    ds_id = get_data_source_id(notion, db_id)

    pages, cursor = [], None
    date_filter = {
        "and": [
            {"timestamp": "created_time", "created_time": {"after":  start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")}},
            {"timestamp": "created_time", "created_time": {"before": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")}},
        ]
    }
    while True:
        kwargs = {"filter": date_filter, "page_size": 100}
        if cursor:
            kwargs["start_cursor"] = cursor
        resp = notion.data_sources.query(ds_id, **kwargs)
        results = resp.get("results", []) if isinstance(resp, dict) else list(resp.results)
        pages.extend(results)
        has_more = resp.get("has_more", False) if isinstance(resp, dict) else bool(resp.has_more)
        if not has_more:
            break
        cursor = resp.get("next_cursor") if isinstance(resp, dict) else resp.next_cursor
    return pages


def fetch_blocks_text(notion: NotionClient, page_id: str) -> str:
    try:
        resp   = notion.blocks.children.list(block_id=page_id, page_size=MAX_BLOCKS_PER_PAGE)
        blocks = resp.get("results", [])
        return blocks_to_text(blocks)
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# DeepSeek LLM-верификация
# ---------------------------------------------------------------------------

async def verify_pair(client, article_a: dict, article_b: dict,
                      sem: asyncio.Semaphore) -> dict:
    prompt = (
        "Ты эксперт по анализу новостных и технологических статей. "
        "Определи, описывают ли две статьи ОДНО И ТО ЖЕ событие или новость.\n\n"
        f"### Статья 1\n**Заголовок:** {article_a['title']}\n{article_a['body']}\n\n"
        f"### Статья 2\n**Заголовок:** {article_b['title']}\n{article_b['body']}\n\n"
        "Дубликатами считаются статьи об одном событии, даже если написаны разными словами, "
        "разными авторами или с разного угла.\n"
        "НЕ считаются дубликатами статьи на одну тему, но о разных событиях.\n\n"
        "Ответь строго в формате JSON без markdown:\n"
        '{"is_duplicate": true/false, "reason": "краткое объяснение на русском"}'
    )
    async with sem:
        try:
            resp = await client.chat.completions.create(
                model=CHAT_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=150,
            )
            raw = resp.choices[0].message.content.strip()
            m = re.search(r"\{.*?\}", raw, re.DOTALL)
            if m:
                data = json.loads(m.group())
                return {
                    "is_duplicate": bool(data.get("is_duplicate", False)),
                    "reason": data.get("reason", ""),
                }
        except Exception as e:
            print(f"\n  [!] Ошибка LLM: {e}")
    return {"is_duplicate": False, "reason": "Ошибка API"}


# ---------------------------------------------------------------------------
# Интерактивная настройка
# ---------------------------------------------------------------------------

def ask(text: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    value  = input(f"{text}{suffix}: ").strip()
    return value if value else default


def setup_notion() -> NotionClient:
    token = os.environ.get("NOTION_API_KEY", "")
    if not token:
        token = ask("Notion Integration Token (или задайте NOTION_API_KEY в окружении)")
    if not token:
        print("Ошибка: Notion token не задан.")
        sys.exit(1)
    return NotionClient(auth=token)


def choose_database(notion: NotionClient) -> tuple:
    """Возвращает (db_id, db_title)."""
    print("\nЗагрузка доступных баз данных...")
    try:
        databases = list_notion_databases(notion)
    except Exception as e:
        print(f"Не удалось получить список баз: {e}")
        databases = []

    if databases:
        print(f"\nДоступные базы данных ({len(databases)}):")
        for i, db in enumerate(databases, 1):
            title = rich_text_to_str(db.get("title", []))
            db_id = db.get("id", "")
            print(f"  {i:2}. {title or '(без названия)'}  [{db_id}]")
        print(f"\n   0. Ввести ID или URL вручную")

        choice = ask("Выберите базу данных (номер или 0)", "0")
        try:
            idx = int(choice)
            if 1 <= idx <= len(databases):
                db   = databases[idx - 1]
                title = rich_text_to_str(db.get("title", []))
                return db["id"], title or "(без названия)"
        except ValueError:
            pass

    raw   = ask("Введите ID или URL базы данных Notion")
    db_id = extract_db_id(raw)
    return db_id, "указанная база"


def choose_period() -> tuple:
    """Возвращает (start_dt, end_dt) в UTC."""
    now = datetime.now(timezone.utc)
    print("\nПериод для статей из Notion:")
    print("  1. Последние 7 дней")
    print("  2. Последние 30 дней")
    print("  3. Последние 90 дней")
    print("  4. Последние 365 дней")
    print("  5. Весь период")
    print("  6. Указать даты вручную")

    choice = ask("Ваш выбор", "2")

    if choice == "1":
        return now - timedelta(days=7), now
    if choice == "3":
        return now - timedelta(days=90), now
    if choice == "4":
        return now - timedelta(days=365), now
    if choice == "5":
        return datetime(2020, 1, 1, tzinfo=timezone.utc), now
    if choice == "6":
        start_str = ask("Начальная дата (YYYY-MM-DD)")
        end_str   = ask("Конечная дата (YYYY-MM-DD)", now.strftime("%Y-%m-%d"))
        try:
            start = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            end   = datetime.strptime(end_str,   "%Y-%m-%d").replace(tzinfo=timezone.utc)
            return start, end
        except ValueError:
            print("Неверный формат — используется последние 30 дней.")
    return now - timedelta(days=30), now


def setup_deepseek() -> tuple:
    """Возвращает (api_key, max_concurrent)."""
    if not HAS_OPENAI:
        print("\nБиблиотека openai не установлена — LLM-верификация недоступна.")
        return "", DEFAULT_MAX_CONCURRENT

    api_key = os.environ.get("DEEPSEEK_API_KEY", "")
    if not api_key:
        raw = ask(
            "\nDeepSeek API ключ (Enter для пропуска — только LaBSE без LLM-проверки)",
            ""
        )
        api_key = raw.strip()

    if not api_key:
        print("  DeepSeek не задан — используются только LaBSE-эмбеддинги.")
        return "", DEFAULT_MAX_CONCURRENT

    raw = ask("Параллельных запросов к DeepSeek", str(DEFAULT_MAX_CONCURRENT))
    try:
        concurrent = max(1, int(raw))
    except ValueError:
        concurrent = DEFAULT_MAX_CONCURRENT
    return api_key, concurrent


# ---------------------------------------------------------------------------
# Основная логика
# ---------------------------------------------------------------------------

async def run(notion: NotionClient, db_id: str, db_title: str,
              start_dt: datetime, end_dt: datetime,
              deepseek_key: str, max_concurrent: int,
              embed_threshold: float, confirm_threshold: float,
              fetch_blocks: bool):

    # --- Локальные статьи ---
    md_files = sorted(glob.glob(os.path.join(ARTICLES_DIR, "*.md")))
    if not md_files:
        print(f"Нет .md файлов в каталоге: {ARTICLES_DIR}")
        return

    local_articles = []
    for fp in md_files:
        with open(fp, "r", encoding="utf-8") as f:
            content = f.read()
        title = extract_title(content)
        body  = extract_body_excerpt(content)
        local_articles.append({
            "filepath":   fp,
            "filename":   os.path.basename(fp),
            "title":      title,
            "body":       body,
            "embed_text": extract_text_for_embedding(content),
            "is_russian": is_russian(body),
        })

    print(f"\nЛокальных статей: {len(local_articles)}")

    # --- Статьи из Notion ---
    print(f"\nЗагрузка страниц из Notion «{db_title}»...")
    print(f"  Период: {start_dt.strftime('%Y-%m-%d')} — {end_dt.strftime('%Y-%m-%d')}")

    try:
        pages = fetch_notion_pages(notion, db_id, start_dt, end_dt)
    except Exception as e:
        print(f"Ошибка при запросе Notion: {e}")
        return

    print(f"  Найдено страниц: {len(pages)}")

    if not pages:
        print("Нет статей в Notion за указанный период. Попробуйте расширить диапазон дат.")
        return

    notion_articles = []
    for i, page in enumerate(pages, 1):
        page_id    = page.get("id", "")
        title      = get_page_title(page)
        props_text = get_page_text_props(page)
        blocks_text = fetch_blocks_text(notion, page_id) if fetch_blocks else ""
        body        = (props_text + "\n" + blocks_text).strip()[:BODY_EXCERPT_CHARS]

        notion_articles.append({
            "page_id":    page_id,
            "title":      title,
            "body":       body,
            "embed_text": notion_embed_text(title, props_text, blocks_text),
            "url":        get_page_url(page),
            "created":    parse_created_date(page),
            "is_russian": is_russian(body or title),
        })

        if i % 20 == 0 or i == len(pages):
            print(f"  Обработано: {i}/{len(pages)}", end="\r")

    print(f"\n  Notion-статей подготовлено: {len(notion_articles)}")

    # --- Шаг 1: LaBSE-эмбеддинги ---
    print("\n[1/3] Загрузка модели LaBSE...")

    if torch.backends.mps.is_available():
        device, device_label = "mps", "Apple MPS (M-series GPU)"
    elif torch.cuda.is_available():
        device, device_label = "cuda", "CUDA GPU"
    else:
        device, device_label = "cpu", "CPU"
    print(f"  Устройство: {device_label}")

    model = SentenceTransformer(LABSE_MODEL, device=device)

    n_local  = len(local_articles)
    n_notion = len(notion_articles)
    all_texts = (
        [a["embed_text"] for a in local_articles] +
        [a["embed_text"] for a in notion_articles]
    )

    print(f"  Генерация эмбеддингов ({n_local + n_notion} текстов)...")
    embeddings = model.encode(
        all_texts,
        batch_size=32,
        show_progress_bar=True,
        normalize_embeddings=True,
        convert_to_numpy=True,
    )

    local_emb  = embeddings[:n_local]
    notion_emb = embeddings[n_local:]

    # --- Шаг 2: Матрица сходства (локальные × Notion) ---
    print("\n[2/3] Вычисление косинусного сходства (локальные vs Notion)...")
    sim_matrix = local_emb @ notion_emb.T   # shape: (n_local, n_notion)

    auto_pairs      = []  # (i, j, sim)       — выше confirm_threshold
    candidate_pairs = []  # (i, j, sim)       — между порогами

    for i in range(n_local):
        for j in range(n_notion):
            sim = float(sim_matrix[i, j])
            if sim >= confirm_threshold:
                auto_pairs.append((i, j, sim))
            elif sim >= embed_threshold:
                candidate_pairs.append((i, j, sim))

    print(f"  Автодубликатов  (>= {confirm_threshold}): {len(auto_pairs)}")
    print(f"  Кандидатов для LLM:              {len(candidate_pairs)}")

    # --- Шаг 3: LLM-верификация ---
    confirmed_pairs = []  # (i, j, sim, reason)

    for i, j, sim in auto_pairs:
        confirmed_pairs.append((i, j, sim, f"LaBSE сходство {sim:.1%} (авто)"))

    if candidate_pairs and deepseek_key:
        print(f"\n[3/3] LLM-проверка {len(candidate_pairs)} пар через DeepSeek...")
        client = AsyncOpenAI(api_key=deepseek_key, base_url=DEEPSEEK_BASE_URL)
        sem    = asyncio.Semaphore(max_concurrent)

        tasks   = [verify_pair(client, local_articles[i], notion_articles[j], sem)
                   for i, j, _ in candidate_pairs]
        results = await asyncio.gather(*tasks)

        llm_confirmed = 0
        for (i, j, sim), verdict in zip(candidate_pairs, results):
            status = "✓" if verdict["is_duplicate"] else "✗"
            notion_title = notion_articles[j]["title"][:50]
            print(f"  [{status}] {local_articles[i]['filename']}  ↔  Notion: {notion_title}")
            if verdict["is_duplicate"]:
                confirmed_pairs.append((i, j, sim, verdict["reason"]))
                llm_confirmed += 1

        print(f"  LLM подтвердил дубликатов: {llm_confirmed}")

    elif candidate_pairs and not deepseek_key:
        print(f"\n[3/3] DeepSeek не задан — {len(candidate_pairs)} кандидатов включены как "
              f"«требует проверки» (сходство {embed_threshold:.0%}–{confirm_threshold:.0%}).")
        for i, j, sim in candidate_pairs:
            confirmed_pairs.append((i, j, sim, f"LaBSE {sim:.1%} — рекомендуется ручная проверка"))
    else:
        print("\n[3/3] Кандидатов нет — LLM-проверка пропущена.")

    # --- Группировка результатов ---
    # local_idx → [(notion_idx, sim, reason), ...]
    local_matches: dict = {}
    for i, j, sim, reason in confirmed_pairs:
        local_matches.setdefault(i, []).append((j, sim, reason))

    _save_report(
        local_articles, notion_articles, local_matches,
        db_title, start_dt, end_dt,
        embed_threshold, confirm_threshold,
    )


# ---------------------------------------------------------------------------
# Отчёт
# ---------------------------------------------------------------------------

def _lang(is_ru: bool) -> str:
    return "[RU]" if is_ru else "[EN]"


def _save_report(local_articles, notion_articles, local_matches,
                 db_title, start_dt, end_dt,
                 embed_threshold, confirm_threshold):

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines = [
        "=" * 70,
        "ОТЧЁТ: ДУБЛИКАТЫ ЛОКАЛЬНЫХ СТАТЕЙ В NOTION",
        f"Дата анализа:      {timestamp}",
        f"База Notion:       {db_title}",
        f"Период Notion:     {start_dt.strftime('%Y-%m-%d')} — {end_dt.strftime('%Y-%m-%d')}",
        f"Локальных статей:  {len(local_articles)}",
        f"Notion-статей:     {len(notion_articles)}",
        f"Порог кандидата:   {embed_threshold}  |  Авто-порог: {confirm_threshold}",
        "=" * 70,
    ]

    if not local_matches:
        lines.append("\nДубликатов не найдено. Все локальные статьи уникальны относительно Notion.")
    else:
        lines.append(f"\nЛокальных статей с совпадениями в Notion: {len(local_matches)}\n")
        lines.append("-" * 70)

        # Сортируем по максимальному сходству (убывание)
        sorted_matches = sorted(
            local_matches.items(),
            key=lambda kv: max(m[1] for m in kv[1]),
            reverse=True,
        )

        for rank, (local_idx, matches) in enumerate(sorted_matches, 1):
            local = local_articles[local_idx]
            lines.append(f"\n#{rank}  ЛОКАЛЬНАЯ СТАТЬЯ {_lang(local['is_russian'])}:")
            lines.append(f"     {local['filename']}")
            if local["title"]:
                lines.append(f"     {local['title']}")

            lines.append(f"\n     Совпадения в Notion ({len(matches)}):")
            for notion_idx, sim, reason in sorted(matches, key=lambda x: x[1], reverse=True):
                na = notion_articles[notion_idx]
                lines.append(f"       — {_lang(na['is_russian'])} {na['title']}")
                lines.append(f"         Создана: {na['created']}  |  Сходство: {sim:.1%}")
                if na["url"]:
                    lines.append(f"         URL: {na['url']}")
                if reason:
                    lines.append(f"         Причина: {reason}")

        lines += [
            "\n" + "-" * 70,
            "\nФАЙЛЫ-ДУБЛИКАТЫ (рекомендуется не включать в дайджест):",
        ]
        for local_idx, _ in sorted_matches:
            lines.append(f"  - {local_articles[local_idx]['filename']}")

    report_text = "\n".join(lines)
    print("\n" + report_text)

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(report_text + "\n")
    print(f"\nОтчёт сохранён: {REPORT_FILE}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("ПОИСК ДУБЛИКАТОВ: ЛОКАЛЬНЫЕ СТАТЬИ vs NOTION  (LaBSE + DeepSeek)")
    print("=" * 70)

    if not os.path.isdir(ARTICLES_DIR):
        print(f"Ошибка: каталог не найден: {ARTICLES_DIR}")
        sys.exit(1)

    # Настройка
    notion             = setup_notion()
    db_id, db_title    = choose_database(notion)
    start_dt, end_dt   = choose_period()
    deepseek_key, concurrent = setup_deepseek()

    # Пороги
    raw = ask(f"\nПорог кандидата  (LaBSE, 0–1)", str(DEFAULT_EMBED_THRESHOLD))
    try:
        embed_threshold = float(raw)
    except ValueError:
        embed_threshold = DEFAULT_EMBED_THRESHOLD

    raw = ask(f"Порог авто-дубликата (LaBSE, 0–1)", str(DEFAULT_CONFIRM_THRESHOLD))
    try:
        confirm_threshold = float(raw)
    except ValueError:
        confirm_threshold = DEFAULT_CONFIRM_THRESHOLD

    # Загружать ли блоки страниц Notion (замедляет, но точнее)
    raw          = ask("\nЗагружать содержимое страниц Notion (медленнее, точнее)? [y/N]", "n")
    fetch_blocks = raw.lower() in ("y", "yes", "да")

    # Итоговая конфигурация
    print("\n" + "=" * 70)
    print(f"База Notion:       {db_title}")
    print(f"Период:            {start_dt.strftime('%Y-%m-%d')} — {end_dt.strftime('%Y-%m-%d')}")
    print(f"Порог кандидата:   {embed_threshold}")
    print(f"Авто-порог:        {confirm_threshold}")
    print(f"LLM:               {'DeepSeek (' + str(concurrent) + ' потоков)' if deepseek_key else 'отключён'}")
    print(f"Блоки страниц:     {'да' if fetch_blocks else 'нет'}")
    print("=" * 70)

    asyncio.run(run(
        notion, db_id, db_title,
        start_dt, end_dt,
        deepseek_key, concurrent,
        embed_threshold, confirm_threshold,
        fetch_blocks,
    ))


if __name__ == "__main__":
    main()
