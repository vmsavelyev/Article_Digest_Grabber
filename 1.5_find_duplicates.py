#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Поиск смысловых дубликатов статей.

Гибридный алгоритм:
  1. Локальные эмбеддинги LaBSE (Google, cross-lingual) — бесплатно, оффлайн.
     Находит кандидатов по косинусному сходству.
  2. DeepSeek Chat API — проверяет только кандидатные пары полным текстом.
  3. Транзитивные дубликаты группируются (A=B, B=C → оставляем A, удаляем B и C).
  4. Отчёт сохраняется в duplicates_report.txt.

Зависимости (один раз):
    pip install sentence-transformers

Модель LaBSE (~1.8 GB) скачивается автоматически при первом запуске.

Использование:
    python3 2.5_find_duplicates.py <DEEPSEEK_API_KEY>
    python3 2.5_find_duplicates.py <DEEPSEEK_API_KEY> --threshold 0.78
    python3 2.5_find_duplicates.py <DEEPSEEK_API_KEY> --action move
    python3 2.5_find_duplicates.py <DEEPSEEK_API_KEY> --concurrent 3
"""

import os
import re
import sys
import glob
from dotenv import load_dotenv

load_dotenv()
import json
import shutil
import asyncio
from datetime import datetime
from openai import AsyncOpenAI

try:
    from sentence_transformers import SentenceTransformer
    import torch
except ImportError:
    print("Ошибка: библиотека sentence-transformers не установлена.")
    print("Установите командой:  pip install sentence-transformers")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ARTICLES_DIR = os.path.join(SCRIPT_DIR, "articles_markdown")
DUPLICATES_DIR = os.path.join(SCRIPT_DIR, "articles_duplicates")
REPORT_FILE = os.path.join(SCRIPT_DIR, "duplicates_report.txt")

DEEPSEEK_BASE_URL = "https://api.deepseek.com"
CHAT_MODEL = "deepseek-v4-flash"

LABSE_MODEL = "sentence-transformers/LaBSE"

# Пары ниже этого порога — не кандидаты
DEFAULT_EMBED_THRESHOLD = 0.65
# Пары выше этого порога — дубликаты без LLM-проверки
DEFAULT_CONFIRM_THRESHOLD = 0.90
DEFAULT_MAX_CONCURRENT = 5

# Символов тела статьи для LLM-проверки
BODY_EXCERPT_CHARS = 1500


# ---------------------------------------------------------------------------
# Работа с текстом
# ---------------------------------------------------------------------------

def is_russian(text: str) -> bool:
    """Возвращает True если текст преимущественно на русском (>30% кириллицы)."""
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
    """Тело статьи после ---, без изображений и метаданных."""
    sep = re.search(r"^---\s*$", content, re.MULTILINE)
    body = content[sep.end():].strip() if sep else content
    body = re.sub(r"!\[.*?\]\(.*?\)", "", body)
    body = re.sub(r"\*\*.+?\*\*:.*\n?", "", body)
    body = re.sub(r"\n{2,}", "\n", body).strip()
    return body[:BODY_EXCERPT_CHARS]


def extract_text_for_embedding(content: str) -> str:
    """Заголовок + начало тела для эмбеддинга."""
    title = extract_title(content)
    body = extract_body_excerpt(content)
    return f"{title}\n\n{body}".strip()


# ---------------------------------------------------------------------------
# Union-Find для транзитивных дубликатов
# ---------------------------------------------------------------------------

class UnionFind:
    def __init__(self, n):
        self.parent = list(range(n))

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, x, y):
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if rx < ry:
            self.parent[ry] = rx
        else:
            self.parent[rx] = ry

    def groups(self, n):
        from collections import defaultdict
        g = defaultdict(list)
        for i in range(n):
            g[self.find(i)].append(i)
        return {k: v for k, v in g.items() if len(v) > 1}


# ---------------------------------------------------------------------------
# DeepSeek LLM-проверка
# ---------------------------------------------------------------------------

async def verify_pair(client, article_a: dict, article_b: dict,
                      sem: asyncio.Semaphore) -> dict:
    """Полнотекстовая проверка одной пары через DeepSeek Chat."""
    prompt = (
        "Ты эксперт по анализу новостных и технологических статей. "
        "Определи, описывают ли две статьи ОДНО И ТО ЖЕ событие или новость.\n\n"
        f"### Статья 1\n**Заголовок:** {article_a['title']}\n{article_a['body']}\n\n"
        f"### Статья 2\n**Заголовок:** {article_b['title']}\n{article_b['body']}\n\n"
        "Дубликатами считаются статьи об одном событии, даже если написаны разными словами, "
        "разными авторами или с разного угла подачи.\n"
        "НЕ считаются дубликатами статьи на одну тему, но о разных событиях.\n\n"
        "Ответь строго в формате JSON без markdown-обёртки:\n"
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
# Основная логика
# ---------------------------------------------------------------------------

async def run(api_key: str, embed_threshold: float, confirm_threshold: float,
              action: str, max_concurrent: int):

    client = AsyncOpenAI(api_key=api_key, base_url=DEEPSEEK_BASE_URL)
    sem = asyncio.Semaphore(max_concurrent)

    # — Чтение файлов --------------------------------------------------------
    md_files = sorted(glob.glob(os.path.join(ARTICLES_DIR, "*.md")))
    if not md_files:
        print(f"Нет .md файлов в каталоге: {ARTICLES_DIR}")
        return

    n = len(md_files)
    total_pairs = n * (n - 1) // 2
    print(f"Статей:                {n}")
    print(f"Всего пар:             {total_pairs}")
    print(f"Порог кандидатов:      >= {embed_threshold}")
    print(f"Порог автодубликата:   >= {confirm_threshold}")
    print(f"Параллельных запросов: {max_concurrent}")

    articles = []
    for fp in md_files:
        with open(fp, "r", encoding="utf-8") as f:
            content = f.read()
        title = extract_title(content)
        body = extract_body_excerpt(content)
        articles.append({
            "filepath": fp,
            "filename": os.path.basename(fp),
            "title": title,
            "body": body,
            "embed_text": extract_text_for_embedding(content),
            "is_russian": is_russian(body),
        })

    # — Шаг 1: Локальные эмбеддинги LaBSE -----------------------------------
    print("\n[1/3] Загрузка модели LaBSE...")

    # Выбираем устройство: MPS (Apple Silicon) > CUDA > CPU
    if torch.backends.mps.is_available():
        device = "mps"
        device_label = "Apple MPS (M-series GPU)"
    elif torch.cuda.is_available():
        device = "cuda"
        device_label = "CUDA GPU"
    else:
        device = "cpu"
        device_label = "CPU"
    print(f"  Устройство: {device_label}")

    model = SentenceTransformer(LABSE_MODEL, device=device)

    print(f"  Генерация эмбеддингов для {n} статей...")
    texts = [a["embed_text"] for a in articles]
    embeddings = model.encode(
        texts,
        batch_size=32,
        show_progress_bar=True,
        normalize_embeddings=True,   # нормализация → cosine = dot product
        convert_to_numpy=True,
    )

    # — Шаг 2: Матрица сходства ---------------------------------------------
    print("\n[2/3] Вычисление косинусного сходства...")

    # Косинусное сходство через матричное умножение (embeddings нормализованы)
    import numpy as np
    sim_matrix = embeddings @ embeddings.T

    auto_pairs = []      # similarity >= confirm_threshold
    candidate_pairs = [] # embed_threshold <= sim < confirm_threshold

    for i in range(n):
        for j in range(i + 1, n):
            sim = float(sim_matrix[i, j])
            if sim >= confirm_threshold:
                auto_pairs.append((i, j, sim))
            elif sim >= embed_threshold:
                candidate_pairs.append((i, j, sim))

    print(f"  Автодубликатов (>= {confirm_threshold}):         {len(auto_pairs)}")
    print(f"  Кандидатов для LLM-проверки:              {len(candidate_pairs)}")

    # — Шаг 3: LLM-проверка кандидатов -------------------------------------
    confirmed_pairs = []  # (i, j, similarity, reason)

    for i, j, sim in auto_pairs:
        confirmed_pairs.append((i, j, sim, f"LaBSE сходство {sim:.1%} (авто)"))

    if candidate_pairs:
        print(f"\n[3/3] LLM-проверка {len(candidate_pairs)} пар через DeepSeek...")
        tasks = [
            verify_pair(client, articles[i], articles[j], sem)
            for i, j, sim in candidate_pairs
        ]
        results = await asyncio.gather(*tasks)
        llm_confirmed = 0
        for (i, j, sim), verdict in zip(candidate_pairs, results):
            status = "✓" if verdict["is_duplicate"] else "✗"
            print(f"  [{status}] {articles[i]['filename']} ↔ {articles[j]['filename']}")
            if verdict["is_duplicate"]:
                confirmed_pairs.append((i, j, sim, verdict["reason"]))
                llm_confirmed += 1
        print(f"  LLM подтвердил дубликатов: {llm_confirmed}")
    else:
        print("\n[3/3] Кандидатов нет, LLM-проверка пропущена.")

    # — Группировка транзитивных дубликатов ----------------------------------
    uf = UnionFind(n)
    pair_info = {}
    for i, j, sim, reason in confirmed_pairs:
        uf.union(i, j)
        pair_info[(min(i, j), max(i, j))] = (sim, reason)

    groups = uf.groups(n)

    # Для каждой группы выбираем "хранителя":
    # приоритет — английская статья; среди равных — наименьший индекс.
    report_groups = []
    for members in groups.values():
        english = [m for m in members if not articles[m]["is_russian"]]
        keeper_idx = min(english) if english else min(members)
        keeper = articles[keeper_idx]

        remove_list = []
        for m in members:
            if m == keeper_idx:
                continue
            key = (min(keeper_idx, m), max(keeper_idx, m))
            sim, reason = pair_info.get(
                key,
                (float(sim_matrix[keeper_idx, m]), "транзитивный дубликат"),
            )
            remove_list.append({
                "filename": articles[m]["filename"],
                "title": articles[m]["title"],
                "filepath": articles[m]["filepath"],
                "is_russian": articles[m]["is_russian"],
                "similarity": sim,
                "reason": reason,
            })

        report_groups.append({
            "keep": {
                "filename": keeper["filename"],
                "title": keeper["title"],
                "is_russian": keeper["is_russian"],
            },
            "remove": remove_list,
        })

    # — Отчёт ---------------------------------------------------------------
    _save_report(report_groups, n, embed_threshold, confirm_threshold)

    if not report_groups:
        return

    # — Действие ------------------------------------------------------------
    if action == "report":
        print("\nРежим: только отчёт. Файлы не изменены.")
        print("Запустите с --action move чтобы переместить дубликаты в articles_duplicates/")
        return

    if action == "move":
        os.makedirs(DUPLICATES_DIR, exist_ok=True)
        moved = 0
        for group in report_groups:
            for item in group["remove"]:
                src = item["filepath"]
                if not os.path.exists(src):
                    continue
                dst = os.path.join(DUPLICATES_DIR, item["filename"])
                shutil.move(src, dst)
                print(f"  Перемещён: {item['filename']}")
                moved += 1
        print(f"\nПеремещено файлов: {moved} → {DUPLICATES_DIR}/")


# ---------------------------------------------------------------------------
# Отчёт
# ---------------------------------------------------------------------------

def _lang_tag(is_ru: bool) -> str:
    return "[RU]" if is_ru else "[EN]"


def _save_report(report_groups: list, total: int,
                 embed_threshold: float, confirm_threshold: float):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    total_to_remove = sum(len(g["remove"]) for g in report_groups)

    lines = [
        "=" * 70,
        "ОТЧЁТ О СМЫСЛОВЫХ ДУБЛИКАТАХ",
        f"Дата:              {timestamp}",
        f"Статей:            {total}",
        f"Порог (LaBSE):     {embed_threshold}  |  Авто-порог: {confirm_threshold}",
        "=" * 70,
    ]

    if not report_groups:
        lines.append("\nДубликатов не найдено.")
    else:
        lines.append(f"\nНайдено групп дубликатов: {len(report_groups)}")
        lines.append(f"Файлов к удалению:        {total_to_remove}\n")
        lines.append("-" * 70)

        for idx, group in enumerate(report_groups, 1):
            keep = group["keep"]
            remove_list = group["remove"]
            count = len(remove_list) + 1  # +1 — сам хранитель
            lang = _lang_tag(keep["is_russian"])

            lines.append(f"\n#{idx}  ({count} статьи об одном событии)")
            lines.append(f"  ОСТАВИТЬ:  {keep['filename']}  {lang}")
            lines.append(f"             {keep['title']}")

            for item in remove_list:
                item_lang = _lang_tag(item["is_russian"])
                lines.append(f"  УДАЛИТЬ:   {item['filename']}  {item_lang}"
                              f"  (сходство: {item['similarity']:.1%})")
                lines.append(f"             {item['title']}")
                if item["reason"]:
                    lines.append(f"             Причина: {item['reason']}")

        lines += [
            "\n" + "-" * 70,
            "\nФАЙЛЫ ДЛЯ УДАЛЕНИЯ:",
        ]
        for group in report_groups:
            for item in group["remove"]:
                lines.append(f"  - {item['filename']}")

    report_text = "\n".join(lines)
    print("\n" + report_text)

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(report_text + "\n")
    print(f"\nОтчёт сохранён: {REPORT_FILE}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    args = sys.argv[1:]
    if args and args[0] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0)

    api_key = os.environ.get("DEEPSEEK_API_KEY", "")
    if args and not args[0].startswith("--"):
        api_key = args[0]
        args = args[1:]
    embed_threshold = DEFAULT_EMBED_THRESHOLD
    confirm_threshold = DEFAULT_CONFIRM_THRESHOLD
    action = "report"
    max_concurrent = DEFAULT_MAX_CONCURRENT

    i = 0
    while i < len(args):
        if args[i] == "--threshold" and i + 1 < len(args):
            try:
                embed_threshold = float(args[i + 1])
                assert 0 < embed_threshold < 1
            except (ValueError, AssertionError):
                print("Ошибка: --threshold должен быть числом от 0 до 1, например 0.75")
                sys.exit(1)
            i += 2
        elif args[i] == "--confirm-threshold" and i + 1 < len(args):
            try:
                confirm_threshold = float(args[i + 1])
                assert 0 < confirm_threshold <= 1
            except (ValueError, AssertionError):
                print("Ошибка: --confirm-threshold должен быть числом от 0 до 1")
                sys.exit(1)
            i += 2
        elif args[i] == "--action" and i + 1 < len(args):
            action = args[i + 1]
            if action not in ("report", "move"):
                print("Ошибка: --action должен быть report или move")
                sys.exit(1)
            i += 2
        elif args[i] == "--concurrent" and i + 1 < len(args):
            try:
                max_concurrent = int(args[i + 1])
                assert max_concurrent > 0
            except (ValueError, AssertionError):
                print("Ошибка: --concurrent должен быть целым числом > 0")
                sys.exit(1)
            i += 2
        else:
            print(f"Неизвестный аргумент: {args[i]}\nИспользуйте --help для справки.")
            sys.exit(1)

    return api_key, embed_threshold, confirm_threshold, action, max_concurrent


def main():
    api_key, embed_threshold, confirm_threshold, action, max_concurrent = parse_args()

    if not os.path.isdir(ARTICLES_DIR):
        print(f"Ошибка: каталог не найден: {ARTICLES_DIR}")
        sys.exit(1)

    print("=" * 70)
    print("ПОИСК СМЫСЛОВЫХ ДУБЛИКАТОВ  (LaBSE + DeepSeek)")
    print("=" * 70)

    asyncio.run(run(api_key, embed_threshold, confirm_threshold, action, max_concurrent))


if __name__ == "__main__":
    main()
