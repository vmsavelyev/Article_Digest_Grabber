#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для сбора тегов из Notion Database и автозаполнения тегов в статьях markdown.

Часть 1 — Сбор тегов:
  Подключается к указанной Notion Database, показывает её поля,
  позволяет выбрать Multi-Select property и сохраняет уникальные теги в tags.txt.

Часть 2 — Автозаполнение:
  Читает теги из tags.txt, ищет их упоминания в заголовках и телах .md файлов
  из articles_markdown/, и добавляет блок **Компания:** с найденными тегами.

Использование:
  export NOTION_TOKEN='secret_xxx'
  python3 collect_tags.py

  python3 collect_tags.py secret_xxx

  python3 collect_tags.py --help
"""
from __future__ import annotations

import os
import sys
import re
from pathlib import Path
from notion_client import Client

# Импортируем утилиты из существующего скрипта
from import_to_notion import extract_database_id, get_user_confirmation, NotionImporter

TAGS_FILE = "tags.txt"
ARTICLES_DIR = "articles_markdown"


# ---------------------------------------------------------------------------
# Утилиты CLI
# ---------------------------------------------------------------------------

def print_help():
    print("Использование: python3 collect_tags.py [NOTION_TOKEN]")
    print()
    print("Скрипт выполняет две задачи:")
    print("  1. Сбор уникальных тегов из Notion Database (Multi-Select property)")
    print("  2. Автозаполнение тегов в markdown файлах из articles_markdown/")
    print()
    print("Переменные окружения:")
    print("  NOTION_TOKEN    API токен Notion (secret_xxx)")
    print()
    print("Примеры:")
    print("  export NOTION_TOKEN='secret_xxx'")
    print("  python3 collect_tags.py")
    print()
    print("  python3 collect_tags.py secret_xxx")


def get_notion_token() -> str:
    """Получает Notion token из окружения или первого аргумента CLI."""
    # Проверяем --help
    if "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        sys.exit(0)

    token = os.getenv("NOTION_TOKEN")
    if not token:
        for arg in sys.argv[1:]:
            if not arg.startswith("--"):
                token = arg
                break

    if not token:
        print("Ошибка: NOTION_TOKEN не указан")
        print()
        print("Укажите токен через переменную окружения или аргумент:")
        print("  export NOTION_TOKEN='secret_xxx'  &&  python3 collect_tags.py")
        print("  python3 collect_tags.py secret_xxx")
        print()
        print("Для справки: python3 collect_tags.py --help")
        sys.exit(1)

    return token


# ---------------------------------------------------------------------------
# Часть 1 — Сбор тегов из Notion
# ---------------------------------------------------------------------------

def get_database_id_from_user() -> str:
    """Запрашивает URL или ID Database у пользователя и извлекает Database ID."""
    print("\nВведите URL или ID Notion Database для сбора тегов:")
    print("  Пример URL: https://www.notion.so/workspace/DatabaseName-abc123def456...")
    print("  Пример ID:  abc123def456ghi789jkl012mno345pq")
    print()
    user_input = input("URL / ID Database: ").strip()

    if not user_input:
        print("Ошибка: ввод не может быть пустым")
        sys.exit(1)

    database_id = extract_database_id(user_input)
    if not database_id or len(database_id.replace("-", "")) != 32:
        print(f"Ошибка: не удалось извлечь Database ID из: {user_input}")
        sys.exit(1)

    return database_id


def select_multi_select_property(structure: dict) -> str:
    """
    Выводит все поля Database и позволяет пользователю выбрать
    поле типа multi_select для сбора тегов.

    Returns:
        Название выбранного property.
    """
    properties = structure["properties"]

    if not properties:
        print("⚠ В Database не найдено полей")
        sys.exit(1)

    # Фильтруем multi_select
    multi_select_props = [
        (name, info) for name, info in properties.items()
        if info["type"] == "multi_select"
    ]

    # Выводим полную структуру Database
    print("\n" + "=" * 80)
    print(f"Структура Database: {structure['title']}")
    print("=" * 80)
    print(f"Database ID: {structure['database_id']}")
    print()
    print("Поля (Properties):")
    print("-" * 80)
    for i, (name, info) in enumerate(properties.items(), 1):
        marker = " ✓" if info["type"] == "multi_select" else "  "
        print(f"  {marker} {i:2d}. {name}  (тип: {info['type']})")
    print("-" * 80)

    if not multi_select_props:
        print()
        print("⚠ В Database не найдено полей типа multi_select.")
        print("  Теги хранятся именно в полях типа Multi-Select.")
        sys.exit(1)

    # Один multi_select — берём автоматически
    if len(multi_select_props) == 1:
        prop_name = multi_select_props[0][0]
        print(f"\n  Единственное поле Multi-Select: \"{prop_name}\" — используем его.")
        return prop_name

    # Несколько — просим выбрать
    print(f"\n  Найдено {len(multi_select_props)} полей типа Multi-Select:")
    for i, (name, _) in enumerate(multi_select_props, 1):
        print(f"    {i}. {name}")
    print()

    while True:
        choice = input("  Введите номер поля: ").strip()
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(multi_select_props):
                return multi_select_props[idx][0]
            print(f"  Введите число от 1 до {len(multi_select_props)}")
        except ValueError:
            print("  Введите число")


def collect_tags_from_database(client: Client, data_source_id: str, property_name: str) -> set:
    """
    Выгружает все уникальные теги из указанного Multi-Select property.
    Обрабатывает пагинацию Notion API (max 100 страниц за запрос).

    Args:
        client:         Notion Client.
        data_source_id: ID data_source (из ответа databases.retrieve).
        property_name:  Название Multi-Select property.

    Returns:
        Множество уникальных названий тегов.
    """
    unique_tags = set()
    has_more = True
    start_cursor = None
    page_count = 0
    spinner_chars = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
    spinner_idx = 0

    print(f"\n  Сбор тегов из поля \"{property_name}\"...")

    while has_more:
        # В этой версии SDK (notion_version 2025-09-03) запрос к Database
        # идёт через data_sources.query(), а не databases.query().
        query_kwargs: dict = {"page_size": 100}
        if start_cursor:
            query_kwargs["start_cursor"] = start_cursor

        result = client.data_sources.query(data_source_id, **query_kwargs)

        for page in result.get("results", []):
            page_count += 1
            prop = page.get("properties", {}).get(property_name, {})
            if prop.get("type") == "multi_select":
                for tag in prop.get("multi_select", []):
                    tag_name = tag.get("name")
                    if tag_name:
                        unique_tags.add(tag_name)

        has_more = result.get("has_more", False)
        start_cursor = result.get("next_cursor")

        # Обновляем строку прогресса на месте
        spinner_idx = (spinner_idx + 1) % len(spinner_chars)
        sys.stdout.write(
            f"\r  {spinner_chars[spinner_idx]}  страниц: {page_count}  |  тегов: {len(unique_tags)}"
        )
        sys.stdout.flush()

    # Стираем spinner-строку и выводим итог
    sys.stdout.write("\r" + " " * 60 + "\r")
    print(f"  Просмотрено страниц: {page_count}")
    print(f"  Уникальных тегов:    {len(unique_tags)}")
    return unique_tags


def save_tags(tags: set) -> list:
    """
    Сохраняет теги в TAGS_FILE — один тег на строку, отсортировано.

    Returns:
        Отсортированный список тегов.
    """
    sorted_tags = sorted(tags)
    with open(TAGS_FILE, "w", encoding="utf-8") as f:
        for tag in sorted_tags:
            f.write(tag + "\n")
    print(f"\n  ✓ Теги сохранены в \"{TAGS_FILE}\" ({len(sorted_tags)} шт.)")
    return sorted_tags


def load_tags() -> list | None:
    """Загружает теги из TAGS_FILE. Возвращает None если файл не существует или пуст."""
    if not os.path.exists(TAGS_FILE):
        return None
    with open(TAGS_FILE, "r", encoding="utf-8") as f:
        tags = [line.strip() for line in f if line.strip()]
    return tags if tags else None


# ---------------------------------------------------------------------------
# Часть 2 — Анализ тегов и автозаполнение
# ---------------------------------------------------------------------------

def detect_trailing_chars(tags: list) -> dict:
    """
    Анализирует теги: для каждого тега с не-буквенно-цифровым последним символом
    группирует их по этому символу.

    Returns:
        {символ: [список тегов с таким окончанием]}
    """
    trailing: dict[str, list[str]] = {}
    for tag in tags:
        if not tag:
            continue
        last_char = tag[-1]
        if not last_char.isalnum():
            trailing.setdefault(last_char, []).append(tag)
    return trailing


def build_tag_search_map(tags: list, normalize: bool) -> dict:
    """
    Строит маппинг {оригинальный тег → строка для поиска}.
    При normalize=True убирает последний символ, если он не буква/цифра.
    """
    tag_map: dict[str, str] = {}
    for tag in tags:
        if normalize and tag and not tag[-1].isalnum():
            tag_map[tag] = tag[:-1]
        else:
            tag_map[tag] = tag
    return tag_map


def read_md_file(filepath: str) -> dict:
    """
    Читает .md файл и возвращает:
        title        — текст заголовка (# ...)
        body         — тело после разделителя ---
        has_kompaniya — есть ли уже блок **Компания:**
        raw_lines    — оригинальные строки файла (для дальнейшей модификации)
    """
    with open(filepath, "r", encoding="utf-8") as f:
        raw_lines = f.readlines()

    title = ""
    body_lines: list[str] = []
    body_started = False
    has_kompaniya = False

    for line in raw_lines:
        stripped = line.strip()

        if stripped.startswith("**Компания:**"):
            has_kompaniya = True

        if stripped.startswith("# ") and not title:
            title = stripped[2:].strip()
            continue

        if stripped == "---" and not body_started:
            body_started = True
            continue

        if body_started:
            body_lines.append(line)

    body = "".join(body_lines)
    return {
        "title": title,
        "body": body,
        "has_kompaniya": has_kompaniya,
        "raw_lines": raw_lines,
    }


def extract_first_paragraph(body: str) -> str:
    """
    Извлекает первый текстовый абзац из тела статьи.
    Пропускает ведущие пустые строки и строки с изображениями (![...]).
    Абзац заканчивается на первой пустой строке после начала текста.
    """
    paragraph_lines: list[str] = []
    started = False

    for line in body.split("\n"):
        stripped = line.strip()

        if not started:
            # до текста пропускаем пустые строки и изображения
            if not stripped or stripped.startswith("!["):
                continue
            started = True

        if started:
            if not stripped:
                break  # пустая строка — конец абзаца
            paragraph_lines.append(stripped)

    return " ".join(paragraph_lines)


def select_search_scope() -> int:
    """
    Просит пользователя выбрать область поиска тегов.

    Returns:
        1 — название + полное тело
        2 — название + первый абзац тела
        3 — только название
    """
    print()
    print("  Область поиска тегов:")
    print("    1. Название и тело статьи")
    print("    2. Название и первый абзац тела статьи")
    print("    3. Только название статьи")
    print()

    while True:
        choice = input("  Выберите вариант (1/2/3): ").strip()
        if choice in ("1", "2", "3"):
            return int(choice)
        print("  Введите 1, 2 или 3")


def find_matching_tags(title: str, body: str, tags: list, tag_map: dict, case_sensitive: bool = False) -> list:
    """
    Ищет теги в заголовке и теле статьи.
    Использует regex с границами слова (\b).

    Args:
        title:          Заголовок статьи.
        body:           Тело статьи.
        tags:           Список оригинальных тегов.
        tag_map:        {оригинальный тег: строка для поиска (возможно нормализованная)}.
        case_sensitive: True — строгое совпадение регистра, False — без учёта регистра.

    Returns:
        Список оригинальных тегов, найденных в тексте.
    """
    full_text = title + " " + body
    flags = 0 if case_sensitive else re.IGNORECASE
    found: list[str] = []

    for original_tag in tags:
        search_term = tag_map[original_tag]
        if not search_term:
            continue
        pattern = r"\b" + re.escape(search_term) + r"\b"
        if re.search(pattern, full_text, flags):
            found.append(original_tag)

    return found


def insert_kompaniya_block(raw_lines: list[str], tags: list) -> list[str]:
    """
    Вставляет строку **Компания:** после последней метаданной строки
    (после **Источник:**), перед пустой строкой / разделителем ---.

    Если строка **Источник:** не найдена — вставляет непосредственно перед ---.

    Returns:
        Новый список строк файла.
    """
    tags_str = ", ".join(tags)
    kompaniya_line = f"**Компания:** {tags_str}\n"

    # Ищем позицию для вставки: после **Источник:**
    insert_after = None
    separator_idx = None

    for i, line in enumerate(raw_lines):
        stripped = line.strip()
        if stripped.startswith("**Источник:**"):
            insert_after = i
        if stripped == "---" and separator_idx is None:
            separator_idx = i
            break  # берём первый ---

    if insert_after is not None:
        # Вставляем сразу после Источник
        new_lines = raw_lines[: insert_after + 1] + [kompaniya_line] + raw_lines[insert_after + 1 :]
    elif separator_idx is not None:
        # Fallback: перед ---
        new_lines = raw_lines[:separator_idx] + [kompaniya_line] + raw_lines[separator_idx:]
    else:
        # Нет ни Источник, ни --- — добавляем в конец
        new_lines = raw_lines + [kompaniya_line]

    return new_lines


def write_lines(filepath: str, lines: list[str]):
    """Записывает список строк в файл."""
    with open(filepath, "w", encoding="utf-8") as f:
        f.writelines(lines)


def apply_tags_to_articles(tags: list):
    """
    Часть 2: полный цикл автозаполнения тегов в .md файлах.
    """
    articles_path = Path(ARTICLES_DIR)

    if not articles_path.exists():
        print(f"\n  ⚠ Директория \"{ARTICLES_DIR}\" не найдена.")
        return

    md_files = sorted(articles_path.glob("*.md"))

    if not md_files:
        print(f"\n  ⚠ В каталоге \"{ARTICLES_DIR}\" нет файлов .md — нечего обрабатывать.")
        return

    print(f"\n  Найдено {len(md_files)} файл(ов) в \"{ARTICLES_DIR}\".")

    # --- Анализ trailing-символов ---
    trailing_chars = detect_trailing_chars(tags)
    normalize = False

    if trailing_chars:
        print()
        print("=" * 80)
        print("  Обнаружены теги со знаками в конце:")
        print("-" * 80)
        for char, char_tags in sorted(trailing_chars.items()):
            print(f"    Символ \"{char}\"  ({len(char_tags)} тег(ов)):")
            # показываем не более 5 примеров
            for tag in char_tags[:5]:
                print(f"      • {tag}")
            if len(char_tags) > 5:
                print(f"      ... и ещё {len(char_tags) - 5}")
        print("-" * 80)
        print()
        normalize = get_user_confirmation(
            "  Нормализовать теги (убрать знак в конце) перед поиском в статьях?",
            default=True,
        )
        if normalize:
            print("  ✓ Теги будут нормализованы для поиска (оригиналы сохраняются в файлах).")
        else:
            print("  Теги используются без изменений.")
    else:
        print("\n  Все теги оканчиваются на букву или цифру — нормализация не требуется.")

    # --- Маппинг тегов ---
    tag_map = build_tag_search_map(tags, normalize)

    # --- Выбор области поиска ---
    search_scope = select_search_scope()
    scope_labels = {1: "название + тело", 2: "название + первый абзац", 3: "только название"}
    print(f"  Выбрано: {scope_labels[search_scope]}")

    # --- Учёт регистра ---
    case_sensitive = get_user_confirmation(
        "  Строгое соответствие регистра букв в тегах?",
        default=False,
    )
    if case_sensitive:
        print('  Регистр: строгий  (тег "Apple" найдёт только "Apple")')
    else:
        print('  Регистр: без учёта (тег "Apple" найдёт "Apple", "apple", "APPLE" и т.д.)')

    # --- Обработка файлов ---
    print()
    print("=" * 80)
    print("  Автозаполнение тегов:")
    print("=" * 80)

    total_tagged = 0
    skipped_exists = 0
    skipped_no_match = 0

    for md_file in md_files:
        file_data = read_md_file(str(md_file))

        # Пропускаем если Компания уже есть
        if file_data["has_kompaniya"]:
            print(f"  ⏭  {md_file.name} — блок «Компания» уже есть, пропускаем")
            skipped_exists += 1
            continue

        # Определяем тело для поиска согласно выбранной области
        if search_scope == 1:
            search_body = file_data["body"]
        elif search_scope == 2:
            search_body = extract_first_paragraph(file_data["body"])
        else:  # 3
            search_body = ""

        # Ищем совпадения
        matched = find_matching_tags(
            file_data["title"],
            search_body,
            tags,
            tag_map,
            case_sensitive,
        )

        if not matched:
            print(f"  ○  {md_file.name} — совпадений не найдено")
            skipped_no_match += 1
            continue

        # Вставляем блок и записываем файл
        new_lines = insert_kompaniya_block(file_data["raw_lines"], matched)
        write_lines(str(md_file), new_lines)

        print(f"  ✓  {md_file.name}")
        print(f"      Теги: {', '.join(matched)}")
        total_tagged += 1

    # --- Итоги ---
    print()
    print("=" * 80)
    print("  Итоги автозаполнения:")
    print(f"    Обработано файлов:              {len(md_files)}")
    print(f"    Добавлен блок «Компания»:       {total_tagged}")
    if skipped_exists:
        print(f"    Пропущено (блок уже есть):      {skipped_exists}")
    if skipped_no_match:
        print(f"    Пропущено (нет совпадений):     {skipped_no_match}")
    print("=" * 80)


# ---------------------------------------------------------------------------
# Главный сценарий
# ---------------------------------------------------------------------------

def main():
    notion_token = get_notion_token()

    print()
    print("=" * 80)
    print("  Сбор тегов из Notion и автозаполнение в статьях")
    print("=" * 80)

    # ------------------------------------------------------------------
    # ЧАСТЬ 1: получаем теги (из Notion или из существующего файла)
    # ------------------------------------------------------------------
    print()
    print("  ЧАСТЬ 1: Сбор тегов")
    print("-" * 80)

    tags: list[str] | None = None
    existing_tags = load_tags()

    if existing_tags:
        print(f"\n  Файл \"{TAGS_FILE}\" уже существует ({len(existing_tags)} тегов).")
        if get_user_confirmation("  Пересобрать теги из Notion?", default=False):
            tags = None  # пересобираем
        else:
            tags = existing_tags
            print("  Используем существующие теги.")
    # Если тегов нет — собираем из Notion
    if tags is None:
        database_id = get_database_id_from_user()

        # Получаем структуру Database через NotionImporter
        importer = NotionImporter(notion_token)
        print("\n  Получение структуры Database...")
        try:
            structure = importer.get_database_structure(database_id)
        except Exception as e:
            print(f"\n  ❌ Ошибка при получении структуры Database: {e}")
            sys.exit(1)

        # Выбор поля
        property_name = select_multi_select_property(structure)

        # Извлекаем data_source_id из ответа databases.retrieve
        # (в версии API 2025-09-03 запрос идёт через data_sources, не databases)
        raw_db = structure.get("raw", {})
        data_sources = raw_db.get("data_sources", []) if isinstance(raw_db, dict) else []
        if not data_sources:
            print("\n  ❌ В Database не найдено data_sources. Невозможно выполнить запрос.")
            sys.exit(1)
        data_source_id = (
            data_sources[0].get("id")
            if isinstance(data_sources[0], dict)
            else getattr(data_sources[0], "id", None)
        )
        if not data_source_id:
            print("\n  ❌ Не удалось извлечь data_source ID.")
            sys.exit(1)
        print(f"  data_source_id: {data_source_id}")

        # Сбор тегов через API
        notion_client = Client(auth=notion_token)
        try:
            raw_tags = collect_tags_from_database(notion_client, data_source_id, property_name)
        except Exception as e:
            print(f"\n  ❌ Ошибка при сборе тегов: {e}")
            sys.exit(1)

        if not raw_tags:
            print("\n  ⚠ Тегов в указанном поле не найдено.")
            sys.exit(1)

        # Сохраняем и выводим
        tags = save_tags(raw_tags)

        print()
        print("  Собранные теги:")
        print("  " + "-" * 60)
        for tag in tags:
            print(f"    • {tag}")
        print("  " + "-" * 60)

    # ------------------------------------------------------------------
    # ЧАСТЬ 2: автозаполнение тегов в статьях
    # ------------------------------------------------------------------
    print()
    print("-" * 80)
    print("  ЧАСТЬ 2: Автозаполнение тегов в статьях")
    print("-" * 80)

    if not get_user_confirmation("  Продолжить с автозаполнением тегов?", default=True):
        print("  Автозаполнение отменено.")
        sys.exit(0)

    apply_tags_to_articles(tags)


if __name__ == "__main__":
    main()
