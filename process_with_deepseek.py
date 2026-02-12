#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для обработки статей через DeepSeek API

Функционал:
1. Читает .md файлы из каталога articles_markdown/
2. Извлекает тело статьи (без изображений)
3. Отправляет текст в DeepSeek API с системным промптом из выбранного файла
4. Записывает ответ API в заголовок H1 (# ) обработанного файла

Использование:
    python3 process_with_deepseek.py <DEEPSEEK_API_KEY>

    Скрипт покажет список доступных .txt файлов и запросит выбор файла промпта.
"""

import os
import re
import sys
import glob
import asyncio
from openai import AsyncOpenAI


# Директория со статьями относительно расположения скрипта
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ARTICLES_DIR = os.path.join(SCRIPT_DIR, "articles_markdown")
SYSTEM_PROMPT_FILE = os.path.join(SCRIPT_DIR, "system_prompt.txt")

# Модель DeepSeek
DEEPSEEK_MODEL = "deepseek-chat"
DEEPSEEK_BASE_URL = "https://api.deepseek.com"

# Количество параллельных запросов по умолчанию
DEFAULT_MAX_CONCURRENT = 5


def load_system_prompt(prompt_file):
    """Загружает системный промпт из указанного файла."""
    if not os.path.exists(prompt_file):
        print(f"Ошибка: файл системного промпта не найден: {prompt_file}")
        sys.exit(1)

    with open(prompt_file, "r", encoding="utf-8") as f:
        prompt = f.read().strip()

    if not prompt:
        print(f"Ошибка: файл системного промпта пуст: {prompt_file}")
        sys.exit(1)

    return prompt


def get_article_body(content):
    """
    Извлекает тело статьи — всё после первого разделителя ---.
    Если разделитель не найден, возвращает весь контент без H1 заголовка.
    """
    separator_match = re.search(r"^---\s*$", content, re.MULTILINE)
    if separator_match:
        return content[separator_match.end():].strip()

    # Если нет разделителя, убираем первую строку (H1) и метаданные
    lines = content.split("\n")
    body_lines = []
    started = False
    for line in lines:
        if started:
            body_lines.append(line)
        elif not line.startswith("#") and not line.startswith("**") and line.strip() == "":
            started = True

    return "\n".join(body_lines).strip() if body_lines else content


def remove_images(text):
    """
    Удаляет изображения из текста:
    - Markdown формат: ![alt](url)
    - HTML формат: <img ... /> и <img ...>
    """
    # Удаляем markdown изображения
    text = re.sub(r"!\[.*?\]\(.*?\)", "", text)
    # Удаляем HTML img теги
    text = re.sub(r"<img[^>]*>", "", text, flags=re.IGNORECASE)
    # Убираем лишние пустые строки, оставшиеся после удаления
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def replace_h1(content, new_title):
    """Заменяет содержимое H1 заголовка (первая строка # ...) на новый текст."""
    if content.startswith("# "):
        first_newline = content.index("\n") if "\n" in content else len(content)
        return f"# {new_title}" + content[first_newline:]
    else:
        # Если H1 не найден в начале, добавляем его
        return f"# {new_title}\n{content}"


def get_h1_title(content):
    """Извлекает текст H1 заголовка из содержимого файла."""
    if content.startswith("# "):
        first_newline = content.index("\n") if "\n" in content else len(content)
        return content[2:first_newline].strip()
    return ""


async def process_file(client, filepath, system_prompt, semaphore):
    """
    Обрабатывает один .md файл:
    1. Читает файл
    2. Извлекает тело статьи без изображений
    3. Отправляет в DeepSeek API
    4. Записывает ответ в H1 заголовок

    Возвращает кортеж (успех, файл, старый_заголовок, новый_заголовок).
    """
    filename = os.path.basename(filepath)

    # Читаем файл
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    old_title = get_h1_title(content)

    # Извлекаем тело статьи
    body = get_article_body(content)
    if not body:
        print(f"  [{filename}] Пропуск: тело статьи пустое")
        return False, filename, None, None

    # Удаляем изображения
    clean_body = remove_images(body)
    if not clean_body:
        print(f"  [{filename}] Пропуск: после удаления изображений текст пуст")
        return False, filename, None, None

    async with semaphore:
        print(f"  [{filename}] Отправка в DeepSeek API ({len(clean_body)} символов)...")

        try:
            response = await client.chat.completions.create(
                model=DEEPSEEK_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": clean_body},
                ],
            )

            new_title = response.choices[0].message.content.strip()

            print(f"  [{filename}] Старый заголовок: {old_title}")
            print(f"  [{filename}] Новый заголовок:  {new_title}")

            # Заменяем H1 заголовок на ответ API
            updated_content = replace_h1(content, new_title)

            # Записываем обновлённый файл
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(updated_content)

            return True, filename, old_title, new_title

        except Exception as e:
            print(f"  [{filename}] Ошибка API: {e}")
            return False, filename, None, None


async def run(api_key, system_prompt, md_files, max_concurrent):
    """Запускает параллельную обработку файлов."""
    client = AsyncOpenAI(
        api_key=api_key,
        base_url=DEEPSEEK_BASE_URL,
    )

    semaphore = asyncio.Semaphore(max_concurrent)

    tasks = [
        process_file(client, filepath, system_prompt, semaphore)
        for filepath in md_files
    ]

    return await asyncio.gather(*tasks)


def main():
    # Проверяем аргументы
    if len(sys.argv) < 2:
        print("Использование: python3 process_with_deepseek.py <DEEPSEEK_API_KEY>")
        sys.exit(1)

    api_key = sys.argv[1]

    # Запрашиваем у пользователя файл системного промпта
    # Показываем доступные .txt файлы в корне проекта
    txt_files = sorted(glob.glob(os.path.join(SCRIPT_DIR, "*.txt")))
    if txt_files:
        print("\nДоступные файлы промптов:")
        for i, f in enumerate(txt_files, 1):
            print(f"  {i}. {os.path.basename(f)}")
        print()

    prompt_input = input("Введите путь к файлу системного промпта (или номер из списка): ").strip()

    # Если ввели номер — выбираем из списка
    if prompt_input.isdigit() and txt_files:
        idx = int(prompt_input) - 1
        if 0 <= idx < len(txt_files):
            prompt_file = txt_files[idx]
        else:
            print("Ошибка: неверный номер")
            sys.exit(1)
    else:
        prompt_file = prompt_input

    # Загружаем системный промпт
    system_prompt = load_system_prompt(prompt_file)
    print(f"Системный промпт загружен из: {os.path.basename(prompt_file)} ({len(system_prompt)} символов)")

    # Запрашиваем количество параллельных запросов
    concurrent_input = input(f"Количество параллельных запросов (по умолчанию {DEFAULT_MAX_CONCURRENT}): ").strip()
    if concurrent_input.isdigit() and int(concurrent_input) > 0:
        max_concurrent = int(concurrent_input)
    else:
        max_concurrent = DEFAULT_MAX_CONCURRENT
    print(f"Параллельных запросов: {max_concurrent}")

    # Проверяем наличие каталога со статьями
    if not os.path.isdir(ARTICLES_DIR):
        print(f"Ошибка: каталог со статьями не найден: {ARTICLES_DIR}")
        sys.exit(1)

    # Находим все .md файлы
    md_files = sorted(glob.glob(os.path.join(ARTICLES_DIR, "*.md")))
    if not md_files:
        print(f"Нет .md файлов в каталоге: {ARTICLES_DIR}")
        sys.exit(0)

    print(f"Найдено файлов: {len(md_files)}")

    # Запускаем параллельную обработку
    results = asyncio.run(run(api_key, system_prompt, md_files, max_concurrent))

    # Итоговый отчёт
    report = []
    success_count = 0
    error_count = 0

    for success, filename, old_title, new_title in results:
        if success:
            success_count += 1
            report.append((filename, old_title, new_title))
        else:
            error_count += 1

    print(f"\n{'='*60}")
    print(f"ОТЧЁТ О ОБРАБОТКЕ")
    print(f"{'='*60}")
    print(f"Всего файлов: {len(md_files)}")
    print(f"Успешно: {success_count}")
    print(f"Ошибок/пропусков: {error_count}")

    if report:
        print(f"\n{'-'*60}")
        print(f"ИЗМЕНЕНИЯ ЗАГОЛОВКОВ:")
        print(f"{'-'*60}")
        for i, (filename, old_t, new_t) in enumerate(report, 1):
            print(f"\n{i}. {filename}")
            print(f"   Было:  {old_t}")
            print(f"   Стало: {new_t}")


if __name__ == "__main__":
    main()
