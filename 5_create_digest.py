#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для создания AI Digest в Notion

Функционал:
1. Создает страницу "AI Digest - Week X YEAR" в Database "Личный блог"
2. Заполняет стандартный шаблон с секциями Research, Notes, Draft
3. Собирает данные из Database "Обзор рынка технологии машинного обучения" за указанный период
4. Агрегирует новости по датам и добавляет в секцию Draft
"""

import os
import re
import sys
import httpx
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from collections import defaultdict
from notion_client import Client
from googletrans import Translator


def markdown_to_notion_blocks(markdown_content: str) -> List[dict]:
    """
    Конвертирует Markdown контент в блоки Notion API.
    Поддерживает: заголовки H1-H3, списки, toggle (blockquote), параграфы, изображения.
    """
    blocks = []
    lines = markdown_content.split('\n')

    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Пропускаем пустые строки
        if not stripped:
            i += 1
            continue

        # Заголовок H3 (### )
        if stripped.startswith('### '):
            blocks.append({
                "object": "block",
                "type": "heading_3",
                "heading_3": {
                    "rich_text": [{"type": "text", "text": {"content": stripped[4:].strip()}}]
                }
            })
            i += 1
            continue

        # Заголовок H2 (## )
        if stripped.startswith('## '):
            blocks.append({
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [{"type": "text", "text": {"content": stripped[3:].strip()}}]
                }
            })
            i += 1
            continue

        # Заголовок H1 (# )
        if stripped.startswith('# '):
            blocks.append({
                "object": "block",
                "type": "heading_1",
                "heading_1": {
                    "rich_text": [{"type": "text", "text": {"content": stripped[2:].strip()}}]
                }
            })
            i += 1
            continue

        # Изображение ![alt](url)
        img_match = re.match(r'!\[([^\]]*)\]\(([^)]+)\)', stripped)
        if img_match:
            blocks.append({
                "object": "block",
                "type": "image",
                "image": {
                    "type": "external",
                    "external": {"url": img_match.group(2)}
                }
            })
            i += 1
            continue

        # Маркированный список (- item)
        if stripped.startswith('- '):
            while i < len(lines) and lines[i].strip().startswith('- '):
                item_text = lines[i].strip()[2:].strip()
                if item_text:
                    blocks.append({
                        "object": "block",
                        "type": "bulleted_list_item",
                        "bulleted_list_item": {
                            "rich_text": [{"type": "text", "text": {"content": item_text}}]
                        }
                    })
                i += 1
            continue

        # Нумерованный список (1. item)
        if re.match(r'^\d+\.\s', stripped):
            while i < len(lines) and re.match(r'^\d+\.\s', lines[i].strip()):
                item_text = re.sub(r'^\d+\.\s', '', lines[i].strip()).strip()
                if item_text:
                    blocks.append({
                        "object": "block",
                        "type": "numbered_list_item",
                        "numbered_list_item": {
                            "rich_text": [{"type": "text", "text": {"content": item_text}}]
                        }
                    })
                i += 1
            continue

        # Toggle блок (> заголовок) — blockquote становится toggle
        if stripped.startswith('> '):
            toggle_title = stripped[2:].strip()
            toggle_content_lines = []
            i += 1

            # Пропускаем пустые строки после заголовка
            while i < len(lines) and lines[i].strip() == '':
                i += 1

            # Собираем контент до следующего toggle, заголовка H1, или конца
            while i < len(lines):
                current = lines[i]
                current_stripped = current.strip()
                # Останавливаемся на новом toggle или H1 заголовке
                if current_stripped.startswith('> ') or (current_stripped.startswith('# ') and not current_stripped.startswith('## ') and not current_stripped.startswith('### ')):
                    break
                toggle_content_lines.append(current)
                i += 1

            # Убираем trailing пустые строки
            while toggle_content_lines and toggle_content_lines[-1].strip() == '':
                toggle_content_lines.pop()

            # Создаём toggle с вложенным контентом
            toggle_block = {
                "object": "block",
                "type": "toggle",
                "toggle": {
                    "rich_text": [{"type": "text", "text": {"content": toggle_title}}],
                    "children": []
                }
            }

            # Добавляем содержимое toggle как вложенные блоки
            if toggle_content_lines:
                content_text = '\n'.join(toggle_content_lines)
                toggle_block["toggle"]["children"].append({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{"type": "text", "text": {"content": content_text}}]
                    }
                })

            blocks.append(toggle_block)
            continue

        # Горизонтальная линия (---)
        if stripped == '---' or stripped == '***' or stripped == '___':
            blocks.append({
                "object": "block",
                "type": "divider",
                "divider": {}
            })
            i += 1
            continue

        # Обычный текст — параграф
        blocks.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{"type": "text", "text": {"content": stripped}}]
            }
        })
        i += 1

    return blocks


def extract_database_id(url_or_id: str) -> str:
    """
    Извлекает Database ID из URL или возвращает ID как есть

    Args:
        url_or_id: URL Database или Database ID

    Returns:
        Database ID (32 символа)
    """
    if not url_or_id:
        return ""

    url_or_id = url_or_id.strip()

    # Если это URL, извлекаем ID
    if url_or_id.startswith('http'):
        # Убираем параметры запроса
        url_without_params = url_or_id.split('?')[0].split('#')[0]

        # Ищем последний сегмент URL
        parts = url_without_params.rstrip('/').split('/')
        if len(parts) > 0:
            last_part = parts[-1]
            # Убираем дефисы для проверки длины
            id_without_dashes = last_part.replace('-', '')
            if len(id_without_dashes) == 32:
                return last_part

    # Если это ID напрямую
    id_without_dashes = url_or_id.replace('-', '')
    if len(id_without_dashes) == 32:
        return url_or_id

    return url_or_id


class TitleVerifier:
    """Класс для проверки соответствия названий статей"""

    def __init__(self, max_concurrent: int = 5):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.max_concurrent = max_concurrent

    def detect_site_type(self, url: str) -> str:
        """Определяет тип сайта по URL"""
        domain = urlparse(url).netloc.lower()

        if 'vc.ru' in domain:
            return 'vcru'
        elif 'techcrunch.com' in domain:
            return 'techcrunch'
        elif 'habr.com' in domain:
            return 'habr'
        else:
            return 'unknown'

    def extract_title_from_soup(self, soup: BeautifulSoup, url: str) -> Optional[str]:
        """Извлекает заголовок из HTML в зависимости от типа сайта"""
        site_type = self.detect_site_type(url)

        if site_type == 'vcru':
            title_tag = soup.find('h1', class_=lambda x: x and 'content-title' in x)
            if title_tag:
                # Удаляем иконки
                title_copy = BeautifulSoup(str(title_tag), 'html.parser')
                for icon in title_copy.find_all('span', class_='content-title__editorial-icon'):
                    icon.decompose()
                for svg in title_copy.find_all('svg'):
                    svg.decompose()
                for use in title_copy.find_all('use'):
                    use.decompose()
                title = title_copy.get_text(separator=' ', strip=True)
                return re.sub(r'\s+', ' ', title).strip()

        elif site_type == 'techcrunch':
            title_tag = soup.find('h1', class_='wp-block-post-title')
            if title_tag:
                return title_tag.get_text(strip=True)

        elif site_type == 'habr':
            title_tag = soup.find('h1', class_='tm-title')
            if title_tag:
                span = title_tag.find('span')
                if span:
                    return span.get_text(strip=True)
                return title_tag.get_text(strip=True)

        # Универсальный парсинг
        for tag in ['h1', 'title']:
            title_tag = soup.find(tag)
            if title_tag:
                return title_tag.get_text(strip=True)

        return None

    async def fetch_title(self, session: aiohttp.ClientSession, url: str) -> Tuple[str, Optional[str], Optional[str]]:
        """
        Асинхронно получает title страницы по URL

        Returns:
            Tuple[url, title, error]
        """
        try:
            async with session.get(url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=15)) as response:
                response.raise_for_status()
                content = await response.read()
                soup = BeautifulSoup(content, 'html.parser')
                title = self.extract_title_from_soup(soup, url)
                return (url, title, None)
        except aiohttp.ClientError as e:
            return (url, None, f"Ошибка загрузки: {e}")
        except Exception as e:
            return (url, None, f"Ошибка: {e}")

    def normalize_title(self, title: str) -> str:
        """Нормализует название для сравнения"""
        if not title:
            return ""
        # Приводим к нижнему регистру, убираем лишние пробелы и спецсимволы
        normalized = title.lower()
        normalized = re.sub(r'\s+', ' ', normalized)
        normalized = normalized.strip()
        return normalized

    def titles_match(self, digest_title: str, actual_title: str, threshold: float = 0.7) -> bool:
        """
        Проверяет соответствие названий

        Args:
            digest_title: Название из Digest
            actual_title: Фактическое название со страницы
            threshold: Минимальный порог совпадения (0.0 - 1.0)

        Returns:
            True если названия соответствуют
        """
        if not digest_title or not actual_title:
            return False

        norm_digest = self.normalize_title(digest_title)
        norm_actual = self.normalize_title(actual_title)

        # Точное совпадение
        if norm_digest == norm_actual:
            return True

        # Проверяем вхождение одного в другое
        if norm_digest in norm_actual or norm_actual in norm_digest:
            return True

        # Расчет схожести через общие слова
        digest_words = set(norm_digest.split())
        actual_words = set(norm_actual.split())

        if not digest_words or not actual_words:
            return False

        common_words = digest_words & actual_words
        # Используем минимальный размер для расчета соответствия
        min_size = min(len(digest_words), len(actual_words))
        similarity = len(common_words) / min_size if min_size > 0 else 0

        return similarity >= threshold

    async def verify_titles_async(self, news_items: List[Dict], log_file: Optional[str] = None) -> List[Dict]:
        """
        Асинхронно проверяет соответствие названий для списка новостей

        Args:
            news_items: Список новостей с полями name и url
            log_file: Путь к файлу для логирования результатов

        Returns:
            Список несоответствий [{name, url, actual_title, error}]
        """
        mismatches = []
        all_results = []  # Для логирования всех результатов
        items_with_urls = [(item['name'], item['url']) for item in news_items if item.get('url')]

        if not items_with_urls:
            return mismatches

        async with aiohttp.ClientSession() as session:
            semaphore = asyncio.Semaphore(self.max_concurrent)

            async def fetch_with_semaphore(name: str, url: str) -> Dict:
                async with semaphore:
                    url_result, actual_title, error = await self.fetch_title(session, url)
                    return {
                        'name': name,
                        'url': url,
                        'actual_title': actual_title,
                        'error': error
                    }

            tasks = [fetch_with_semaphore(name, url) for name, url in items_with_urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    continue

                # Определяем статус соответствия
                if result.get('error'):
                    match_status = 'ERROR'
                    is_match = False
                elif result.get('actual_title'):
                    is_match = self.titles_match(result['name'], result['actual_title'])
                    match_status = 'MATCH' if is_match else 'MISMATCH'
                else:
                    match_status = 'NO_TITLE'
                    is_match = False

                # Сохраняем для логирования
                all_results.append({
                    'name': result['name'],
                    'url': result['url'],
                    'actual_title': result.get('actual_title'),
                    'error': result.get('error'),
                    'status': match_status
                })

                # Добавляем в список несоответствий
                if result.get('error'):
                    mismatches.append({
                        'name': result['name'],
                        'url': result['url'],
                        'actual_title': None,
                        'error': result['error']
                    })
                elif result.get('actual_title') and not is_match:
                    mismatches.append({
                        'name': result['name'],
                        'url': result['url'],
                        'actual_title': result['actual_title'],
                        'error': None
                    })

        # Логирование в файл
        if log_file:
            self._write_log(log_file, all_results)

        return mismatches

    def _write_log(self, log_file: str, results: List[Dict]):
        """Записывает результаты проверки в лог-файл"""
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write(f"# Лог проверки соответствия названий статей\n")
            f.write(f"# Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Всего проверено: {len(results)}\n")

            match_count = sum(1 for r in results if r['status'] == 'MATCH')
            mismatch_count = sum(1 for r in results if r['status'] == 'MISMATCH')
            error_count = sum(1 for r in results if r['status'] == 'ERROR')

            f.write(f"# Совпадений: {match_count}\n")
            f.write(f"# Несоответствий: {mismatch_count}\n")
            f.write(f"# Ошибок: {error_count}\n")
            f.write("=" * 80 + "\n\n")

            for i, result in enumerate(results, 1):
                status_icon = {
                    'MATCH': '✅',
                    'MISMATCH': '❌',
                    'ERROR': '⚠️',
                    'NO_TITLE': '❓'
                }.get(result['status'], '?')

                f.write(f"{i}. [{result['status']}] {status_icon}\n")
                f.write(f"   URL: {result['url']}\n")
                f.write(f"   Название в Digest:    {result['name']}\n")
                if result.get('error'):
                    f.write(f"   Ошибка: {result['error']}\n")
                elif result.get('actual_title'):
                    f.write(f"   Фактическое название: {result['actual_title']}\n")
                else:
                    f.write(f"   Фактическое название: (не удалось извлечь)\n")
                f.write("\n")

    def verify_titles(self, news_items: List[Dict], log_file: Optional[str] = None) -> List[Dict]:
        """Синхронная обертка для проверки названий"""
        return asyncio.run(self.verify_titles_async(news_items, log_file))


class ArticleTranslator:
    """Класс для перевода заголовков статей с английского на русский"""

    def __init__(self):
        self.translator = Translator()
        self._cache: Dict[str, str] = {}

    def is_english(self, text: str) -> bool:
        """
        Определяет, написан ли текст на английском языке.
        Использует эвристику на основе ASCII символов.

        Args:
            text: Текст для проверки

        Returns:
            True если текст на английском
        """
        if not text or not text.strip():
            return False

        # Проверяем по ASCII символам
        # Если большинство букв - латиница, считаем английским
        letters = [c for c in text if c.isalpha()]
        if not letters:
            return False

        ascii_letters = [c for c in letters if ord(c) < 128]
        # Если более 80% букв - ASCII (латиница), считаем английским
        return len(ascii_letters) / len(letters) > 0.8

    async def translate_to_russian_async(self, text: str) -> str:
        """
        Асинхронно переводит текст с английского на русский.

        Args:
            text: Текст для перевода

        Returns:
            Переведённый текст или оригинал при ошибке
        """
        if not text or not text.strip():
            return text

        # Проверяем кэш
        if text in self._cache:
            return self._cache[text]

        try:
            result = await self.translator.translate(text, src='en', dest='ru')
            translated = result.text
            self._cache[text] = translated
            return translated
        except Exception as e:
            print(f"⚠️ Ошибка перевода '{text[:50]}...': {e}")
            return text

    async def translate_if_english_async(self, text: str) -> Tuple[str, bool]:
        """
        Переводит текст, если он на английском языке.

        Args:
            text: Текст для проверки и перевода

        Returns:
            Tuple[переведённый_текст, был_ли_перевод]
        """
        if not text or not text.strip():
            return text, False

        if self.is_english(text):
            translated = await self.translate_to_russian_async(text)
            return translated, translated != text

        return text, False

    async def translate_news_titles_async(self, news_items: List[Dict]) -> List[Dict]:
        """
        Асинхронно переводит заголовки новостей, если они на английском.

        Args:
            news_items: Список новостей с полем 'name'

        Returns:
            Список новостей с переведёнными заголовками
        """
        translated_items = []
        translated_count = 0

        for item in news_items:
            new_item = item.copy()
            name = item.get('name', '')

            translated_name, was_translated = await self.translate_if_english_async(name)
            if was_translated:
                new_item['name'] = translated_name
                new_item['original_name'] = name  # Сохраняем оригинал
                translated_count += 1

            translated_items.append(new_item)

        if translated_count > 0:
            print(f"🌐 Переведено заголовков: {translated_count}")

        return translated_items

    def translate_news_titles(self, news_items: List[Dict]) -> List[Dict]:
        """Синхронная обёртка для перевода заголовков"""
        return asyncio.run(self.translate_news_titles_async(news_items))


class DigestCreator:
    """Класс для создания AI Digest в Notion"""

    def __init__(self, notion_token: str, blog_db_id: str, news_db_id: str, template_path: Optional[str] = None):
        """
        Инициализация

        Args:
            notion_token: API токен Notion
            blog_db_id: ID базы данных "Личный блог"
            news_db_id: ID базы данных "Обзор рынка технологии машинного обучения"
            template_path: Путь к Markdown файлу шаблона (опционально)
        """
        self.notion = Client(auth=notion_token)
        self.notion_token = notion_token
        self.blog_db_id = blog_db_id
        self.news_db_id = news_db_id
        self.template_path = template_path

    def get_current_week_info(self) -> Tuple[int, int]:
        """
        Получает номер текущей недели и год

        Returns:
            Tuple[week_number, year]
        """
        now = datetime.now()
        week_number = now.isocalendar()[1]
        year = now.year
        return week_number, year

    def create_digest_page(self, week_str: str, year: int) -> str:
        """
        Создает страницу AI Digest в Database "Личный блог"

        Args:
            week_str: Номер недели в виде строки (например, "01", "23", "N")
            year: Год

        Returns:
            ID созданной страницы
        """
        title = f"AI Digest - Week {week_str} {year}"

        # Properties для страницы
        properties = {
            "Name": {
                "title": [
                    {
                        "type": "text",
                        "text": {"content": title}
                    }
                ]
            },
            "Type": {
                "select": {"name": "Blog Post"}
            },
            "Тематика": {
                "multi_select": [{"name": "Новости"}]
            },
            "Status": {
                "select": {"name": "In Progress"}
            }
        }

        # Загружаем шаблон из Markdown файла и конвертируем в блоки Notion
        template_blocks = self._load_template_blocks()

        # Создаем страницу
        response = self.notion.pages.create(
            parent={"database_id": self.blog_db_id},
            properties=properties,
            children=template_blocks
        )

        return response["id"]

    def _load_template_blocks(self) -> List[dict]:
        """Загружает шаблон из Markdown файла и конвертирует в блоки Notion"""
        if not self.template_path:
            raise FileNotFoundError("Путь к файлу шаблона не указан")

        if not os.path.exists(self.template_path):
            raise FileNotFoundError(f"Файл шаблона не найден: {self.template_path}")

        with open(self.template_path, 'r', encoding='utf-8') as f:
            markdown_content = f.read()

        return markdown_to_notion_blocks(markdown_content)

    def fetch_news_from_database(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        Получает новости из Database "Обзор рынка технологии машинного обучения"
        за указанный период

        Args:
            start_date: Начальная дата
            end_date: Конечная дата

        Returns:
            Список новостей с полями name, url, date
        """
        # Формируем фильтр по дате
        filter_params = {
            "and": [
                {
                    "property": "Date",
                    "date": {
                        "on_or_after": start_date.strftime("%Y-%m-%d")
                    }
                },
                {
                    "property": "Date",
                    "date": {
                        "on_or_before": end_date.strftime("%Y-%m-%d")
                    }
                }
            ]
        }

        news_items = []
        has_more = True
        next_cursor = None

        while has_more:
            # Формируем тело запроса
            body = {
                "filter": filter_params,
                "sorts": [
                    {
                        "property": "Date",
                        "direction": "descending"
                    }
                ]
            }

            if next_cursor:
                body["start_cursor"] = next_cursor

            # Используем httpx напрямую для запроса к Notion API
            headers = {
                "Authorization": f"Bearer {self.notion_token}",
                "Content-Type": "application/json",
                "Notion-Version": "2022-06-28"
            }

            with httpx.Client() as client:
                api_response = client.post(
                    f"https://api.notion.com/v1/databases/{self.news_db_id}/query",
                    headers=headers,
                    json=body,
                    timeout=30.0
                )
                api_response.raise_for_status()
                response = api_response.json()

            for page in response.get("results", []):
                properties = page.get("properties", {})

                # Извлекаем Name (title)
                name = ""
                name_prop = properties.get("Name", {})
                if name_prop.get("type") == "title":
                    title_array = name_prop.get("title", [])
                    name = "".join([t.get("plain_text", "") for t in title_array])

                # Извлекаем URL
                url = ""
                url_prop = properties.get("URL", {})
                if url_prop.get("type") == "url":
                    url = url_prop.get("url", "") or ""

                # Извлекаем Date
                date_str = ""
                date_prop = properties.get("Date", {})
                if date_prop.get("type") == "date":
                    date_obj = date_prop.get("date", {})
                    if date_obj and date_obj.get("start"):
                        # Конвертируем из YYYY-MM-DD в DD.MM.YYYY
                        try:
                            dt = datetime.strptime(date_obj["start"], "%Y-%m-%d")
                            date_str = dt.strftime("%d.%m.%Y")
                        except:
                            date_str = date_obj["start"]

                if name:  # Добавляем только если есть название
                    news_items.append({
                        "name": name,
                        "url": url,
                        "date": date_str
                    })

            has_more = response.get("has_more", False)
            next_cursor = response.get("next_cursor")

        return news_items

    def aggregate_news_by_date(self, news_items: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Агрегирует новости по датам

        Args:
            news_items: Список новостей

        Returns:
            Словарь {дата: [новости]}
        """
        aggregated = defaultdict(list)

        for item in news_items:
            date = item.get("date", "Без даты")
            aggregated[date].append(item)

        return dict(aggregated)

    def format_news_as_markdown_blocks(self, aggregated_news: Dict[str, List[Dict]]) -> List[dict]:
        """
        Форматирует агрегированные новости в блоки Notion

        Args:
            aggregated_news: Словарь {дата: [новости]}

        Returns:
            Список блоков Notion
        """
        blocks = []

        # Сортируем даты в обратном порядке (новые сверху)
        sorted_dates = sorted(
            aggregated_news.keys(),
            key=lambda x: datetime.strptime(x, "%d.%m.%Y") if x != "Без даты" else datetime.min,
            reverse=False
        )

        for date in sorted_dates:
            news_list = aggregated_news[date]

            # Заголовок с датой (### DD.MM.YYYY)
            blocks.append({
                "object": "block",
                "type": "heading_3",
                "heading_3": {
                    "rich_text": [{"type": "text", "text": {"content": date}}]
                }
            })

            # Список новостей
            for news in news_list:
                name = news.get("name", "")
                url = news.get("url", "")

                if url:
                    # Создаем ссылку внутри названия
                    blocks.append({
                        "object": "block",
                        "type": "bulleted_list_item",
                        "bulleted_list_item": {
                            "rich_text": [
                                {
                                    "type": "text",
                                    "text": {
                                        "content": name,
                                        "link": {"url": url}
                                    }
                                }
                            ]
                        }
                    })
                else:
                    # Без ссылки
                    blocks.append({
                        "object": "block",
                        "type": "bulleted_list_item",
                        "bulleted_list_item": {
                            "rich_text": [
                                {
                                    "type": "text",
                                    "text": {"content": name}
                                }
                            ]
                        }
                    })

            # Пустая строка между датами
            blocks.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": []}
            })

        return blocks

    def append_blocks_to_page(self, page_id: str, blocks: List[dict]):
        """
        Добавляет блоки в конец страницы

        Args:
            page_id: ID страницы
            blocks: Список блоков для добавления
        """
        # Notion API ограничивает до 100 блоков за один запрос
        batch_size = 100

        for i in range(0, len(blocks), batch_size):
            batch = blocks[i:i + batch_size]
            self.notion.blocks.children.append(
                block_id=page_id,
                children=batch
            )


def ask_mode() -> str:
    """
    Спрашивает пользователя о режиме создания дайджеста

    Returns:
        'previous_week' или 'custom'
    """
    print("\n" + "=" * 60)
    print("Выберите режим создания дайджеста:")
    print("  1. За предыдущую неделю")
    print("  2. За произвольный период")
    print("=" * 60)

    while True:
        choice = input("\nВаш выбор [1/2]: ").strip()
        if choice == '1':
            return 'previous_week'
        elif choice == '2':
            return 'custom'
        print("Ошибка: введите 1 или 2")


def get_previous_week_info() -> Tuple[str, int, datetime, datetime]:
    """
    Вычисляет информацию о предыдущей неделе относительно текущей даты

    Returns:
        Tuple[week_str, year, start_date, end_date]
        week_str — номер недели в формате "01", "02", ...
    """
    today = datetime.now()
    current_week_start = today - timedelta(days=today.weekday())
    prev_week_start = current_week_start - timedelta(weeks=1)
    prev_week_end = prev_week_start + timedelta(days=6)

    week_number = prev_week_start.isocalendar()[1]
    year = prev_week_start.year
    week_str = f"{week_number:02d}"

    return week_str, year, prev_week_start, prev_week_end


def parse_date(date_str: str) -> Optional[datetime]:
    """
    Парсит дату в формате DD.MM.YYYY

    Args:
        date_str: Строка с датой

    Returns:
        datetime объект или None
    """
    try:
        return datetime.strptime(date_str.strip(), "%d.%m.%Y")
    except ValueError:
        return None


def get_database_urls_from_user() -> Tuple[str, str]:
    """
    Запрашивает URL баз данных у пользователя

    Returns:
        Tuple[blog_db_id, news_db_id]
    """
    print("\n" + "=" * 60)
    print("Введите URL или ID баз данных Notion")
    print("=" * 60)

    # База "Личный блог"
    while True:
        blog_input = input("\nURL Database 'Личный блог': ").strip()
        if blog_input:
            blog_db_id = extract_database_id(blog_input)
            if blog_db_id and len(blog_db_id.replace('-', '')) == 32:
                break
        print("Ошибка: введите корректный URL или ID базы данных")

    # База "Обзор рынка технологии машинного обучения"
    while True:
        news_input = input("URL Database 'Обзор рынка технологии машинного обучения': ").strip()
        if news_input:
            news_db_id = extract_database_id(news_input)
            if news_db_id and len(news_db_id.replace('-', '')) == 32:
                break
        print("Ошибка: введите корректный URL или ID базы данных")

    return blog_db_id, news_db_id


def get_date_range_from_user() -> Tuple[datetime, datetime]:
    """
    Запрашивает диапазон дат у пользователя

    Returns:
        Tuple[start_date, end_date]
    """
    print("\n" + "=" * 60)
    print("Введите диапазон дат для сбора новостей")
    print("Формат: DD.MM.YYYY")
    print("=" * 60)

    # Начальная дата
    while True:
        start_input = input("\nНачальная дата (DD.MM.YYYY): ").strip()
        start_date = parse_date(start_input)
        if start_date:
            break
        print("Ошибка: неверный формат даты. Используйте DD.MM.YYYY")

    # Конечная дата
    while True:
        end_input = input("Конечная дата (DD.MM.YYYY): ").strip()
        end_date = parse_date(end_input)
        if end_date:
            if end_date >= start_date:
                break
            print("Ошибка: конечная дата должна быть >= начальной")
        else:
            print("Ошибка: неверный формат даты. Используйте DD.MM.YYYY")

    return start_date, end_date


def get_template_path_from_user() -> str:
    """
    Сканирует директорию скрипта, показывает меню выбора MD-файлов шаблона.

    Returns:
        Путь к файлу шаблона
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Ищем все .md файлы в директории скрипта
    md_files = sorted([
        f for f in os.listdir(script_dir)
        if f.lower().endswith('.md') and os.path.isfile(os.path.join(script_dir, f))
    ])

    print("\n" + "=" * 60)
    print("Выбор файла шаблона")
    print("=" * 60)

    if md_files:
        print("\nНайденные Markdown-файлы:")
        for i, fname in enumerate(md_files, 1):
            print(f"  {i}. {fname}")
        print(f"  0. Ввести путь вручную")

        while True:
            choice = input("\nВаш выбор: ").strip()
            if choice == '0':
                break
            if choice.isdigit() and 1 <= int(choice) <= len(md_files):
                return os.path.join(script_dir, md_files[int(choice) - 1])
            print(f"Ошибка: введите число от 0 до {len(md_files)}")
    else:
        print("\nMD-файлы в директории скрипта не найдены. Введите путь вручную.")

    # Ручной ввод
    while True:
        template_input = input("\nВведите путь к файлу шаблона: ").strip()

        if not template_input:
            print("Ошибка: путь не может быть пустым")
            continue

        template_path = os.path.expanduser(template_input)

        if not os.path.isabs(template_path):
            template_path = os.path.join(script_dir, template_path)

        if not os.path.exists(template_path):
            print(f"Ошибка: файл не найден: {template_path}")
            continue

        return template_path


def main():
    """Основная функция"""
    # Получаем токен из переменных окружения или аргументов
    notion_token = os.getenv("NOTION_TOKEN")

    if len(sys.argv) > 1:
        notion_token = sys.argv[1]

    if not notion_token:
        print("Ошибка: NOTION_TOKEN не указан")
        print("\nИспользование:")
        print("  export NOTION_TOKEN='your_token'")
        print("  python3 create_digest.py")
        print("\nИли:")
        print("  python3 create_digest.py <NOTION_TOKEN>")
        sys.exit(1)

    print("=" * 60)
    print("🚀 Создание AI Digest в Notion")
    print("=" * 60)

    # Запрашиваем режим создания дайджеста
    mode = ask_mode()

    # Запрашиваем URL баз данных
    blog_db_id, news_db_id = get_database_urls_from_user()

    # Запрашиваем путь к шаблону
    template_path = get_template_path_from_user()

    print(f"\n📋 Используется шаблон: {template_path}")

    # Создаем экземпляр
    creator = DigestCreator(notion_token, blog_db_id, news_db_id, template_path=template_path)

    if mode == 'previous_week':
        week_str, year, start_date, end_date = get_previous_week_info()
        print(f"\n📅 Предыдущая неделя: {week_str}, Год: {year}")
        print(f"📊 Период сбора новостей: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}")
    else:
        year = datetime.now().year
        week_str = "N"
        start_date, end_date = get_date_range_from_user()
        print(f"\n📊 Период сбора новостей: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}")

    print(f"📝 Будет создана страница: AI Digest - Week {week_str} {year}")

    # Подтверждение
    confirm = input("\nПродолжить? [y/N]: ").strip().lower()
    if confirm not in ['y', 'yes', 'да', 'д']:
        print("Отменено пользователем.")
        sys.exit(0)

    # Шаг 1: Создаем страницу
    print("\n" + "-" * 60)
    print("📄 Создание страницы в Database 'Личный блог'...")

    try:
        page_id = creator.create_digest_page(week_str, year)
        print(f"✅ Страница создана! ID: {page_id}")
    except Exception as e:
        print(f"❌ Ошибка при создании страницы: {e}")
        sys.exit(1)

    # Шаг 2: Собираем новости
    print("\n" + "-" * 60)
    print("📰 Сбор новостей из Database 'Обзор рынка технологии машинного обучения'...")

    try:
        news_items = creator.fetch_news_from_database(start_date, end_date)
        print(f"✅ Найдено новостей: {len(news_items)}")
    except Exception as e:
        print(f"❌ Ошибка при сборе новостей: {e}")
        sys.exit(1)

    if not news_items:
        print("⚠️ Новостей за указанный период не найдено.")
        print("Страница создана с пустой секцией Draft.")
        sys.exit(0)

    # Шаг 3: Проверка соответствия названий статей (до перевода!)
    print("\n" + "-" * 60)
    print("🔍 Проверка соответствия названий статей...")

    logs_dir = "logs"
    os.makedirs(logs_dir, exist_ok=True)
    log_filename = os.path.join(logs_dir, f"title_verification_week{week_str}_{year}.log")
    verifier = TitleVerifier(max_concurrent=5)
    mismatches = verifier.verify_titles(news_items, log_file=log_filename)

    print(f"📝 Результаты сохранены в: {log_filename}")

    if mismatches:
        print(f"\n⚠️ Найдено несоответствий: {len(mismatches)}")
        print("-" * 60)
        for i, mismatch in enumerate(mismatches, 1):
            print(f"\n{i}. URL: {mismatch['url']}")
            print(f"   Название в Digest: {mismatch['name']}")
            if mismatch.get('error'):
                print(f"   Ошибка: {mismatch['error']}")
            else:
                print(f"   Фактическое название: {mismatch['actual_title']}")
    else:
        print("✅ Все названия соответствуют фактическим заголовкам статей")

    # Шаг 4: Перевод английских заголовков
    print("\n" + "-" * 60)
    print("🌐 Перевод английских заголовков...")

    try:
        translator = ArticleTranslator()
        news_items = translator.translate_news_titles(news_items)
    except Exception as e:
        print(f"⚠️ Ошибка при переводе (продолжаем без перевода): {e}")

    # Шаг 5: Агрегация по датам
    print("\n" + "-" * 60)
    print("📊 Агрегация новостей по датам...")

    aggregated = creator.aggregate_news_by_date(news_items)
    print(f"✅ Уникальных дат: {len(aggregated)}")

    for date, items in sorted(aggregated.items(), key=lambda x: datetime.strptime(x[0], "%d.%m.%Y") if x[0] != "Без даты" else datetime.min, reverse=False):
        print(f"   {date}: {len(items)} новостей")

    # Шаг 6: Форматируем и добавляем в страницу
    print("\n" + "-" * 60)
    print("📝 Добавление новостей в секцию Draft...")

    try:
        news_blocks = creator.format_news_as_markdown_blocks(aggregated)
        creator.append_blocks_to_page(page_id, news_blocks)
        print(f"✅ Добавлено блоков: {len(news_blocks)}")
    except Exception as e:
        print(f"❌ Ошибка при добавлении контента: {e}")
        sys.exit(1)

    # Готово
    print("\n" + "=" * 60)
    print("🎉 AI Digest успешно создан!")
    print(f"📄 Страница: AI Digest - Week {week_str} {year}")
    print(f"🔗 ID: {page_id}")
    print("=" * 60)


if __name__ == "__main__":
    main()
