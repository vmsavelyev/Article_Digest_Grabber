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
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from collections import defaultdict
from notion_client import Client


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


class DigestCreator:
    """Класс для создания AI Digest в Notion"""

    def __init__(self, notion_token: str, blog_db_id: str, news_db_id: str):
        """
        Инициализация

        Args:
            notion_token: API токен Notion
            blog_db_id: ID базы данных "Личный блог"
            news_db_id: ID базы данных "Обзор рынка технологии машинного обучения"
        """
        self.notion = Client(auth=notion_token)
        self.notion_token = notion_token
        self.blog_db_id = blog_db_id
        self.news_db_id = news_db_id

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

    def create_digest_page(self, week_number: int, year: int) -> str:
        """
        Создает страницу AI Digest в Database "Личный блог"

        Args:
            week_number: Номер недели
            year: Год

        Returns:
            ID созданной страницы
        """
        title = f"AI Digest - Week {week_number} {year}"

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

        # Шаблон контента страницы
        template_blocks = self._create_template_blocks()

        # Создаем страницу
        response = self.notion.pages.create(
            parent={"database_id": self.blog_db_id},
            properties=properties,
            children=template_blocks
        )

        page_id = response["id"]

        # Добавляем контент в toggle блоки
        self._populate_toggle_blocks(page_id)

        return page_id

    def _populate_toggle_blocks(self, page_id: str):
        """
        Добавляет контент в toggle блоки после создания страницы

        Args:
            page_id: ID созданной страницы
        """
        # Получаем все блоки страницы
        blocks_response = self.notion.blocks.children.list(block_id=page_id)
        blocks = blocks_response.get("results", [])

        # Находим toggle блоки и добавляем в них контент
        toggle_contents = self._get_toggle_content_blocks()

        toggle_index = 0
        for block in blocks:
            if block.get("type") == "toggle":
                if toggle_index < len(toggle_contents):
                    _, children = toggle_contents[toggle_index]
                    block_id = block.get("id")
                    if block_id and children:
                        self.notion.blocks.children.append(
                            block_id=block_id,
                            children=children
                        )
                    toggle_index += 1

    def _create_template_blocks(self) -> List[dict]:
        """Создает блоки шаблона для страницы (без вложенных children)"""
        blocks = [
            # # Research
            {
                "object": "block",
                "type": "heading_1",
                "heading_1": {
                    "rich_text": [{"type": "text", "text": {"content": "Research"}}]
                }
            },
            # Пустой параграф
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": []}
            },
            # # Notes
            {
                "object": "block",
                "type": "heading_1",
                "heading_1": {
                    "rich_text": [{"type": "text", "text": {"content": "Notes"}}]
                }
            },
            # Пункт списка: Шрифты для Linked IN
            {
                "object": "block",
                "type": "bulleted_list_item",
                "bulleted_list_item": {
                    "rich_text": [{"type": "text", "text": {"content": "Шрифты для Linked IN - Time New Romans. Прозрачность примерно 53%"}}]
                }
            },
            # Toggle: Промпт для нормализации текста
            {
                "object": "block",
                "type": "toggle",
                "toggle": {
                    "rich_text": [{"type": "text", "text": {"content": "Промпт для нормализации текста"}}]
                }
            },
            # Toggle: Промпт с переводом
            {
                "object": "block",
                "type": "toggle",
                "toggle": {
                    "rich_text": [{"type": "text", "text": {"content": "Тут я тестирую версию промпта с переводом"}}]
                }
            },
            # Toggle: Промпт для вступительного абзаца
            {
                "object": "block",
                "type": "toggle",
                "toggle": {
                    "rich_text": [{"type": "text", "text": {"content": "Промпт для вступительного абзаца"}}]
                }
            },
            # Toggle: Промпт для генерации названия темы
            {
                "object": "block",
                "type": "toggle",
                "toggle": {
                    "rich_text": [{"type": "text", "text": {"content": "Промпт для генерации названия темы"}}]
                }
            },
            # # Draft
            {
                "object": "block",
                "type": "heading_1",
                "heading_1": {
                    "rich_text": [{"type": "text", "text": {"content": "Draft"}}]
                }
            },
        ]

        return blocks

    def _get_toggle_content_blocks(self) -> List[Tuple[int, List[dict]]]:
        """
        Возвращает контент для toggle блоков (будет добавлен после создания страницы)

        Returns:
            Список кортежей (индекс_toggle_блока, [дочерние_блоки])
        """
        return [
            # Промпт для нормализации текста (индекс 4)
            (4, [
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{"type": "text", "text": {"content": """В это список идет список новостей в формате Название новости Ссылка на новость Дата новости

Агрегируй эти новости по дате в формет

Дата

Список новостей

Оригинальный текст названия новости не меняй. Ссылку на новости добавь во внутрь названия новости.  Дату измени в формат через точку."""}}]
                    }
                }
            ]),
            # Промпт с переводом (индекс 5)
            (5, [
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{"type": "text", "text": {"content": """В это список идет список новостей в формате Название новости Ссылка на новость Дата новости

Агрегируй эти новости по дате в формет

Дата

Список новостей

Ссылку на новости добавь во внутрь названия новости.  Дату измени в формат через точку. Названия статей переведи на русский.

Перевод не должен быть дословным, он дожен  передавать смысл статьи и быть адаптированным под русскоговорящего человека.  Используй англицизмы в переводе согласно последним веяниям разговорнгй моды. Изменяй граматику и пунктуацию и расстановку слов  так что бы твой перевол выглядел естественно. Перевод не должен выглядит топорно, он должен быть креативным и изобритетельным."""}}]
                    }
                }
            ]),
            # Промпт для вступительного абзаца (индекс 6)
            (6, [
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{"type": "text", "text": {"content": "Отлично, теперь ты дожен составить вступительный абзац для дайжества который будет опубликован в телеграмм. Во вступительном абаце нужно выделить 3 произволных новости кратко рассказав о них. Новости должны быть произвольно выбраныы из разных дат дайджеста."}}]
                    }
                }
            ]),
            # Промпт для генерации названия темы (индекс 7)
            (7, [
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{"type": "text", "text": {"content": "Придумай максимально точное название для этой статьи которое бы наиболее прозрачно описывваала суть этой стать. Вот пример хорошей темы **Google обновила Gemini 2.5 Pro - теперь модель лучше справляется с программированием и сложными вычислениями**"}}]
                    }
                }
            ]),
        ]

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
            reverse=True
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

    # Запрашиваем URL баз данных
    blog_db_id, news_db_id = get_database_urls_from_user()

    # Создаем экземпляр
    creator = DigestCreator(notion_token, blog_db_id, news_db_id)

    # Получаем номер недели и год
    week_number, year = creator.get_current_week_info()

    print(f"\n📅 Текущая неделя: {week_number}, Год: {year}")
    print(f"📝 Будет создана страница: AI Digest - Week {week_number} {year}")

    # Запрашиваем диапазон дат
    start_date, end_date = get_date_range_from_user()

    print(f"\n📊 Период сбора новостей: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}")

    # Подтверждение
    confirm = input("\nПродолжить? [y/N]: ").strip().lower()
    if confirm not in ['y', 'yes', 'да', 'д']:
        print("Отменено пользователем.")
        sys.exit(0)

    # Шаг 1: Создаем страницу
    print("\n" + "-" * 60)
    print("📄 Создание страницы в Database 'Личный блог'...")

    try:
        page_id = creator.create_digest_page(week_number, year)
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

    # Шаг 3: Агрегируем по датам
    print("\n" + "-" * 60)
    print("📊 Агрегация новостей по датам...")

    aggregated = creator.aggregate_news_by_date(news_items)
    print(f"✅ Уникальных дат: {len(aggregated)}")

    for date, items in sorted(aggregated.items(), key=lambda x: datetime.strptime(x[0], "%d.%m.%Y") if x[0] != "Без даты" else datetime.min, reverse=True):
        print(f"   {date}: {len(items)} новостей")

    # Шаг 4: Форматируем и добавляем в страницу
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
    print(f"📄 Страница: AI Digest - Week {week_number} {year}")
    print(f"🔗 ID: {page_id}")
    print("=" * 60)


if __name__ == "__main__":
    main()
