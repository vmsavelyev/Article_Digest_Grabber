#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для импорта markdown файлов статей в Notion Database
"""

import os
import json
import re
import sys
import asyncio
import time
from pathlib import Path
from notion_client import Client, AsyncClient
from datetime import datetime
from typing import List, Dict, Tuple, Optional


class NotionImporter:
    """Класс для импорта статей в Notion"""
    
    def __init__(self, notion_token: str, database_id: str = None, max_concurrent: int = 3):
        """
        Инициализация импортера
        
        Args:
            notion_token: API токен Notion (получить на https://www.notion.so/my-integrations)
            database_id: ID базы данных Notion (извлекается из URL), опционально
            max_concurrent: Максимальное количество одновременных запросов (по умолчанию 3, Notion rate limit)
        """
        self.notion_token = notion_token
        self.notion = Client(auth=notion_token)
        self.database_id = database_id
        self.database_properties = None
        self.max_concurrent = max_concurrent
    
    def get_database_structure(self, database_id: str) -> dict:
        """
        Получает структуру Database через Notion API
        
        Args:
            database_id: ID базы данных
            
        Returns:
            dict с информацией о структуре Database и properties
        """
        try:
            # Получаем информацию о Database
            database = self.notion.databases.retrieve(database_id)
            
            # Получаем properties через data_sources (правильный способ согласно Notion API)
            properties = {}
            
            # Получаем data_sources из database
            data_sources = []
            if isinstance(database, dict):
                data_sources = database.get('data_sources', [])
            elif hasattr(database, 'data_sources'):
                data_sources = database.data_sources if database.data_sources else []
            
            # Если есть data_sources, получаем properties из первого data source
            if data_sources and len(data_sources) > 0:
                # Берем первый data source
                data_source = data_sources[0]
                if isinstance(data_source, dict):
                    data_source_id = data_source.get('id')
                else:
                    data_source_id = getattr(data_source, 'id', None)
                
                if data_source_id:
                    # Получаем структуру data source
                    try:
                        data_source_info = self.notion.data_sources.retrieve(data_source_id)
                        if isinstance(data_source_info, dict):
                            properties = data_source_info.get('properties', {})
                        elif hasattr(data_source_info, 'properties'):
                            props = data_source_info.properties
                            if isinstance(props, dict):
                                properties = props
                    except Exception as e:
                        raise Exception(f"Не удалось получить data source: {e}")
            
            # Если properties все еще пустой, пробуем получить напрямую из database (fallback)
            if not properties:
                if isinstance(database, dict):
                    properties = database.get('properties', {})
                elif hasattr(database, 'properties'):
                    props = database.properties
                    if isinstance(props, dict):
                        properties = props
            
            # Формируем структурированную информацию о полях
            fields_info = {}
            if properties:
                for prop_name, prop_data in properties.items():
                    # Обрабатываем разные форматы prop_data
                    prop_type = 'unknown'
                    prop_id = None
                    
                    if isinstance(prop_data, dict):
                        prop_type = prop_data.get('type', 'unknown')
                        prop_id = prop_data.get('id')
                    elif hasattr(prop_data, 'type'):
                        prop_type = prop_data.type
                        prop_id = getattr(prop_data, 'id', None)
                    elif isinstance(prop_data, str):
                        # Если prop_data - это просто строка (название типа)
                        prop_type = prop_data
                    else:
                        # Пробуем получить тип через другие способы
                        prop_type = str(prop_data) if prop_data else 'unknown'
                    
                    fields_info[prop_name] = {
                        'type': prop_type,
                        'id': prop_id,
                        'name': prop_name
                    }
            
            # Получаем title
            title = "Без названия"
            if isinstance(database, dict):
                title = self._extract_title(database.get('title', []))
            elif hasattr(database, 'title'):
                title_obj = database.title
                if isinstance(title_obj, list):
                    title = self._extract_title(title_obj)
                else:
                    title = str(title_obj)
            
            return {
                'database_id': database_id,
                'title': title,
                'properties': fields_info,
                'raw': database if isinstance(database, dict) else str(database)
            }
        except Exception as e:
            raise Exception(f"Ошибка при получении структуры Database: {e}")
    
    def _extract_title(self, title_array: list) -> str:
        """Извлекает текст из массива rich text объектов Notion"""
        if not title_array:
            return "Без названия"
        text_parts = []
        for item in title_array:
            if item.get('type') == 'text':
                text_parts.append(item.get('text', {}).get('content', ''))
        return ''.join(text_parts) if text_parts else "Без названия"
    
    def display_database_structure(self, structure: dict):
        """Выводит структуру Database в читаемом формате"""
        print("\n" + "=" * 80)
        print(f"📊 Структура Database: {structure['title']}")
        print("=" * 80)
        print(f"Database ID: {structure['database_id']}")
        print(f"\nПоля (Properties):")
        print("-" * 80)
        
        if not structure['properties']:
            print("  ⚠ В Database не найдено полей")
            return
        
        for i, (prop_name, prop_info) in enumerate(structure['properties'].items(), 1):
            prop_type = prop_info['type']
            print(f"  {i}. {prop_name}")
            print(f"     Тип: {prop_type}")
        
        print("-" * 80)
    
    def explain_mapping(self, structure: dict) -> dict:
        """
        Объясняет, какие данные будут импортированы в какие поля
        
        Returns:
            dict с маппингом полей
        """
        mapping = {}
        properties = structure['properties']
        
        # Автоматическое сопоставление полей
        # Ищем поля по стандартным названиям
        title_field = None
        url_field = None
        date_field = None
        
        # Ищем поле типа Title для заголовка
        for prop_name, prop_info in properties.items():
            if prop_info['type'] == 'title':
                title_field = prop_name
                break
        
        # Ищем поле URL
        for prop_name, prop_info in properties.items():
            if prop_info['type'] == 'url':
                if url_field is None or 'url' in prop_name.lower():
                    url_field = prop_name
        
        # Ищем поле Date
        for prop_name, prop_info in properties.items():
            if prop_info['type'] == 'date':
                if date_field is None or 'дата' in prop_name.lower() or 'date' in prop_name.lower():
                    date_field = prop_name
        
        mapping['title'] = title_field
        mapping['url'] = url_field
        mapping['date'] = date_field
        
        return mapping
    
    def display_mapping(self, mapping: dict, structure: dict):
        """Выводит информацию о маппинге данных"""
        print("\n" + "=" * 80)
        print("📋 Маппинг данных для импорта:")
        print("=" * 80)
        
        print("\nБудут импортированы следующие данные:")
        print("-" * 80)
        
        # Заголовок статьи
        title_field = mapping.get('title')
        if title_field:
            print(f"  ✓ Заголовок статьи → поле '{title_field}' (Title)")
        else:
            print(f"  ⚠ Заголовок статьи → НЕ НАЙДЕНО подходящее поле (нужно поле типа Title)")
        
        # URL статьи
        url_field = mapping.get('url')
        if url_field:
            print(f"  ✓ URL статьи → поле '{url_field}' (URL)")
        else:
            print(f"  ⚠ URL статьи → НЕ НАЙДЕНО подходящее поле (нужно поле типа URL)")
        
        # Дата публикации
        date_field = mapping.get('date')
        if date_field:
            print(f"  ✓ Дата публикации → поле '{date_field}' (Date)")
        else:
            print(f"  ⚠ Дата публикации → НЕ НАЙДЕНО подходящее поле (нужно поле типа Date)")
        
        print("\nКонтент статьи:")
        print("  ✓ Тело статьи будет добавлено как контент страницы (блоки)")
        
        print("-" * 80)
        
        # Предупреждения
        warnings = []
        if not title_field:
            warnings.append("Не найдено поле типа Title для заголовка")
        if not url_field:
            warnings.append("Не найдено поле типа URL для ссылки")
        if not date_field:
            warnings.append("Не найдено поле типа Date для даты")
        
        if warnings:
            print("\n⚠ Предупреждения:")
            for warning in warnings:
                print(f"  - {warning}")
            print("\nИмпорт продолжится, но эти данные не будут сохранены в properties.")
    
    def parse_markdown_file(self, filepath: str) -> dict:
        """Парсит markdown файл и извлекает метаданные и контент"""
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Извлекаем заголовок (первая строка с #)
        # Используем более надежный способ - ищем первую строку, начинающуюся с #
        lines = content.split('\n')
        title = None
        for line in lines:
            stripped = line.strip()
            if stripped.startswith('# '):
                # Берем все после "# "
                title = stripped[2:].strip()
                break
            elif stripped.startswith('#'):
                # Берем все после первого #
                title = stripped.lstrip('#').strip()
                break
        
        # Извлекаем дату публикации
        date_match = re.search(r'\*\*Дата публикации:\*\*\s+(\d{2}\.\d{2}\.\d{4})', content)
        date_str = date_match.group(1) if date_match else None
        
        # Извлекаем URL источника
        url_match = re.search(r'\*\*Источник:\*\*\s+(https?://[^\s]+)', content)
        url = url_match.group(1) if url_match else None
        
        # Извлекаем только тело статьи (после разделителя ---)
        lines = content.split('\n')
        body_started = False
        body_lines = []
        
        for line in lines:
            # Пропускаем заголовок
            if line.startswith('# '):
                continue
            # Пропускаем метаданные (дата, источник)
            if line.startswith('**') and ('Дата' in line or 'Источник' in line):
                continue
            # Начинаем собирать тело после разделителя
            if line.strip() == '---':
                body_started = True
                continue
            # Если разделитель уже прошел, собираем контент
            if body_started:
                body_lines.append(line)
            # Если разделителя нет, но мы уже прошли заголовок и метаданные
            elif not body_started and not line.startswith('#') and not (line.startswith('**') and ('Дата' in line or 'Источник' in line)):
                # Проверяем, что это не пустая строка между заголовком и метаданными
                if line.strip() or body_lines:
                    body_started = True
                    body_lines.append(line)
        
        body_content = '\n'.join(body_lines).strip()
        
        return {
            'title': title,
            'date': date_str,
            'url': url,
            'body': body_content
        }
    
    def markdown_to_notion_blocks(self, markdown_content: str) -> list:
        """
        Конвертирует markdown контент в список блоков Notion API.

        Эта функция преобразует текст в формате markdown в структурированные блоки,
        совместимые с Notion API. Поддерживается преобразование параграфов, изображений
        и маркированных списков в соответствующие типы блоков Notion.

        Args:
            markdown_content (str): Строка с контентом в формате markdown для конвертации.
                                   Может содержать:
                                   - Обычный текст (преобразуется в paragraph блоки)
                                   - Изображения в формате ![alt](url)
                                   - Маркированные списки, начинающиеся с "- "

        Returns:
            list: Список словарей, представляющих блоки Notion API. Каждый блок содержит:
                  - object: "block" (тип объекта Notion)
                  - type: тип блока ("paragraph", "image", "bulleted_list_item")
                  - [type]: вложенный объект с содержимым блока

                  Пример структуры блока параграфа:
                  {
                      "object": "block",
                      "type": "paragraph",
                      "paragraph": {
                          "rich_text": [{
                              "type": "text",
                              "text": {"content": "текст параграфа"}
                          }]
                      }
                  }

                  Пример структуры блока изображения:
                  {
                      "object": "block",
                      "type": "image",
                      "image": {
                          "type": "external",
                          "external": {"url": "https://example.com/image.jpg"}
                      }
                  }

        Notes:
            - Пустые строки игнорируются и не создают блоки
            - Изображения извлекаются с использованием regex паттерна ![alt](url)
            - Маркированные списки определяются по префиксу "- " в начале строки
            - Последовательные элементы списка обрабатываются как отдельные блоки
            - Множественные параграфы объединяются до встречи специального элемента
              (изображения, списка или пустой строки)
            - Функция не поддерживает:
              * Заголовки (h1, h2, и т.д.)
              * Нумерованные списки
              * Форматирование текста (жирный, курсив)
              * Ссылки (кроме изображений)
              * Блоки кода
              * Цитаты

        Example:
            >>> importer = NotionImporter(token="secret_xxx")
            >>> markdown = '''
            ... Это первый параграф текста.
            ...
            ... ![Описание](https://example.com/image.jpg)
            ...
            ... - Первый элемент списка
            ... - Второй элемент списка
            ...
            ... Это второй параграф.
            ... '''
            >>> blocks = importer.markdown_to_notion_blocks(markdown)
            >>> len(blocks)
            5
            >>> blocks[0]['type']
            'paragraph'
            >>> blocks[1]['type']
            'image'
            >>> blocks[2]['type']
            'bulleted_list_item'

        Raises:
            None: Функция не генерирует исключений. Некорректный markdown
                  будет обработан как обычный текст.

        See Also:
            - parse_markdown_file: Парсит markdown файл и извлекает метаданные
            - create_page: Создает страницу в Notion с использованием блоков
        """
        blocks = []
        lines = markdown_content.split('\n')
        
        i = 0
        while i < len(lines):
            line = lines[i]
            stripped_line = line.strip()
            
            if not stripped_line:
                i += 1
                continue
            
            # Изображение: ![alt](url) - может быть на отдельной строке
            img_match = re.search(r'!\[([^\]]*)\]\(([^)]+)\)', stripped_line)
            if img_match:
                img_url = img_match.group(2)
                blocks.append({
                    "object": "block",
                    "type": "image",
                    "image": {
                        "type": "external",
                        "external": {
                            "url": img_url
                        }
                    }
                })
                i += 1
                continue
            
            # Маркированный список: - item
            if stripped_line.startswith('- '):
                list_items = []
                while i < len(lines) and lines[i].strip().startswith('- '):
                    item_text = lines[i].strip()[2:].strip()
                    if item_text:
                        list_items.append(item_text)
                    i += 1
                
                # Создаем элементы списка
                for item in list_items:
                    blocks.append({
                        "object": "block",
                        "type": "bulleted_list_item",
                        "bulleted_list_item": {
                            "rich_text": [
                                {
                                    "type": "text",
                                    "text": {"content": item}
                                }
                            ]
                        }
                    })
                continue
            
            # Обычный текст (параграф)
            # Собираем текст до следующего специального элемента
            paragraph_lines = [stripped_line]
            i += 1
            
            while i < len(lines):
                next_line = lines[i].strip()
                # Останавливаемся на пустой строке, списке или изображении
                if not next_line:
                    break
                if next_line.startswith('- '):
                    break
                if re.search(r'!\[.*\]\(.*\)', next_line):
                    break
                paragraph_lines.append(next_line)
                i += 1
            
            paragraph_text = ' '.join(paragraph_lines).strip()
            if paragraph_text:
                blocks.append({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [
                            {
                                "type": "text",
                                "text": {"content": paragraph_text}
                            }
                        ]
                    }
                })
        
        return blocks
    
    def parse_date(self, date_str: str) -> dict:
        """Парсит дату в формате DD.MM.YYYY для Notion"""
        try:
            dt = datetime.strptime(date_str, '%d.%m.%Y')
            return {
                "start": dt.strftime('%Y-%m-%d'),
                "time_zone": None
            }
        except:
            return None
    
    def create_page(self, article_data: dict, field_mapping: dict = None) -> str:
        """
        Создает страницу в Notion Database
        
        Args:
            article_data: Данные статьи для импорта
            field_mapping: Маппинг полей (если None, используется стандартный)
        """
        if field_mapping is None:
            # Стандартный маппинг для обратной совместимости
            field_mapping = {
                'title': 'Name',
                'url': 'URL',
                'date': 'Дата публикации'
            }
        
        # Подготовка properties
        properties = {}
        title_text = article_data.get('title') or "Без заголовка"
        
        # Title property
        title_field = field_mapping.get('title')
        if title_field:
            properties[title_field] = {
                "title": [
                    {
                        "type": "text",
                        "text": {
                            "content": title_text
                        }
                    }
                ]
            }
        
        # URL property
        url_field = field_mapping.get('url')
        if url_field and article_data.get('url'):
            properties[url_field] = {
                "url": article_data['url']
            }
        
        # Дата публикации property
        date_field = field_mapping.get('date')
        if date_field and article_data.get('date'):
            date_obj = self.parse_date(article_data['date'])
            if date_obj:
                properties[date_field] = {
                    "date": date_obj
                }
        
        # Конвертируем markdown body в блоки Notion
        blocks = self.markdown_to_notion_blocks(article_data['body'])
        
        # Создаем страницу
        try:
            response = self.notion.pages.create(
                parent={"database_id": self.database_id},
                properties=properties,
                children=blocks
            )
            return response['id']
        except Exception as e:
            print(f"  Ошибка при создании страницы: {e}")
            raise
    
    async def create_page_async(self, async_client: AsyncClient, article_data: dict, field_mapping: dict = None) -> Tuple[str, Optional[str], Optional[str]]:
        """
        Асинхронно создает страницу в Notion Database
        
        Args:
            async_client: Асинхронный клиент Notion
            article_data: Данные статьи для импорта
            field_mapping: Маппинг полей
            
        Returns:
            Tuple[title, page_id или None, error или None]
        """
        if field_mapping is None:
            field_mapping = {
                'title': 'Name',
                'url': 'URL',
                'date': 'Дата публикации'
            }
        
        title_text = article_data.get('title') or "Без заголовка"
        
        # Подготовка properties
        properties = {}
        
        # Title property
        title_field = field_mapping.get('title')
        if title_field:
            properties[title_field] = {
                "title": [
                    {
                        "type": "text",
                        "text": {
                            "content": title_text
                        }
                    }
                ]
            }
        
        # URL property
        url_field = field_mapping.get('url')
        if url_field and article_data.get('url'):
            properties[url_field] = {
                "url": article_data['url']
            }
        
        # Дата публикации property
        date_field = field_mapping.get('date')
        if date_field and article_data.get('date'):
            date_obj = self.parse_date(article_data['date'])
            if date_obj:
                properties[date_field] = {
                    "date": date_obj
                }
        
        # Конвертируем markdown body в блоки Notion
        blocks = self.markdown_to_notion_blocks(article_data['body'])
        
        try:
            response = await async_client.pages.create(
                parent={"database_id": self.database_id},
                properties=properties,
                children=blocks
            )
            return (title_text, response['id'], None)
        except Exception as e:
            return (title_text, None, str(e))
    
    async def import_batch_async(self, articles_data: List[dict], field_mapping: dict = None) -> Tuple[int, int]:
        """
        Асинхронно импортирует список статей
        
        Args:
            articles_data: Список данных статей
            field_mapping: Маппинг полей
            
        Returns:
            Tuple[успешно импортировано, ошибок]
        """
        async with AsyncClient(auth=self.notion_token) as async_client:
            semaphore = asyncio.Semaphore(self.max_concurrent)
            
            async def create_with_semaphore(article_data: dict, index: int) -> Tuple[int, str, Optional[str], Optional[str]]:
                async with semaphore:
                    # Небольшая задержка для соблюдения rate limits
                    await asyncio.sleep(0.1)
                    title, page_id, error = await self.create_page_async(async_client, article_data, field_mapping)
                    return (index, title, page_id, error)
            
            tasks = [create_with_semaphore(article, i) for i, article in enumerate(articles_data)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            imported = 0
            errors = 0
            
            # Сортируем результаты по индексу для правильного вывода
            sorted_results = sorted(
                [(r if not isinstance(r, Exception) else (r, None, None, str(r))) for r in results],
                key=lambda x: x[0] if isinstance(x[0], int) else 0
            )
            
            for result in sorted_results:
                if isinstance(result, Exception):
                    print(f"  ✗ Критическая ошибка: {result}")
                    errors += 1
                else:
                    index, title, page_id, error = result
                    if error:
                        print(f"  ✗ [{index + 1}/{len(articles_data)}] {title[:50]}... - Ошибка: {error}")
                        errors += 1
                    else:
                        print(f"  ✓ [{index + 1}/{len(articles_data)}] {title[:50]}... (ID: {page_id[:8]}...)")
                        imported += 1
            
            return (imported, errors)
    
    def import_from_directory(self, markdown_dir: str, json_file: str = None, field_mapping: dict = None, use_async: bool = True):
        """Импортирует все markdown файлы из директории"""
        markdown_path = Path(markdown_dir)
        
        if not markdown_path.exists():
            print(f"Директория {markdown_dir} не найдена")
            return
        
        # Загружаем данные из JSON для получения метаданных
        json_data = []
        articles_metadata = {}
        if json_file and os.path.exists(json_file):
            with open(json_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
                for article in json_data:
                    if article.get('url'):
                        articles_metadata[article['url']] = article
        
        # Получаем все markdown файлы
        md_files = sorted(markdown_path.glob('*.md'))
        
        print(f"Найдено {len(md_files)} markdown файлов для импорта")
        print(f"Режим: {'асинхронный' if use_async else 'последовательный'} (до {self.max_concurrent} запросов одновременно)")
        print("=" * 80)
        
        # Подготавливаем данные статей
        articles_data = []
        skipped = 0
        
        for i, md_file in enumerate(md_files, 1):
            try:
                # Парсим markdown файл
                article_data = self.parse_markdown_file(str(md_file))
                
                # Если есть метаданные из JSON, используем их (они более точные)
                file_num = re.match(r'^(\d+)_', md_file.name)
                if file_num and json_data:
                    file_index = int(file_num.group(1)) - 1
                    if 0 <= file_index < len(json_data):
                        json_article = json_data[file_index]
                        if json_article.get('title'):
                            article_data['title'] = json_article['title']
                        if json_article.get('date'):
                            article_data['date'] = json_article['date']
                        if json_article.get('url'):
                            article_data['url'] = json_article['url']
                elif article_data.get('url') and article_data['url'] in articles_metadata:
                    json_article = articles_metadata[article_data['url']]
                    if json_article.get('title'):
                        article_data['title'] = json_article['title']
                    if json_article.get('date'):
                        article_data['date'] = json_article['date']
                    if json_article.get('url'):
                        article_data['url'] = json_article['url']
                
                # Проверяем, что есть необходимые данные
                if not article_data.get('title'):
                    print(f"  ⚠ [{i}/{len(md_files)}] {md_file.name} - заголовок не найден, пропускаю")
                    skipped += 1
                    continue
                
                articles_data.append(article_data)
                
            except Exception as e:
                print(f"  ✗ [{i}/{len(md_files)}] {md_file.name} - ошибка парсинга: {e}")
                skipped += 1
        
        if not articles_data:
            print("Нет статей для импорта")
            return
        
        print(f"\nПодготовлено {len(articles_data)} статей для импорта")
        if skipped > 0:
            print(f"Пропущено: {skipped}")
        print("-" * 80)
        
        start_time = time.time()
        
        if use_async:
            # Асинхронный импорт
            imported, errors = asyncio.run(self.import_batch_async(articles_data, field_mapping))
        else:
            # Последовательный импорт (fallback)
            imported = 0
            errors = 0
            for i, article_data in enumerate(articles_data, 1):
                try:
                    title = article_data.get('title', '')
                    page_id = self.create_page(article_data, field_mapping)
                    print(f"  ✓ [{i}/{len(articles_data)}] {title[:50]}... (ID: {page_id[:8]}...)")
                    imported += 1
                except Exception as e:
                    print(f"  ✗ [{i}/{len(articles_data)}] {title[:50]}... - Ошибка: {e}")
                    errors += 1
        
        elapsed_time = time.time() - start_time
        
        print("\n" + "=" * 80)
        print(f"Импорт завершен за {elapsed_time:.2f} секунд!")
        print(f"Успешно импортировано: {imported}")
        print(f"Ошибок: {errors}")
        if skipped > 0:
            print(f"Пропущено (ошибки парсинга): {skipped}")
        if imported > 0:
            print(f"Средняя скорость: {imported / elapsed_time:.2f} статей/сек")


def extract_database_id(input_value: str) -> str:
    """
    Извлекает Database ID из URL или возвращает ID, если он уже в правильном формате
    
    Args:
        input_value: URL Database или Database ID
        
    Returns:
        Database ID (32 символа, может содержать дефисы)
    """
    if not input_value:
        return ""
    
    input_value = input_value.strip()
    
    # Если это URL, извлекаем ID
    if input_value.startswith('http'):
        # Ищем паттерн: .../ID?v=... или .../ID
        # Database ID - это 32 символа (может быть с дефисами или без)
        # Формат: abc123def456ghi789jkl012mno345pq или abc123def-456ghi-789jkl-012mno345pq
        
        # Убираем параметры запроса и якоря
        url_without_params = input_value.split('?')[0].split('#')[0]
        
        # Ищем последний сегмент URL (после последнего /)
        parts = url_without_params.rstrip('/').split('/')
        if len(parts) > 0:
            last_part = parts[-1]
            
            # Database ID может быть:
            # 1. 32 символа без дефисов: abc123def456ghi789jkl012mno345pq
            # 2. 32 символа с дефисами: abc123def-456ghi-789jkl-012mno345pq
            # 3. UUID формат: abc123def-4567-89ab-cdef-0123456789ab
            
            # Убираем дефисы для проверки длины
            id_without_dashes = last_part.replace('-', '')
            
            # Database ID должен быть 32 символа (без дефисов)
            if len(id_without_dashes) == 32:
                return last_part
    
    # Если это не URL, проверяем, что это похоже на Database ID
    # Database ID: 32 символа (может быть с дефисами или без)
    id_without_dashes = input_value.replace('-', '')
    if len(id_without_dashes) == 32:
        return input_value
    
    # Если не похоже на ID и не URL, возвращаем как есть (будет ошибка при использовании)
    return input_value


def get_user_confirmation(prompt: str, default: bool = False) -> bool:
    """Запрашивает подтверждение у пользователя"""
    default_text = "Y/n" if default else "y/N"
    response = input(f"{prompt} [{default_text}]: ").strip().lower()
    
    if not response:
        return default
    
    return response in ['y', 'yes', 'да', 'д']


def main():
    """Основная функция"""
    # Парсим аргументы командной строки
    notion_token = os.getenv('NOTION_TOKEN')
    database_id = os.getenv('NOTION_DATABASE_ID')
    max_concurrent = 3  # Notion rate limit: 3 запроса в секунду
    use_async = True
    
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '--concurrent' and i + 1 < len(sys.argv):
            try:
                max_concurrent = int(sys.argv[i + 1])
                if max_concurrent < 1:
                    max_concurrent = 1
                elif max_concurrent > 10:
                    max_concurrent = 10
                    print(f"Предупреждение: количество одновременных запросов ограничено до 10 (Notion rate limits)")
            except ValueError:
                print(f"Ошибка: неверное значение для --concurrent: {sys.argv[i + 1]}")
                sys.exit(1)
            i += 2
        elif arg == '--sync':
            use_async = False
            i += 1
        elif arg == '--database' and i + 1 < len(sys.argv):
            database_id = sys.argv[i + 1]
            i += 2
        elif arg == '--help' or arg == '-h':
            print("Использование: python3 import_to_notion.py [опции] [NOTION_TOKEN] [DATABASE_ID]")
            print("\nОпции:")
            print("  --concurrent <число>  Количество одновременных запросов (1-10, по умолчанию 3)")
            print("  --sync                Использовать последовательный режим вместо асинхронного")
            print("  --database <id/url>   Database ID или URL")
            print("  --help, -h            Показать эту справку")
            print("\nПеременные окружения:")
            print("  NOTION_TOKEN          API токен Notion")
            print("  NOTION_DATABASE_ID    Database ID")
            print("\nПримеры:")
            print("  python3 import_to_notion.py secret_xxx")
            print("  python3 import_to_notion.py --concurrent 5 --database https://notion.so/...")
            sys.exit(0)
        elif not notion_token:
            notion_token = arg
            i += 1
        elif not database_id:
            database_id = arg
            i += 1
        else:
            i += 1
    
    # Проверяем наличие токена
    if not notion_token:
        print("Ошибка: NOTION_TOKEN не указан")
        print("\nИспользование:")
        print("  1. Через переменные окружения:")
        print("     export NOTION_TOKEN='your_token'")
        print("     python3 import_to_notion.py")
        print("\n  2. Через аргументы командной строки:")
        print("     python3 import_to_notion.py <NOTION_TOKEN>")
        print("\nГде:")
        print("  NOTION_TOKEN - API токен Notion (получить на https://www.notion.so/my-integrations)")
        print("\nПример:")
        print("  python3 import_to_notion.py secret_xxx")
        print("\nДля справки: python3 import_to_notion.py --help")
        sys.exit(1)
    
    # Создаем импортер
    importer = NotionImporter(notion_token, max_concurrent=max_concurrent)
    
    # Интерактивный запрос Database ID
    print("=" * 80)
    print("📥 Импорт статей в Notion Database")
    print("=" * 80)
    
    # Извлекаем Database ID из URL, если передан URL
    if database_id:
        database_id = extract_database_id(database_id)
    
    # Если database_id не указан, запрашиваем у пользователя
    if not database_id:
        print("\nВведите Database ID или URL Database для импорта данных.")
        print("Вы можете ввести:")
        print("  - Database ID: abc123def456ghi789jkl012mno345pq")
        print("  - Полный URL: https://www.notion.so/workspace/abc123def456ghi789jkl012mno345pq?v=...")
        print()
        print("Database ID можно найти в URL вашей Database:")
        print("  https://www.notion.so/workspace/abc123def456ghi789jkl012mno345pq?v=...")
        print("  Database ID: abc123def456ghi789jkl012mno345pq (часть между последним '/' и '?')")
        print()
        user_input = input("Database ID или URL: ").strip()
        
        if not user_input:
            print("Ошибка: Database ID или URL не может быть пустым")
            sys.exit(1)
        
        # Извлекаем Database ID из введенного значения
        database_id = extract_database_id(user_input)
        
        if not database_id or len(database_id.replace('-', '')) != 32:
            print(f"Ошибка: Не удалось извлечь Database ID из введенного значения: {user_input}")
            print("Убедитесь, что вы ввели правильный Database ID или URL Database")
            sys.exit(1)
    
    # Получаем структуру Database
    print("\n🔍 Получение структуры Database...")
    try:
        structure = importer.get_database_structure(database_id)
        importer.database_id = database_id
        importer.database_properties = structure['properties']
    except Exception as e:
        print(f"❌ Ошибка при получении структуры Database: {e}")
        sys.exit(1)
    
    # Отображаем структуру Database
    importer.display_database_structure(structure)
    
    # Объясняем маппинг данных
    field_mapping = importer.explain_mapping(structure)
    importer.display_mapping(field_mapping, structure)
    
    # Запрашиваем подтверждение
    print("\n" + "=" * 80)
    if not get_user_confirmation("Продолжить импорт?", default=False):
        print("Импорт отменен пользователем.")
        sys.exit(0)
    
    # Пути к файлам
    markdown_dir = 'articles_markdown'
    json_file = 'parsed_articles.json'
    
    # Запускаем импорт
    print("\n🚀 Начинаем импорт...")
    importer.import_from_directory(markdown_dir, json_file, field_mapping, use_async=use_async)


if __name__ == "__main__":
    main()
