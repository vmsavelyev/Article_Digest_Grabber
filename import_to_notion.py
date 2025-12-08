#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для импорта markdown файлов статей в Notion Database
"""

import os
import json
import re
import sys
from pathlib import Path
from notion_client import Client
from datetime import datetime


class NotionImporter:
    """Класс для импорта статей в Notion"""
    
    def __init__(self, notion_token: str, database_id: str):
        """
        Инициализация импортера
        
        Args:
            notion_token: API токен Notion (получить на https://www.notion.so/my-integrations)
            database_id: ID базы данных Notion (извлекается из URL)
        """
        self.notion = Client(auth=notion_token)
        self.database_id = database_id
    
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
        """Конвертирует markdown в блоки Notion"""
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
    
    def create_page(self, article_data: dict) -> str:
        """Создает страницу в Notion Database"""
        # Подготовка properties
        title_text = article_data.get('title') or "Без заголовка"
        
        # Notion API поддерживает заголовки до 2000 символов в одном text объекте
        # Используем полный заголовок без обрезки
        properties = {
            "Name": {
                "title": [
                    {
                        "type": "text",
                        "text": {
                            "content": title_text
                        }
                    }
                ]
            }
        }
        
        # URL property
        if article_data.get('url'):
            properties["URL"] = {
                "url": article_data['url']
            }
        
        # Дата публикации property
        if article_data.get('date'):
            date_obj = self.parse_date(article_data['date'])
            if date_obj:
                properties["Дата публикации"] = {
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
    
    def import_from_directory(self, markdown_dir: str, json_file: str = None):
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
        print("=" * 80)
        
        imported = 0
        errors = 0
        
        for i, md_file in enumerate(md_files, 1):
            print(f"\n[{i}/{len(md_files)}] Обрабатываю: {md_file.name}")
            
            try:
                # Парсим markdown файл
                article_data = self.parse_markdown_file(str(md_file))
                
                # Если есть метаданные из JSON, используем их (они более точные)
                # Сопоставляем по номеру файла или URL
                file_num = re.match(r'^(\d+)_', md_file.name)
                if file_num and json_data:
                    file_index = int(file_num.group(1)) - 1
                    if 0 <= file_index < len(json_data):
                        json_article = json_data[file_index]
                        # Используем заголовок из JSON (он полный и точный)
                        if json_article.get('title'):
                            article_data['title'] = json_article['title']
                        if json_article.get('date'):
                            article_data['date'] = json_article['date']
                        if json_article.get('url'):
                            article_data['url'] = json_article['url']
                elif article_data.get('url') and article_data['url'] in articles_metadata:
                    json_article = articles_metadata[article_data['url']]
                    # Используем заголовок из JSON (он полный и точный)
                    if json_article.get('title'):
                        article_data['title'] = json_article['title']
                    if json_article.get('date'):
                        article_data['date'] = json_article['date']
                    if json_article.get('url'):
                        article_data['url'] = json_article['url']
                
                # Проверяем, что есть необходимые данные
                if not article_data.get('title'):
                    print(f"  ⚠ Предупреждение: заголовок не найден, пропускаю файл")
                    errors += 1
                    continue
                
                # Отладочный вывод (показываем полный заголовок)
                title = article_data.get('title', '')
                title_len = len(title)
                print(f"  Заголовок ({title_len} символов): {title}")
                
                # Проверяем, что заголовок не обрезан (если он короче 50 символов, возможно проблема)
                if title_len < 50 and '...' in title:
                    print(f"  ⚠ Предупреждение: заголовок может быть обрезан")
                
                # Создаем страницу в Notion
                page_id = self.create_page(article_data)
                print(f"  ✓ Страница создана (ID: {page_id[:8]}...)")
                imported += 1
                
            except Exception as e:
                print(f"  ✗ Ошибка: {e}")
                errors += 1
        
        print("\n" + "=" * 80)
        print(f"Импорт завершен!")
        print(f"Успешно импортировано: {imported}")
        print(f"Ошибок: {errors}")


def main():
    """Основная функция"""
    # Получаем NOTION_TOKEN: приоритет у аргументов командной строки, затем переменные окружения
    notion_token = None
    if len(sys.argv) >= 2:
        notion_token = sys.argv[1]
    if not notion_token:
        notion_token = os.getenv('NOTION_TOKEN')
    
    # Получаем DATABASE_ID: приоритет у аргументов командной строки, затем переменные окружения
    database_id = None
    if len(sys.argv) >= 3:
        database_id = sys.argv[2]
    if not database_id:
        database_id = os.getenv('NOTION_DATABASE_ID')
    
    # Проверяем наличие обязательных параметров
    if not notion_token:
        print("Ошибка: NOTION_TOKEN не указан")
        print("\nИспользование:")
        print("  1. Через переменные окружения:")
        print("     export NOTION_TOKEN='your_token'")
        print("     export NOTION_DATABASE_ID='your_database_id'")
        print("     python3 import_to_notion.py")
        print("\n  2. Через аргументы командной строки:")
        print("     python3 import_to_notion.py <NOTION_TOKEN> <DATABASE_ID>")
        print("\n  3. Комбинированный способ:")
        print("     export NOTION_DATABASE_ID='your_database_id'")
        print("     python3 import_to_notion.py <NOTION_TOKEN>")
        sys.exit(1)
    
    if not database_id:
        print("Ошибка: DATABASE_ID не указан")
        print("\nИспользование:")
        print("  1. Через переменные окружения:")
        print("     export NOTION_TOKEN='your_token'")
        print("     export NOTION_DATABASE_ID='your_database_id'")
        print("     python3 import_to_notion.py")
        print("\n  2. Через аргументы командной строки:")
        print("     python3 import_to_notion.py <NOTION_TOKEN> <DATABASE_ID>")
        print("\n  3. Комбинированный способ:")
        print("     export NOTION_TOKEN='your_token'")
        print("     python3 import_to_notion.py <DATABASE_ID>")
        print("\nГде:")
        print("  NOTION_TOKEN - API токен Notion (получить на https://www.notion.so/my-integrations)")
        print("  DATABASE_ID - ID базы данных (извлекается из URL базы данных)")
        print("\nПример:")
        print("  python3 import_to_notion.py secret_xxx abc123def456ghi789jkl012mno345pq")
        sys.exit(1)
    
    # Пути к файлам
    markdown_dir = 'articles_markdown'
    json_file = 'parsed_articles.json'
    
    # Создаем импортер и запускаем импорт
    importer = NotionImporter(notion_token, database_id)
    importer.import_from_directory(markdown_dir, json_file)


if __name__ == "__main__":
    main()
