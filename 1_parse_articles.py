#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для парсинга статей с разных сайтов
Извлекает заголовок, дату публикации, текст и изображения из статей
"""

import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import json
import sys
import re
import os
import shutil
import time
import random
from datetime import datetime
from typing import List, Dict, Optional


class ArticleParser:
    """Класс для парсинга статей с разных сайтов"""
    
    def __init__(self, max_concurrent: int = 10, retry_count: int = 3, min_delay: float = 0.5, max_delay: float = 2.0):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
        }
        self.max_concurrent = max_concurrent
        self.retry_count = retry_count
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.session = None
    
    def detect_site_type(self, url: str) -> str:
        """Определяет тип сайта по URL"""
        domain = urlparse(url).netloc.lower()

        if 'vc.ru' in domain:
            return 'vcru'
        elif 'techcrunch.com' in domain:
            return 'techcrunch'
        elif 'habr.com' in domain:
            return 'habr'
        elif 'crunchbase.com' in domain:
            return 'crunchbase'
        elif 'infoq.com' in domain:
            return 'infoq'
        else:
            return 'unknown'
    
    def format_date(self, datetime_str: str) -> Optional[str]:
        """Преобразует дату из различных форматов в DD.MM.YYYY"""
        if not datetime_str:
            return None
        
        try:
            # Очищаем строку от лишних символов
            cleaned = datetime_str.strip()
            
            # Удаляем 'Z' в конце, если есть
            if cleaned.endswith('Z'):
                cleaned = cleaned[:-1]
            
            # Обрабатываем формат с временной зоной (например, -08:00)
            if '+' in cleaned or (cleaned.count('-') > 2):
                # Разделяем по '+' или последнему '-'
                if '+' in cleaned:
                    cleaned = cleaned.split('+')[0]
                else:
                    # Находим последний '-' который разделяет дату/время и временную зону
                    parts = cleaned.rsplit('-', 1)
                    if len(parts) == 2 and ':' in parts[1]:
                        cleaned = parts[0]
            
            # Пробуем различные форматы даты
            formats = [
                '%Y-%m-%dT%H:%M:%S.%f',  # 2025-11-10T19:24:46.000
                '%Y-%m-%dT%H:%M:%S',     # 2025-11-10T19:24:46
                '%Y-%m-%d',              # 2025-11-10
                '%B %d, %Y',             # January 22, 2026
                '%b %d, %Y',             # Jan 22, 2026
                '%B %d %Y',              # January 22 2026
                '%b %d %Y',              # Jan 22 2026
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(cleaned, fmt)
                    return dt.strftime('%d.%m.%Y')
                except ValueError:
                    continue
            
            # Если не удалось распарсить, возвращаем None
            return None
        except Exception:
            return None
    
    def parse_vcru(self, soup: BeautifulSoup, url: str) -> Dict:
        """Парсинг статей с vc.ru"""
        result = {
            'title': None,
            'date': None,
            'text': '',
            'images': []
        }
        
        # Извлечение заголовка
        title_tag = soup.find('h1', class_=lambda x: x and 'content-title' in x)
        if title_tag:
            # Создаем копию для безопасного удаления элементов
            title_copy = BeautifulSoup(str(title_tag), 'html.parser')
            # Удаляем только иконку редакции (SVG и span с иконкой), но сохраняем весь текст заголовка
            # Текст внутри content-title__editorial является частью заголовка
            for icon in title_copy.find_all('span', class_='content-title__editorial-icon'):
                icon.decompose()
            # Также удаляем SVG элементы (иконки)
            for svg in title_copy.find_all('svg'):
                svg.decompose()
            # Удаляем use элементы (ссылки на SVG спрайты)
            for use in title_copy.find_all('use'):
                use.decompose()
            # Извлекаем весь текст рекурсивно, включая текст внутри всех вложенных элементов
            # Используем separator=' ' чтобы правильно объединить текст из разных элементов
            result['title'] = title_copy.get_text(separator=' ', strip=True)
            # Убираем лишние пробелы (двойные, тройные и т.д.)
            result['title'] = re.sub(r'\s+', ' ', result['title']).strip()
        
        # Извлечение даты публикации
        time_tag = soup.find('time', datetime=True)
        if time_tag:
            datetime_str = time_tag.get('datetime')
            if datetime_str:
                result['date'] = self.format_date(datetime_str)
        
        # Извлечение тела статьи
        article_tag = soup.find('article', class_='content__blocks')
        if article_tag:
            # Удаляем скрипты и стили
            for script in article_tag.find_all(['script', 'style']):
                script.decompose()
            
            # Извлекаем текст из всех блоков с текстом
            text_blocks = []
            
            # Ищем блоки с текстом (block-text, block-list)
            for block_wrapper in article_tag.find_all('figure', class_='block-wrapper'):
                # Текстовые блоки
                block_text = block_wrapper.find('div', class_='block-text')
                if block_text:
                    for p in block_text.find_all('p'):
                        # Используем separator=' ' для сохранения пробелов между словами
                        # Это важно, когда текст разбит ссылками или другими элементами
                        text = p.get_text(separator=' ', strip=True)
                        # Убираем множественные пробелы, но сохраняем одиночные
                        text = re.sub(r'\s+', ' ', text).strip()
                        if text:
                            text_blocks.append(text)
                
                # Списки
                block_list = block_wrapper.find('ul', class_='block-list')
                if block_list:
                    for li in block_list.find_all('li'):
                        # Используем separator=' ' для сохранения пробелов между словами
                        text = li.get_text(separator=' ', strip=True)
                        # Убираем множественные пробелы
                        text = re.sub(r'\s+', ' ', text).strip()
                        if text:
                            text_blocks.append(text)
            
            result['text'] = '\n\n'.join(text_blocks)
            
            # Извлечение изображений из блока block-media
            for block_media in article_tag.find_all('div', class_='block-media'):
                for img in block_media.find_all('img'):
                    img_src = img.get('src') or img.get('data-src')
                    if img_src:
                        # Преобразуем относительные URL в абсолютные
                        if img_src.startswith('//'):
                            img_src = 'https:' + img_src
                        elif img_src.startswith('/'):
                            img_src = urljoin(url, img_src)
                        elif not img_src.startswith('http'):
                            img_src = urljoin(url, img_src)
                        
                        # Пропускаем base64 изображения
                        if img_src.startswith('data:'):
                            continue
                        
                        # Проверяем на дубликаты
                        if any(existing_img['url'] == img_src for existing_img in result['images']):
                            continue
                        
                        img_alt = img.get('alt', '')
                        # Также проверяем заголовок изображения
                        media_title = block_media.find('div', class_='media-title')
                        if media_title:
                            img_alt = media_title.get_text(strip=True) or img_alt
                        
                        result['images'].append({
                            'url': img_src,
                            'alt': img_alt
                        })
        
        return result
    
    def parse_techcrunch(self, soup: BeautifulSoup, url: str) -> Dict:
        """Парсинг статей с techcrunch.com"""
        result = {
            'title': None,
            'date': None,
            'text': '',
            'images': []
        }
        
        # Извлечение заголовка
        title_tag = soup.find('h1', class_='wp-block-post-title')
        if title_tag:
            result['title'] = title_tag.get_text(strip=True)
        
        # Извлечение даты публикации
        date_div = soup.find('div', class_='wp-block-post-date')
        if date_div:
            time_tag = date_div.find('time', datetime=True)
            if time_tag:
                datetime_str = time_tag.get('datetime')
                if datetime_str:
                    result['date'] = self.format_date(datetime_str)
        
        # Извлечение featured image (главное изображение статьи)
        featured_figure = soup.find('figure', class_='wp-block-post-featured-image')
        if featured_figure:
            featured_img = featured_figure.find('img')
            if featured_img:
                # Получаем URL изображения (предпочитаем src, затем srcset)
                img_src = featured_img.get('src')
                if not img_src:
                    srcset = featured_img.get('srcset')
                    if srcset:
                        # Берем первый URL из srcset (обычно самый маленький)
                        # или ищем версию без resize параметров
                        srcset_parts = srcset.split(',')
                        for part in srcset_parts:
                            part_url = part.strip().split()[0]
                            if 'resize' not in part_url:
                                img_src = part_url
                                break
                        if not img_src:
                            img_src = srcset_parts[0].strip().split()[0]
                
                if img_src:
                    if img_src.startswith('//'):
                        img_src = 'https:' + img_src
                    elif img_src.startswith('/'):
                        img_src = urljoin(url, img_src)
                    elif not img_src.startswith('http'):
                        img_src = urljoin(url, img_src)
                    
                    if not img_src.startswith('data:'):
                        img_alt = featured_img.get('alt', '')
                        # Проверяем подпись к изображению
                        figcaption = featured_figure.find('figcaption')
                        if figcaption:
                            img_alt = figcaption.get_text(strip=True) or img_alt
                        
                        result['images'].append({
                            'url': img_src,
                            'alt': img_alt
                        })
        
        # Извлечение тела статьи
        content_div = soup.find('div', class_='entry-content')
        if content_div:
            # Удаляем рекламные блоки
            for ad in content_div.find_all(['div'], class_=lambda x: x and 'ad-unit' in str(x)):
                ad.decompose()
            
            # Удаляем скрипты и стили
            for script in content_div.find_all(['script', 'style']):
                script.decompose()
            
            # Извлекаем текст из параграфов
            text_blocks = []
            for p in content_div.find_all('p'):
                # Используем separator=' ' для сохранения пробелов между словами
                text = p.get_text(separator=' ', strip=True)
                # Убираем множественные пробелы
                text = re.sub(r'\s+', ' ', text).strip()
                if text and len(text) > 10:  # Игнорируем очень короткие тексты
                    text_blocks.append(text)
            
            result['text'] = '\n\n'.join(text_blocks)
            
            # Извлечение изображений
            # Сначала проверяем picture элементы
            for picture in content_div.find_all('picture'):
                img = picture.find('img')
                if img:
                    img_src = (img.get('src') or 
                              img.get('data-src') or 
                              img.get('data-lazy-src') or 
                              img.get('data-original'))
                    
                    # Если нет src, проверяем source внутри picture
                    if not img_src:
                        source = picture.find('source')
                        if source:
                            srcset = source.get('srcset') or source.get('src')
                            if srcset:
                                # Берем первый URL из srcset
                                img_src = str(srcset).split()[0] if ' ' in str(srcset) else srcset
                    
                    if img_src:
                        if img_src.startswith('//'):
                            img_src = 'https:' + img_src
                        elif img_src.startswith('/'):
                            img_src = urljoin(url, img_src)
                        elif not img_src.startswith('http'):
                            img_src = urljoin(url, img_src)
                        
                        if not img_src.startswith('data:'):
                            if not any(existing_img['url'] == img_src for existing_img in result['images']):
                                img_alt = img.get('alt', '')
                                result['images'].append({
                                    'url': img_src,
                                    'alt': img_alt
                                })
            
            # Затем проверяем обычные img элементы
            for img in content_div.find_all('img'):
                # Проверяем различные атрибуты для получения URL изображения
                img_src = (img.get('src') or 
                          img.get('data-src') or 
                          img.get('data-lazy-src') or 
                          img.get('data-original') or
                          img.get('data-lazy-loaded'))
                
                # Если srcset, берем первый URL
                if not img_src:
                    srcset = img.get('srcset')
                    if srcset:
                        img_src = str(srcset).split()[0] if ' ' in str(srcset) else srcset
                
                if img_src:
                    # Преобразуем относительные URL в абсолютные
                    if img_src.startswith('//'):
                        img_src = 'https:' + img_src
                    elif img_src.startswith('/'):
                        img_src = urljoin(url, img_src)
                    elif not img_src.startswith('http'):
                        img_src = urljoin(url, img_src)
                    
                    # Пропускаем base64 изображения
                    if img_src.startswith('data:'):
                        continue
                    
                    # Проверяем на дубликаты
                    if any(existing_img['url'] == img_src for existing_img in result['images']):
                        continue
                    
                    img_alt = img.get('alt', '')
                    # Проверяем подпись к изображению
                    parent = img.find_parent(['figure', 'div'])
                    if parent:
                        figcaption = parent.find('figcaption')
                        if figcaption:
                            img_alt = figcaption.get_text(strip=True) or img_alt
                    
                    result['images'].append({
                        'url': img_src,
                        'alt': img_alt
                    })
        
        return result
    
    def parse_habr(self, soup: BeautifulSoup, url: str) -> Dict:
        """Парсинг статей с habr.com"""
        result = {
            'title': None,
            'date': None,
            'text': '',
            'images': []
        }
        
        # Извлечение заголовка
        title_tag = soup.find('h1', class_='tm-title')
        if title_tag:
            # Извлекаем текст из span внутри h1
            span = title_tag.find('span')
            if span:
                result['title'] = span.get_text(strip=True)
            else:
                result['title'] = title_tag.get_text(strip=True)
        
        # Извлечение даты публикации
        date_span = soup.find('span', class_='tm-article-datetime-published')
        if date_span:
            time_tag = date_span.find('time', datetime=True)
            if time_tag:
                datetime_str = time_tag.get('datetime')
                if datetime_str:
                    result['date'] = self.format_date(datetime_str)
        
        # Извлечение тела статьи
        content_div = soup.find('div', id='post-content-body')
        if content_div:
            # Удаляем скрипты и стили
            for script in content_div.find_all(['script', 'style']):
                script.decompose()
            
            # Извлекаем текст из параграфов
            text_blocks = []
            for p in content_div.find_all('p'):
                # Используем separator=' ' для сохранения пробелов между словами
                text = p.get_text(separator=' ', strip=True)
                # Убираем множественные пробелы
                text = re.sub(r'\s+', ' ', text).strip()
                if text and len(text) > 5:
                    text_blocks.append(text)
            
            result['text'] = '\n\n'.join(text_blocks)
            
            # Извлечение изображений (включая picture с source)
            for img in content_div.find_all('img'):
                img_src = img.get('src') or img.get('data-src')
                
                # Если изображение внутри picture, проверяем source
                if not img_src:
                    picture = img.find_parent('picture')
                    if picture:
                        source = picture.find('source')
                        if source:
                            srcset = source.get('srcset', '')
                            if srcset:
                                # Берем первый URL из srcset
                                img_src = srcset.split()[0]
                
                if img_src:
                    # Преобразуем относительные URL в абсолютные
                    if img_src.startswith('//'):
                        img_src = 'https:' + img_src
                    elif img_src.startswith('/'):
                        img_src = urljoin(url, img_src)
                    elif not img_src.startswith('http'):
                        img_src = urljoin(url, img_src)
                    
                    # Пропускаем base64 изображения
                    if img_src.startswith('data:'):
                        continue
                    
                    # Проверяем на дубликаты
                    if any(existing_img['url'] == img_src for existing_img in result['images']):
                        continue
                    
                    img_alt = img.get('alt', '')
                    result['images'].append({
                        'url': img_src,
                        'alt': img_alt
                    })

        return result

    def parse_crunchbase(self, soup: BeautifulSoup, url: str) -> Dict:
        """Парсинг статей с news.crunchbase.com"""
        result = {
            'title': None,
            'date': None,
            'text': '',
            'images': []
        }

        # Извлечение заголовка
        title_tag = soup.find('h1', class_='entry-title')
        if title_tag:
            result['title'] = title_tag.get_text(strip=True)

        # Извлечение даты публикации из <span class="updated">
        date_span = soup.find('span', class_='updated')
        if date_span:
            date_text = date_span.get_text(strip=True)
            if date_text:
                result['date'] = self.format_date(date_text)

        # Извлечение тела статьи из herald-entry-content
        content_div = soup.find('div', class_='herald-entry-content')
        if content_div:
            # Удаляем рекламные блоки herald-ad
            for ad in content_div.find_all('div', class_='herald-ad'):
                ad.decompose()

            # Удаляем скрипты и стили
            for script in content_div.find_all(['script', 'style']):
                script.decompose()

            # Удаляем формы (например, iterable подписки)
            for form in content_div.find_all('form'):
                form.decompose()

            # Извлекаем текст из параграфов
            text_blocks = []
            for p in content_div.find_all('p'):
                text = p.get_text(separator=' ', strip=True)
                text = re.sub(r'\s+', ' ', text).strip()
                if text and len(text) > 10:
                    text_blocks.append(text)

            result['text'] = '\n\n'.join(text_blocks)

            # Извлечение изображений
            for img in content_div.find_all('img'):
                img_src = img.get('src') or img.get('data-src')
                if img_src:
                    if img_src.startswith('//'):
                        img_src = 'https:' + img_src
                    elif img_src.startswith('/'):
                        img_src = urljoin(url, img_src)
                    elif not img_src.startswith('http'):
                        img_src = urljoin(url, img_src)

                    if img_src.startswith('data:'):
                        continue

                    if any(existing_img['url'] == img_src for existing_img in result['images']):
                        continue

                    img_alt = img.get('alt', '')
                    result['images'].append({
                        'url': img_src,
                        'alt': img_alt
                    })

        return result

    def parse_infoq(self, soup: BeautifulSoup, url: str) -> Dict:
        """Парсинг статей с www.infoq.com"""
        result = {
            'title': None,
            'date': None,
            'text': '',
            'images': []
        }

        # Извлечение заголовка
        title_tag = soup.find('h1', class_='article__title')
        if not title_tag:
            title_tag = soup.find('h1')
        if title_tag:
            result['title'] = title_tag.get_text(strip=True)

        # Извлечение даты из <p class="article__readTime date">
        date_p = soup.find('p', class_='article__readTime')
        if date_p:
            # Извлекаем только текст даты (до span с dot)
            date_text = ''
            for child in date_p.children:
                if hasattr(child, 'name'):
                    if child.name == 'span' and 'dot' in child.get('class', []):
                        break
                else:
                    date_text += str(child)
            date_text = date_text.strip()
            if date_text:
                result['date'] = self.format_date(date_text)

        # Извлечение тела статьи из article__data
        content_div = soup.find('div', class_='article__data')
        if content_div:
            # Удаляем скрипты и стили
            for script in content_div.find_all(['script', 'style']):
                script.decompose()

            # Удаляем рекламные блоки
            for ad in content_div.find_all('div', class_=lambda x: x and 'ad' in x.lower()):
                ad.decompose()

            # Извлекаем текст из параграфов
            text_blocks = []
            for p in content_div.find_all('p'):
                text = p.get_text(separator=' ', strip=True)
                text = re.sub(r'\s+', ' ', text).strip()
                if text and len(text) > 10:
                    text_blocks.append(text)

            result['text'] = '\n\n'.join(text_blocks)

            # Извлечение изображений (включая из параграфов)
            for img in content_div.find_all('img'):
                img_src = (img.get('src') or
                          img.get('data-src') or
                          img.get('data-lazy-src') or
                          img.get('data-original'))
                if img_src:
                    if img_src.startswith('//'):
                        img_src = 'https:' + img_src
                    elif img_src.startswith('/'):
                        img_src = urljoin(url, img_src)
                    elif not img_src.startswith('http'):
                        img_src = urljoin(url, img_src)

                    if img_src.startswith('data:'):
                        continue

                    if any(existing_img['url'] == img_src for existing_img in result['images']):
                        continue

                    img_alt = img.get('alt', '')
                    result['images'].append({
                        'url': img_src,
                        'alt': img_alt
                    })

        return result

    def extract_structured_content_vcru(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """Извлекает структурированный контент с vc.ru с сохранением порядка элементов"""
        content_items = []
        article_tag = soup.find('article', class_='content__blocks')
        
        if article_tag:
            for block_wrapper in article_tag.find_all('figure', class_='block-wrapper'):
                # Текстовые блоки
                block_text = block_wrapper.find('div', class_='block-text')
                if block_text:
                    for p in block_text.find_all('p'):
                        # Используем separator=' ' для сохранения пробелов между словами
                        text = p.get_text(separator=' ', strip=True)
                        # Убираем множественные пробелы
                        text = re.sub(r'\s+', ' ', text).strip()
                        if text:
                            content_items.append({'type': 'text', 'content': text})
                
                # Списки
                block_list = block_wrapper.find('ul', class_='block-list')
                if block_list:
                    list_items = []
                    for li in block_list.find_all('li'):
                        # Используем separator=' ' для сохранения пробелов между словами
                        text = li.get_text(separator=' ', strip=True)
                        # Убираем множественные пробелы
                        text = re.sub(r'\s+', ' ', text).strip()
                        if text:
                            list_items.append(text)
                    if list_items:
                        content_items.append({'type': 'list', 'content': list_items})
                
                # Изображения
                block_media = block_wrapper.find('div', class_='block-media')
                if block_media:
                    for img in block_media.find_all('img'):
                        img_src = img.get('src') or img.get('data-src')
                        if img_src:
                            if img_src.startswith('//'):
                                img_src = 'https:' + img_src
                            elif img_src.startswith('/'):
                                img_src = urljoin(url, img_src)
                            elif not img_src.startswith('http'):
                                img_src = urljoin(url, img_src)
                            
                            if not img_src.startswith('data:'):
                                img_alt = img.get('alt', '')
                                media_title = block_media.find('div', class_='media-title')
                                if media_title:
                                    img_alt = media_title.get_text(strip=True) or img_alt
                                content_items.append({'type': 'image', 'url': img_src, 'alt': img_alt})
        
        return content_items
    
    def extract_structured_content_techcrunch(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """Извлекает структурированный контент с techcrunch.com с сохранением порядка элементов"""
        content_items = []
        
        # Извлекаем featured image (главное изображение статьи) - добавляем в начало
        featured_figure = soup.find('figure', class_='wp-block-post-featured-image')
        if featured_figure:
            featured_img = featured_figure.find('img')
            if featured_img:
                img_src = featured_img.get('src')
                if not img_src:
                    srcset = featured_img.get('srcset')
                    if srcset:
                        srcset_parts = srcset.split(',')
                        for part in srcset_parts:
                            part_url = part.strip().split()[0]
                            if 'resize' not in part_url:
                                img_src = part_url
                                break
                        if not img_src:
                            img_src = srcset_parts[0].strip().split()[0]
                
                if img_src:
                    if img_src.startswith('//'):
                        img_src = 'https:' + img_src
                    elif img_src.startswith('/'):
                        img_src = urljoin(url, img_src)
                    elif not img_src.startswith('http'):
                        img_src = urljoin(url, img_src)
                    
                    if not img_src.startswith('data:'):
                        img_alt = featured_img.get('alt', '')
                        figcaption = featured_figure.find('figcaption')
                        if figcaption:
                            img_alt = figcaption.get_text(strip=True) or img_alt
                        
                        content_items.append({
                            'type': 'image',
                            'url': img_src,
                            'alt': img_alt
                        })
        
        content_div = soup.find('div', class_='entry-content')
        
        if content_div:
            # Сначала извлекаем изображения, потом удаляем рекламу
            # Это гарантирует, что мы не потеряем изображения контента
            
            # Проходим по всем элементам в порядке их появления в DOM
            # Добавляем уже обработанные изображения (featured image)
            processed_images = set()
            for item in content_items:
                if item.get('type') == 'image' and item.get('url'):
                    processed_images.add(item['url'])
            seen_texts = set()
            
            # Рекурсивная функция для обхода элементов в порядке появления
            def process_element(elem):
                if not hasattr(elem, 'name'):
                    return
                
                # Пропускаем рекламные блоки
                is_ad = False
                for parent in elem.parents:
                    if hasattr(parent, 'get'):
                        classes = parent.get('class', [])
                        if classes:
                            classes_str = ' '.join(classes) if isinstance(classes, list) else str(classes)
                            if 'ad-unit' in classes_str.lower():
                                is_ad = True
                                break
                
                if is_ad:
                    return
                
                # Обрабатываем параграфы
                if elem.name == 'p':
                    text = elem.get_text(separator=' ', strip=True)
                    text = re.sub(r'\s+', ' ', text).strip()
                    if text and len(text) > 10:
                        text_key = text[:100]
                        if text_key not in seen_texts:
                            seen_texts.add(text_key)
                            content_items.append({'type': 'text', 'content': text})
                    # Не обрабатываем дочерние элементы параграфа отдельно
                    return
                
                # Обрабатываем изображения
                elif elem.name == 'img':
                    # Проверяем все возможные атрибуты для получения URL изображения
                    img_src = (elem.get('src') or 
                              elem.get('data-src') or 
                              elem.get('data-lazy-src') or 
                              elem.get('data-original') or
                              elem.get('data-lazy-loaded') or
                              elem.get('srcset'))
                    
                    # Если srcset, берем первый URL
                    if img_src and ' ' in str(img_src):
                        img_src = str(img_src).split()[0]
                    
                    if img_src:
                        if img_src.startswith('//'):
                            img_src = 'https:' + img_src
                        elif img_src.startswith('/'):
                            img_src = urljoin(url, img_src)
                        elif not img_src.startswith('http'):
                            img_src = urljoin(url, img_src)
                        
                        if not img_src.startswith('data:') and img_src not in processed_images:
                            processed_images.add(img_src)
                            img_alt = elem.get('alt', '')
                            
                            # Проверяем подпись к изображению
                            parent = elem.find_parent(['figure', 'div'])
                            if parent:
                                figcaption = parent.find('figcaption')
                                if figcaption:
                                    img_alt = figcaption.get_text(strip=True) or img_alt
                            
                            content_items.append({
                                'type': 'image',
                                'url': img_src,
                                'alt': img_alt
                            })
                    return  # Не обрабатываем дочерние элементы изображения
                
                # Также проверяем picture элементы (они содержат source с изображениями)
                elif elem.name == 'picture':
                    img = elem.find('img')
                    if img:
                        img_src = (img.get('src') or 
                                  img.get('data-src') or 
                                  img.get('data-lazy-src') or 
                                  img.get('data-original'))
                        
                        # Если нет src, проверяем source внутри picture
                        if not img_src:
                            source = elem.find('source')
                            if source:
                                img_src = source.get('srcset') or source.get('src')
                                if img_src and ' ' in str(img_src):
                                    img_src = str(img_src).split()[0]
                        
                        if img_src:
                            if img_src.startswith('//'):
                                img_src = 'https:' + img_src
                            elif img_src.startswith('/'):
                                img_src = urljoin(url, img_src)
                            elif not img_src.startswith('http'):
                                img_src = urljoin(url, img_src)
                            
                            if not img_src.startswith('data:') and img_src not in processed_images:
                                processed_images.add(img_src)
                                img_alt = img.get('alt', '')
                                content_items.append({
                                    'type': 'image',
                                    'url': img_src,
                                    'alt': img_alt
                                })
                    return  # Не обрабатываем дочерние элементы picture
                
                # Рекурсивно обрабатываем дочерние элементы для других типов элементов
                if hasattr(elem, 'children'):
                    for child in elem.children:
                        process_element(child)
            
            # Обрабатываем все дочерние элементы контента
            for child in content_div.children:
                process_element(child)
            
            # Дополнительная проверка: ищем все изображения напрямую (на случай, если они пропущены)
            # Это гарантирует, что мы найдем изображения даже если они в нестандартной структуре
            # Сначала проверяем picture элементы
            all_pictures = content_div.find_all('picture')
            for picture in all_pictures:
                img = picture.find('img')
                if img:
                    img_src = (img.get('src') or 
                              img.get('data-src') or 
                              img.get('data-lazy-src') or 
                              img.get('data-original'))
                    
                    if not img_src:
                        source = picture.find('source')
                        if source:
                            srcset = source.get('srcset') or source.get('src')
                            if srcset:
                                img_src = str(srcset).split()[0] if ' ' in str(srcset) else srcset
                    
                    if img_src:
                        if img_src.startswith('//'):
                            img_src = 'https:' + img_src
                        elif img_src.startswith('/'):
                            img_src = urljoin(url, img_src)
                        elif not img_src.startswith('http'):
                            img_src = urljoin(url, img_src)
                        
                        if not img_src.startswith('data:') and img_src not in processed_images:
                            already_added = any(item.get('type') == 'image' and item.get('url') == img_src 
                                              for item in content_items)
                            if not already_added:
                                processed_images.add(img_src)
                                img_alt = img.get('alt', '')
                                content_items.append({
                                    'type': 'image',
                                    'url': img_src,
                                    'alt': img_alt
                                })
            
            # Затем проверяем все img элементы
            all_imgs_in_content = content_div.find_all('img')
            for img in all_imgs_in_content:
                # Проверяем, что изображение не в рекламном блоке
                is_ad_img = False
                for parent in img.parents:
                    if hasattr(parent, 'get'):
                        classes = parent.get('class', [])
                        if classes:
                            classes_str = ' '.join(classes) if isinstance(classes, list) else str(classes)
                            if 'ad-unit' in classes_str.lower():
                                is_ad_img = True
                                break
                
                if not is_ad_img:
                    img_src = (img.get('src') or 
                              img.get('data-src') or 
                              img.get('data-lazy-src') or 
                              img.get('data-original') or
                              img.get('data-lazy-loaded'))
                    
                    if img_src:
                        if img_src.startswith('//'):
                            img_src = 'https:' + img_src
                        elif img_src.startswith('/'):
                            img_src = urljoin(url, img_src)
                        elif not img_src.startswith('http'):
                            img_src = urljoin(url, img_src)
                        
                        if not img_src.startswith('data:') and img_src not in processed_images:
                            # Проверяем, не добавлено ли уже это изображение
                            already_added = any(item.get('type') == 'image' and item.get('url') == img_src 
                                              for item in content_items)
                            if not already_added:
                                processed_images.add(img_src)
                                img_alt = img.get('alt', '')
                                
                                # Проверяем подпись к изображению
                                parent = img.find_parent(['figure', 'div'])
                                if parent:
                                    figcaption = parent.find('figcaption')
                                    if figcaption:
                                        img_alt = figcaption.get_text(strip=True) or img_alt
                                
                                # Находим позицию для вставки (после последнего текстового элемента перед этим изображением)
                                # Для простоты добавляем в конец, но можно улучшить сортировку
                                content_items.append({
                                    'type': 'image',
                                    'url': img_src,
                                    'alt': img_alt
                                })
        
        return content_items
    
    def extract_structured_content_habr(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """Извлекает структурированный контент с habr.com с сохранением порядка элементов"""
        content_items = []
        content_div = soup.find('div', id='post-content-body')
        
        if content_div:
            # Проходим по всем элементам в порядке их появления
            for element in content_div.descendants:
                if hasattr(element, 'name'):
                    if element.name == 'p' and element.parent.name == 'div':
                        # Используем separator=' ' для сохранения пробелов между словами
                        text = element.get_text(separator=' ', strip=True)
                        # Убираем множественные пробелы
                        text = re.sub(r'\s+', ' ', text).strip()
                        if text and len(text) > 5:
                            # Проверяем, не дублируем ли мы уже этот текст
                            if not content_items or content_items[-1]['type'] != 'text' or content_items[-1]['content'] != text:
                                content_items.append({'type': 'text', 'content': text})
                    elif element.name == 'img':
                        img_src = element.get('src') or element.get('data-src')
                        if not img_src:
                            picture = element.find_parent('picture')
                            if picture:
                                source = picture.find('source')
                                if source:
                                    srcset = source.get('srcset', '')
                                    if srcset:
                                        img_src = srcset.split()[0]
                        
                        if img_src:
                            if img_src.startswith('//'):
                                img_src = 'https:' + img_src
                            elif img_src.startswith('/'):
                                img_src = urljoin(url, img_src)
                            elif not img_src.startswith('http'):
                                img_src = urljoin(url, img_src)
                            
                            if not img_src.startswith('data:'):
                                img_alt = element.get('alt', '')
                                # Проверяем на дубликаты
                                if not any(item.get('url') == img_src for item in content_items if item.get('type') == 'image'):
                                    content_items.append({'type': 'image', 'url': img_src, 'alt': img_alt})

        return content_items

    def extract_structured_content_crunchbase(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """Извлекает структурированный контент с news.crunchbase.com с сохранением порядка элементов"""
        content_items = []
        content_div = soup.find('div', class_='herald-entry-content')

        if content_div:
            # Удаляем рекламные блоки herald-ad
            for ad in content_div.find_all('div', class_='herald-ad'):
                ad.decompose()

            # Удаляем скрипты и стили
            for script in content_div.find_all(['script', 'style']):
                script.decompose()

            # Удаляем формы подписки
            for form in content_div.find_all('form'):
                form.decompose()

            processed_images = set()

            # Проходим по всем элементам в порядке их появления
            for element in content_div.descendants:
                if hasattr(element, 'name'):
                    if element.name == 'p':
                        text = element.get_text(separator=' ', strip=True)
                        text = re.sub(r'\s+', ' ', text).strip()
                        if text and len(text) > 10:
                            # Проверяем на дубликаты
                            if not content_items or content_items[-1].get('type') != 'text' or content_items[-1].get('content') != text:
                                content_items.append({'type': 'text', 'content': text})
                    elif element.name == 'img':
                        img_src = element.get('src') or element.get('data-src')
                        if img_src:
                            if img_src.startswith('//'):
                                img_src = 'https:' + img_src
                            elif img_src.startswith('/'):
                                img_src = urljoin(url, img_src)
                            elif not img_src.startswith('http'):
                                img_src = urljoin(url, img_src)

                            if not img_src.startswith('data:') and img_src not in processed_images:
                                processed_images.add(img_src)
                                img_alt = element.get('alt', '')
                                content_items.append({'type': 'image', 'url': img_src, 'alt': img_alt})

        return content_items

    def extract_structured_content_infoq(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """Извлекает структурированный контент с www.infoq.com с сохранением порядка элементов"""
        content_items = []
        content_div = soup.find('div', class_='article__data')

        if content_div:
            # Удаляем рекламные блоки
            for ad in content_div.find_all('div', class_=lambda x: x and 'ad' in x.lower()):
                ad.decompose()

            # Удаляем скрипты и стили
            for script in content_div.find_all(['script', 'style']):
                script.decompose()

            processed_images = set()
            seen_texts = set()

            def process_img(img_element):
                """Обрабатывает img элемент и добавляет в content_items"""
                img_src = (img_element.get('src') or
                          img_element.get('data-src') or
                          img_element.get('data-lazy-src') or
                          img_element.get('data-original'))
                if img_src:
                    if img_src.startswith('//'):
                        img_src = 'https:' + img_src
                    elif img_src.startswith('/'):
                        img_src = urljoin(url, img_src)
                    elif not img_src.startswith('http'):
                        img_src = urljoin(url, img_src)

                    if not img_src.startswith('data:') and img_src not in processed_images:
                        processed_images.add(img_src)
                        img_alt = img_element.get('alt', '')
                        content_items.append({'type': 'image', 'url': img_src, 'alt': img_alt})

            # Обрабатываем прямых потомков article__data для сохранения порядка
            for child in content_div.children:
                if not hasattr(child, 'name'):
                    continue

                if child.name == 'p':
                    # Сначала проверяем, есть ли изображения внутри параграфа
                    imgs_in_p = child.find_all('img')

                    # Извлекаем текст (без учёта alt текста изображений)
                    text_parts = []
                    for content in child.children:
                        if hasattr(content, 'name'):
                            if content.name == 'img':
                                # Добавляем изображение в нужном месте
                                if text_parts:
                                    text = ' '.join(text_parts)
                                    text = re.sub(r'\s+', ' ', text).strip()
                                    if text and len(text) > 10:
                                        text_key = text[:100]
                                        if text_key not in seen_texts:
                                            seen_texts.add(text_key)
                                            content_items.append({'type': 'text', 'content': text})
                                    text_parts = []
                                process_img(content)
                            else:
                                text_parts.append(content.get_text(separator=' ', strip=True))
                        else:
                            text_str = str(content).strip()
                            if text_str:
                                text_parts.append(text_str)

                    # Добавляем оставшийся текст после последнего изображения
                    if text_parts:
                        text = ' '.join(text_parts)
                        text = re.sub(r'\s+', ' ', text).strip()
                        if text and len(text) > 10:
                            text_key = text[:100]
                            if text_key not in seen_texts:
                                seen_texts.add(text_key)
                                content_items.append({'type': 'text', 'content': text})

                    # Если параграф не содержал изображений, обрабатываем как обычный текст
                    if not imgs_in_p:
                        text = child.get_text(separator=' ', strip=True)
                        text = re.sub(r'\s+', ' ', text).strip()
                        if text and len(text) > 10:
                            text_key = text[:100]
                            if text_key not in seen_texts:
                                seen_texts.add(text_key)
                                content_items.append({'type': 'text', 'content': text})

                elif child.name == 'img':
                    process_img(child)

                elif child.name in ['figure', 'div']:
                    # Ищем изображения внутри figure или div
                    for img in child.find_all('img'):
                        process_img(img)

        return content_items

    def article_content_to_notion_markdown(self, article_data: Dict, structured_content: List[Dict] = None) -> str:
        """Создает markdown контент только с телом статьи (без заголовка, URL, даты) для Notion"""
        md_lines = []
        
        # Контент (только тело статьи)
        if structured_content:
            # Используем структурированный контент с порядком элементов
            for item in structured_content:
                if item['type'] == 'text':
                    md_lines.append(f"{item['content']}\n\n")
                elif item['type'] == 'list':
                    for list_item in item['content']:
                        md_lines.append(f"- {list_item}\n")
                    md_lines.append("\n")
                elif item['type'] == 'image':
                    alt_text = item.get('alt', 'Изображение')
                    img_url = item.get('url', '')
                    md_lines.append(f"![{alt_text}]({img_url})\n\n")
        else:
            # Fallback: используем обычный текст и изображения
            if article_data.get('text'):
                md_lines.append(f"{article_data['text']}\n\n")
            
            if article_data.get('images'):
                for img in article_data['images']:
                    alt_text = img.get('alt', 'Изображение')
                    img_url = img.get('url', '')
                    md_lines.append(f"![{alt_text}]({img_url})\n\n")
        
        return ''.join(md_lines).strip()
    
    def article_to_markdown(self, article_data: Dict, structured_content: List[Dict] = None) -> str:
        """Преобразует данные статьи в markdown формат для Notion"""
        md_lines = []
        
        # Заголовок
        md_lines.append(f"# {article_data.get('title', 'Без заголовка')}\n")
        
        # Дата публикации
        if article_data.get('date'):
            md_lines.append(f"**Дата публикации:** {article_data['date']}\n")
        
        # URL источника
        md_lines.append(f"**Источник:** {article_data.get('url', '')}\n")
        md_lines.append("\n---\n\n")
        
        # Контент
        if structured_content:
            # Используем структурированный контент с порядком элементов
            for item in structured_content:
                if item['type'] == 'text':
                    md_lines.append(f"{item['content']}\n\n")
                elif item['type'] == 'list':
                    for list_item in item['content']:
                        md_lines.append(f"- {list_item}\n")
                    md_lines.append("\n")
                elif item['type'] == 'image':
                    alt_text = item.get('alt', 'Изображение')
                    img_url = item.get('url', '')
                    md_lines.append(f"![{alt_text}]({img_url})\n\n")
        else:
            # Fallback: используем обычный текст и изображения
            if article_data.get('text'):
                md_lines.append(f"{article_data['text']}\n\n")
            
            if article_data.get('images'):
                for img in article_data['images']:
                    alt_text = img.get('alt', 'Изображение')
                    img_url = img.get('url', '')
                    md_lines.append(f"![{alt_text}]({img_url})\n\n")
        
        return ''.join(md_lines)
    
    def sanitize_filename(self, filename: str, max_length: int = 100) -> str:
        """Создает безопасное имя файла из строки"""
        # Удаляем недопустимые символы для имен файлов
        filename = re.sub(r'[<>:"/\\|?*]', '', filename)
        # Заменяем пробелы на подчеркивания
        filename = filename.replace(' ', '_')
        # Удаляем множественные подчеркивания
        filename = re.sub(r'_+', '_', filename)
        # Обрезаем до максимальной длины
        if len(filename) > max_length:
            filename = filename[:max_length]
        return filename.strip('_')
    
    async def _fetch_url(self, session: aiohttp.ClientSession, url: str) -> bytes:
        """Асинхронно загружает страницу с retry логикой"""
        last_error = None

        for attempt in range(self.retry_count):
            try:
                # Добавляем случайную задержку перед запросом (кроме первой попытки)
                if attempt > 0:
                    delay = random.uniform(self.min_delay * (attempt + 1), self.max_delay * (attempt + 1))
                    print(f"  Повторная попытка {attempt + 1}/{self.retry_count} через {delay:.1f}с...")
                    await asyncio.sleep(delay)

                async with session.get(url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=15)) as response:
                    if response.status == 403:
                        last_error = aiohttp.ClientResponseError(
                            response.request_info,
                            response.history,
                            status=403,
                            message='Forbidden'
                        )
                        continue  # Пробуем ещё раз
                    response.raise_for_status()
                    return await response.read()

            except aiohttp.ClientResponseError as e:
                last_error = e
                if e.status != 403:
                    raise  # Другие ошибки пробрасываем сразу
            except asyncio.TimeoutError:
                last_error = asyncio.TimeoutError(f"Timeout при загрузке {url}")
                continue

        # Если все попытки исчерпаны
        if last_error:
            raise last_error
        raise aiohttp.ClientError(f"Не удалось загрузить {url} после {self.retry_count} попыток")
    
    async def parse_article_async(self, session: aiohttp.ClientSession, url: str) -> Dict:
        """Асинхронно парсит статью по URL"""
        print(f"Обрабатываю: {url}")
        
        try:
            content = await self._fetch_url(session, url)
            soup = BeautifulSoup(content, 'html.parser')
            site_type = self.detect_site_type(url)
            
            # Парсинг в зависимости от типа сайта
            if site_type == 'vcru':
                parsed_data = self.parse_vcru(soup, url)
                structured_content = self.extract_structured_content_vcru(soup, url)
            elif site_type == 'techcrunch':
                parsed_data = self.parse_techcrunch(soup, url)
                structured_content = self.extract_structured_content_techcrunch(soup, url)
            elif site_type == 'habr':
                parsed_data = self.parse_habr(soup, url)
                structured_content = self.extract_structured_content_habr(soup, url)
            elif site_type == 'crunchbase':
                parsed_data = self.parse_crunchbase(soup, url)
                structured_content = self.extract_structured_content_crunchbase(soup, url)
            elif site_type == 'infoq':
                parsed_data = self.parse_infoq(soup, url)
                structured_content = self.extract_structured_content_infoq(soup, url)
            else:
                # Универсальный парсинг для неизвестных сайтов
                parsed_data = self.parse_generic(soup, url)
                structured_content = None
            
            return {
                'url': url,
                'site_type': site_type,
                'title': parsed_data['title'] or 'Заголовок не найден',
                'date': parsed_data.get('date'),
                'text': parsed_data['text'] or 'Текст не найден',
                'images': parsed_data['images'],
                'structured_content': structured_content,
                'status': 'success'
            }
            
        except aiohttp.ClientError as e:
            print(f"  Ошибка при загрузке страницы: {e}")
            return {
                'url': url,
                'site_type': 'unknown',
                'title': 'Ошибка загрузки',
                'date': None,
                'text': f'Не удалось загрузить страницу: {str(e)}',
                'images': [],
                'status': 'error'
            }
        except Exception as e:
            print(f"  Ошибка при обработке: {e}")
            return {
                'url': url,
                'site_type': 'unknown',
                'title': 'Ошибка обработки',
                'date': None,
                'text': f'Ошибка при обработке статьи: {str(e)}',
                'images': [],
                'status': 'error'
            }
    
    def parse_article(self, url: str) -> Dict:
        """Синхронная обертка для обратной совместимости"""
        return asyncio.run(self._parse_article_sync(url))
    
    async def _parse_article_sync(self, url: str) -> Dict:
        """Вспомогательная функция для синхронной обертки"""
        async with aiohttp.ClientSession() as session:
            return await self.parse_article_async(session, url)
    
    async def parse_articles_batch(self, urls: List[str]) -> List[Dict]:
        """Асинхронно парсит список статей"""
        async with aiohttp.ClientSession() as session:
            semaphore = asyncio.Semaphore(self.max_concurrent)

            async def parse_with_semaphore(url: str) -> Dict:
                async with semaphore:
                    # Небольшая случайная задержка для имитации естественного поведения
                    await asyncio.sleep(random.uniform(0.1, 0.5))
                    return await self.parse_article_async(session, url)
            
            tasks = [parse_with_semaphore(url) for url in urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Обрабатываем исключения
            processed_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    print(f"  Критическая ошибка при обработке {urls[i]}: {result}")
                    processed_results.append({
                        'url': urls[i],
                        'site_type': 'unknown',
                        'title': 'Критическая ошибка',
                        'date': None,
                        'text': f'Критическая ошибка: {str(result)}',
                        'images': [],
                        'status': 'error'
                    })
                else:
                    processed_results.append(result)
            
            return processed_results
    
    def parse_generic(self, soup: BeautifulSoup, url: str) -> Dict:
        """Универсальный парсинг для неизвестных сайтов"""
        result = {
            'title': None,
            'date': None,
            'text': '',
            'images': []
        }
        
        # Попытка найти заголовок
        for tag in ['h1', 'title']:
            title_tag = soup.find(tag)
            if title_tag:
                result['title'] = title_tag.get_text(strip=True)
                break
        
        # Попытка найти дату публикации
        time_tag = soup.find('time', datetime=True)
        if time_tag:
            datetime_str = time_tag.get('datetime')
            if datetime_str:
                result['date'] = self.format_date(datetime_str)
        
        # Попытка найти основной контент
        for selector in ['article', 'main', '[role="article"]', '.article-content', '.post-content', '.entry-content']:
            content = soup.select_one(selector)
            if content:
                # Удаляем скрипты и стили
                for script in content.find_all(['script', 'style']):
                    script.decompose()
                
                # Извлекаем текст
                result['text'] = content.get_text(separator='\n\n', strip=True)
                
                # Извлекаем изображения
                for img in content.find_all('img'):
                    img_src = img.get('src') or img.get('data-src')
                    if img_src:
                        if img_src.startswith('//'):
                            img_src = 'https:' + img_src
                        elif img_src.startswith('/'):
                            img_src = urljoin(url, img_src)
                        elif not img_src.startswith('http'):
                            img_src = urljoin(url, img_src)
                        
                        # Пропускаем base64 изображения
                        if img_src.startswith('data:'):
                            continue
                        
                        # Проверяем на дубликаты
                        if any(existing_img['url'] == img_src for existing_img in result['images']):
                            continue
                        
                        result['images'].append({
                            'url': img_src,
                            'alt': img.get('alt', '')
                        })
                break
        
        return result


def extract_urls_from_line(line: str) -> List[str]:
    """Извлекает URL из строки, игнорируя текст названия статьи"""
    # Регулярное выражение для поиска URL (http:// или https://)
    url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
    urls = re.findall(url_pattern, line)
    return urls


async def main_async():
    """Асинхронная основная функция"""
    # Проверяем аргументы командной строки
    if len(sys.argv) < 2:
        print("Использование: python parse_articles.py <url1> [url2] [url3] ...")
        print("Или: python parse_articles.py --file <путь_к_файлу_с_url.txt>")
        print("Или: python parse_articles.py --file <путь_к_файлу_с_url.txt> --concurrent <число>")
        sys.exit(1)
    
    urls = []
    max_concurrent = 10
    file_path = None
    
    # Парсим аргументы
    i = 1
    while i < len(sys.argv):
        if sys.argv[i] == '--file' and i + 1 < len(sys.argv):
            file_path = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == '--concurrent' and i + 1 < len(sys.argv):
            try:
                max_concurrent = int(sys.argv[i + 1])
                if max_concurrent < 1:
                    max_concurrent = 1
                elif max_concurrent > 50:
                    max_concurrent = 50
                    print(f"Предупреждение: максимальное количество одновременных запросов ограничено до 50")
            except ValueError:
                print(f"Ошибка: неверное значение для --concurrent: {sys.argv[i + 1]}")
                sys.exit(1)
            i += 2
        else:
            # Если не флаг, то это URL
            urls.append(sys.argv[i])
            i += 1
    
    # Если указан файл, читаем URL из файла
    if file_path:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                file_urls = []
                for line in f:
                    line = line.strip()
                    # Пропускаем пустые строки и комментарии
                    if not line or line.startswith('#'):
                        continue
                    # Извлекаем URL из строки (может быть название статьи + URL на одной строке)
                    found_urls = extract_urls_from_line(line)
                    file_urls.extend(found_urls)
                # Если URL были переданы и через файл, и напрямую, объединяем их
                urls = file_urls + urls
        except FileNotFoundError:
            print(f"Файл {file_path} не найден")
            sys.exit(1)
    
    if not urls:
        print("Список URL пуст")
        sys.exit(1)
    
    # Очистка перед запуском
    print("Очистка предыдущих данных...")
    
    # Удаляем и создаем заново parsed_articles.json
    output_file = 'parsed_articles.json'
    if os.path.exists(output_file):
        os.remove(output_file)
        print(f"  ✓ Удален файл: {output_file}")
    
    # Очищаем каталог articles_markdown
    md_dir = 'articles_markdown'
    if os.path.exists(md_dir):
        shutil.rmtree(md_dir)
        print(f"  ✓ Очищен каталог: {md_dir}/")
    
    # Создаем каталог заново
    os.makedirs(md_dir, exist_ok=True)
    print(f"  ✓ Создан каталог: {md_dir}/")
    
    print(f"\nНайдено {len(urls)} URL для обработки")
    print(f"Максимальное количество одновременных запросов: {max_concurrent}")
    print("=" * 80)
    
    start_time = time.time()
    
    parser = ArticleParser(max_concurrent=max_concurrent)
    
    # Асинхронно обрабатываем все URL
    results = await parser.parse_articles_batch(urls)
    
    # Сохраняем markdown файлы для каждой статьи
    for i, article_data in enumerate(results, 1):
        if article_data['status'] == 'success':
            # Создаем имя файла из заголовка или URL
            if article_data.get('title') and article_data['title'] != 'Заголовок не найден':
                filename = parser.sanitize_filename(article_data['title'])
            else:
                # Используем часть URL как имя файла
                url_path = urlparse(article_data['url']).path.strip('/')
                filename = parser.sanitize_filename(url_path.replace('/', '_'))
            
            # Добавляем индекс для уникальности
            filename = f"{i:03d}_{filename}.md"
            md_filepath = os.path.join(md_dir, filename)
            
            # Преобразуем в markdown
            markdown_content = parser.article_to_markdown(
                article_data, 
                article_data.get('structured_content')
            )
            
            # Сохраняем markdown файл
            with open(md_filepath, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
            
            print(f"  ✓ [{i}/{len(results)}] Markdown сохранен: {md_filepath}")
    
    # Сохраняем результаты в JSON
    output_file = 'parsed_articles.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    elapsed_time = time.time() - start_time
    
    print("\n" + "=" * 80)
    print(f"Обработка завершена за {elapsed_time:.2f} секунд!")
    print(f"Успешно обработано: {sum(1 for r in results if r['status'] == 'success')}")
    print(f"Ошибок: {sum(1 for r in results if r['status'] == 'error')}")
    print(f"Средняя скорость: {len(urls) / elapsed_time:.2f} статей/сек")
    print(f"Результаты сохранены в файл: {output_file}")
    print(f"Markdown файлы сохранены в папку: {md_dir}/")


def main():
    """Основная функция (синхронная обертка)"""
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
