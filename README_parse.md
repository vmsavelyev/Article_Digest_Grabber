# Скрипт парсинга статей

Скрипт для извлечения заголовков, даты публикации, текста и изображений из статей с различных сайтов.

## Поддерживаемые сайты

- **vc.ru** - российский бизнес-портал
- **techcrunch.com** - технологический новостной сайт
- **habr.com** - российский IT-портал
- **news.crunchbase.com** - российский IT-портал
- **infoq.com** - российский IT-портал
- **Другие сайты** - универсальный парсинг (может работать с любыми сайтами)

## Свойства
- **Параллелизация** - Запрашивается одновременно несколько URL-ов
- **Retry логика** - При ошибке 403 парсер автоматически повторяет запрос до 3 раз с увеличивающейся задержкой
- **Есть функция случайных задержек** - Между параллельными запросами добавлена случайная задержка 0.1-0.5 сек для имитации естественного поведения.
- **Не создает image в md infoq** - Связанно с тем что изображения на этом сайте   использует встроенные base64 данные вместо внешних URL и прямое добавление такие ссылок в md может увеличить размер md файла.

## Установка зависимостей

```bash
pip install -r requirements.txt
```

## Использование

### Вариант 1: Передача URL через аргументы командной строки

**Важно:** URL с параметрами (содержащие символы `?`, `&` и т.д.) нужно заключать в кавычки!

```bash
# Простой URL без параметров
python3 parse_articles.py https://vc.ru/media/123456

# URL с параметрами (обязательно в кавычках!)
python3 parse_articles.py "https://vc.ru/media/2330878-kinopoisk-obnovil-podpiski-na-servisy-bez-yandeks-plyusa?from=rss"

# Несколько URL
python3 parse_articles.py "https://vc.ru/media/123456?from=rss" "https://techcrunch.com/2025/11/09/article/"
```

**Альтернативный способ запуска** (если скрипт имеет права на выполнение):
```bash
chmod +x parse_articles.py
./parse_articles.py "https://vc.ru/media/123456?from=rss"
```

**Примечание:** На macOS обычно используется `python3` вместо `python`. Если команда `python` не работает, используйте `python3`.

### Вариант 2: Передача URL из файла

Создайте файл `urls.txt` со списком URL (по одному на строку):

```
https://vc.ru/media/123456
https://techcrunch.com/2025/11/09/article/
https://habr.com/ru/news/123456/
https://www.infoq.com
https://news.crunchbase.com

```

Затем запустите:

```bash
python3 parse_articles.py --file urls.txt
```

## Формат выходных данных

Скрипт создает два типа файлов:

### 1. JSON файл с данными всех статей

Результаты сохраняются в файл `parsed_articles.json` в формате JSON:

```json
[
  {
    "url": "https://vc.ru/media/123456",
    "site_type": "vcru",
    "title": "Заголовок статьи",
    "date": "10.11.2025",
    "text": "Полный текст статьи...",
    "images": [
      {
        "url": "https://example.com/image.jpg",
        "alt": "Описание изображения"
      }
    ],
    "status": "success"
  }
]
```

### 2. Markdown файлы для каждой статьи

Каждая статья также сохраняется в отдельный markdown файл в папке `articles_markdown/`. Файлы имеют формат, совместимый с Notion, и сохраняют структуру текста и последовательность изображений.

**Формат имени файла:** `001_Заголовок_статьи.md`

**Структура markdown файла:**
```markdown
# Заголовок статьи

**Дата публикации:** 10.11.2025
**Источник:** https://vc.ru/media/123456

---

Текст статьи...

![Описание изображения](https://example.com/image.jpg)

- Элемент списка 1
- Элемент списка 2
```

Markdown файлы готовы для импорта в Notion database как pages.

## Структура данных

- **url** - исходный URL статьи
- **site_type** - тип сайта (vcru, techcrunch, habr, unknown)
- **title** - заголовок статьи
- **date** - дата публикации в формате DD.MM.YYYY (может быть null, если дата не найдена)
- **text** - полный текст статьи
- **images** - массив объектов с изображениями (url и alt текст)
- **status** - статус обработки (success/error)

## Примеры использования

```bash
# Одна статья (без параметров)
python3 parse_articles.py https://vc.ru/media/2330878-kinopoisk-obnovil-podpiski

# Одна статья (с параметрами - обязательно кавычки!)
python3 parse_articles.py "https://vc.ru/media/2330878-kinopoisk-obnovil-podpiski-na-servisy-bez-yandeks-plyusa?from=rss"

# Несколько статей
python3 parse_articles.py \
  "https://vc.ru/media/123456?from=rss" \
  "https://techcrunch.com/2025/11/09/article/" \
  "https://habr.com/ru/news/123456/"

# Из файла (рекомендуется для URL с параметрами)
python3 parse_articles.py --file my_urls.txt
```

## Импорт в Notion Database

После создания markdown файлов вы можете импортировать их в Notion Database.

### Подготовка

1. **Получите API токен Notion:**
   - Перейдите на https://www.notion.so/my-integrations
   - Создайте новую интеграцию (integration)
   - Скопируйте "Internal Integration Token"

2. **Подключите интеграцию к вашей Database:**
   - Откройте вашу Notion Database
   - Нажмите на "..." в правом верхнем углу
   - Выберите "Connections" → "Add connections"
   - Выберите вашу интеграцию

3. **Получите Database ID:**
   - Откройте вашу Database в браузере
   - URL будет выглядеть так: `https://www.notion.so/workspace/abc123def456ghi789jkl012mno345pq?v=...`
   - Database ID - это часть между последним `/` и `?`: `abc123def456ghi789jkl012mno345pq`

### Запуск импорта

```bash
python3 import_to_notion.py <NOTION_TOKEN> <DATABASE_ID>
```

**Пример:**
```bash
python3 import_to_notion.py secret_abc123xyz abc123def456ghi789jkl012mno345pq
```

### Что импортируется

- **Properties:**
  - `Name` - заголовок статьи
  - `URL` - URL исходной статьи
  - `Дата публикации` - дата публикации в формате DD.MM.YYYY
  - Остальные properties остаются пустыми

- **Контент страницы:**
  - Только тело статьи (текст, изображения, списки)
  - Заголовок, URL и дата НЕ добавляются в контент (они в properties)

### Структура Database

Убедитесь, что в вашей Notion Database есть следующие properties:
- `Name` (тип: Title)
- `URL` (тип: URL)
- `Дата публикации` (тип: Date)

## Решение проблем

### Ошибка "zsh: no matches found"

Если вы видите ошибку `zsh: no matches found`, это означает, что URL содержит специальные символы (например, `?` или `&`), которые zsh интерпретирует как команды. **Решение:** заключите URL в кавычки:

```bash
# Неправильно:
python3 parse_articles.py https://example.com/article?id=123

# Правильно:
python3 parse_articles.py "https://example.com/article?id=123"
```

Если вы видите ошибку `command not found: parse_articles.py`, используйте `python3` перед именем скрипта:

```bash
# Неправильно:
parse_articles.py "https://example.com/article?id=123"

# Правильно:
python3 parse_articles.py "https://example.com/article?id=123"
```

Или используйте файл со списком URL (рекомендуется для множества URL):
```bash
python3 parse_articles.py --file urls.txt
```
