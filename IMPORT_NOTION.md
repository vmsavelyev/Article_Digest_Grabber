# Инструкция по импорту статей в Notion

## Быстрый старт

1. **Установите зависимости:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Получите Notion API токен:**
   - Перейдите на https://www.notion.so/my-integrations
   - Создайте новую интеграцию
   - Скопируйте "Internal Integration Token"

3. **Подключите интеграцию к Database:**
   - Откройте вашу Database в Notion
   - Нажмите "..." → "Connections" → "Add connections"
   - Выберите вашу интеграцию

4. **Запустите импорт:**
   
   Есть несколько способов указать параметры:
   
   **Способ 1: Через переменные окружения (рекомендуется)**
   ```bash
   export NOTION_TOKEN='your_token_here'
   export NOTION_DATABASE_ID='your_database_id_here'
   python3 import_to_notion.py
   ```
   
   **Способ 2: Через аргументы командной строки**
   ```bash
   python3 import_to_notion.py <NOTION_TOKEN> <DATABASE_ID>
   ```
   
   **Способ 3: Комбинированный**
   ```bash
   export NOTION_DATABASE_ID='your_database_id_here'
   python3 import_to_notion.py <NOTION_TOKEN>
   ```

## Database ID

Database ID извлекается из URL вашей базы данных:
- URL: `https://www.notion.so/workspace/abc123def456ghi789jkl012mno345pq?v=...`
- Database ID: `abc123def456ghi789jkl012mno345pq` (часть между последним `/` и `?`)

**Важно:** Вы можете указать любой Database ID через:
- Переменную окружения `NOTION_DATABASE_ID`
- Аргумент командной строки
- Комбинацию обоих способов (аргументы имеют приоритет)

## Структура Database

Убедитесь, что в вашей Database есть следующие properties:
- **Name** (тип: Title) - заголовок статьи
- **URL** (тип: URL) - ссылка на исходную статью
- **Дата публикации** (тип: Date) - дата публикации

## Что импортируется

- **Properties заполняются:**
  - Name ← заголовок статьи
  - URL ← URL статьи
  - Дата публикации ← дата публикации (DD.MM.YYYY)
  - Остальные properties остаются пустыми

- **Контент страницы:**
  - Только тело статьи (текст, изображения, списки)
  - Заголовок, URL и дата НЕ добавляются в контент

## Примеры использования

```bash
# Способ 1: Все через переменные окружения
export NOTION_TOKEN='secret_abc123xyz'
export NOTION_DATABASE_ID='abc123def456ghi789jkl012mno345pq'
python3 import_to_notion.py

# Способ 2: Все через аргументы командной строки
python3 import_to_notion.py secret_abc123xyz abc123def456ghi789jkl012mno345pq

# Способ 3: DATABASE_ID в переменной окружения, токен в аргументе
export NOTION_DATABASE_ID='abc123def456ghi789jkl012mno345pq'
python3 import_to_notion.py secret_abc123xyz

# Способ 4: Токен в переменной окружения, DATABASE_ID в аргументе
export NOTION_TOKEN='secret_abc123xyz'
python3 import_to_notion.py abc123def456ghi789jkl012mno345pq
```

**Примечание:** Аргументы командной строки имеют приоритет над переменными окружения.

## Устранение проблем

### Ошибка "Unauthorized"
- Проверьте правильность API токена
- Убедитесь, что интеграция подключена к Database

### Ошибка "Object not found"
- Проверьте правильность Database ID
- Убедитесь, что интеграция имеет доступ к Database

### Properties не заполняются
- Проверьте названия properties в Database (должны точно совпадать: "Name", "URL", "Дата публикации")
- Убедитесь, что типы properties правильные (Title, URL, Date)
