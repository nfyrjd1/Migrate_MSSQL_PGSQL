Скрипт по переносу данных из MSSQL в PGSQL  

**Важно:** Скрипт переносит только те таблицы и колонки, которые присутствуют в конечной БД.  
Если в конечной БД есть столбец, которого нет в исходной БД, то выдаст ошибку. 
Если в исходной БД нарушены внешние ключи, присутствующие в конечной БД, то также выдаст ошибку.  
То есть по большей части схемы баз данных должны **совпадать**.

При первом запуске создаст файл настроек, в котором нужно указать строки подключения.  
В случае прерывания работы скрипта, он продолжит работу при следующем запуске, основываясь на файлах: 
- `tables.txt` - список перенесенных таблиц
- `after_script.txt` - скрипт по восстанавлению внешних ключей.

### Алгоритм работы скрипта:
1. Получает структуру конечной БД
2. Удаляет внешние ключи всех таблиц
3. Параллельно запускает перенос нескольких таблиц (по количеству соединений). При переносе таблицы:  
    - Делается `TRUNCATE` этой таблицы в конечной БД
    - Читаются данные и сразу же пишутся в конечную БД с помощью `COPY "table" FROM STDIN (FORMAT BINARY)`
    - Восстанавливается идентификатор таблицы с помощью: `ALTER SEQUENCE "table" RESTART WITH value;`
4. Восстанавливаются внешние ключи таблиц  

За основу взят скрипт из статьи: [https://habr.com/ru/companies/rshb/articles/829738/](https://habr.com/ru/companies/rshb/articles/829738/)
