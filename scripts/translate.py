"""Word-boundary-safe Russian→English translation for .cs files."""
import re, os, io

BASE = r'F:\Projects\Mediana'
RUSSIAN = re.compile(r'[а-яА-ЯёЁ]')

# Only replace COMPLETE words (word boundaries), never substrings
WORD_MAP = {
    # Technical nouns
    'хендлер': 'handler', 'хендлеры': 'handlers', 'хендлера': 'handler',
    'сообщение': 'message', 'сообщения': 'messages', 'сообщению': 'message', 'сообщением': 'message',
    'команда': 'command', 'команды': 'commands', 'команду': 'command',
    'запрос': 'query', 'запросы': 'queries', 'запроса': 'query',
    'событие': 'event', 'события': 'events', 'событию': 'event', 'событием': 'event',
    'событий': 'events',
    'пайплайн': 'pipeline', 'пайплайны': 'pipelines', 'пайплайна': 'pipeline',
    'мидлвар': 'middleware', 'мидлвары': 'middlewares', 'мидлвара': 'middleware',
    'цепочка': 'chain', 'цепочки': 'chains', 'цепочку': 'chain',
    'реестр': 'registry', 'реестра': 'registry', 'реестре': 'registry',
    'конверт': 'envelope', 'конверта': 'envelope', 'конверте': 'envelope',
    'транспорт': 'transport', 'транспорты': 'transports', 'транспорта': 'transport',
    'хранилище': 'store', 'хранилища': 'stores',
    'очередь': 'queue', 'очереди': 'queues', 'очередью': 'queue',
    'хендлеру': 'handler', 'хендлером': 'handler',
    'исключение': 'exception', 'исключения': 'exceptions', 'исключению': 'exception', 'исключением': 'exception',
    'ошибка': 'error', 'ошибки': 'errors', 'ошибке': 'error', 'ошибку': 'error', 'ошибок': 'errors',
    'зависимость': 'dependency', 'зависимости': 'dependencies', 'зависимостей': 'dependencies',
    'ветка': 'branch', 'ветки': 'branches', 'ветке': 'branch',
    'путь': 'path', 'пути': 'paths', 'путём': 'path',
    'пул': 'pool', 'пула': 'pool', 'пулом': 'pool',
    'канал': 'channel', 'каналы': 'channels', 'канала': 'channel',
    'поток': 'thread', 'потоки': 'threads', 'потока': 'thread', 'потоков': 'threads',
    'вызов': 'dispatch', 'вызовы': 'dispatches', 'вызова': 'dispatch', 'вызове': 'dispatch',
    'аллокация': 'allocation', 'аллокации': 'allocations',
    'латентность': 'latency', 'латентности': 'latency',
    'сборка': 'build', 'сборки': 'builds',
    'операция': 'operation', 'операции': 'operations',
    'счётчик': 'counter', 'счётчики': 'counters', 'счётчика': 'counter',
    'опция': 'option', 'опции': 'options',
    'режим': 'mode', 'режимы': 'modes',
    'ветвление': 'branching',
    'состояние': 'state', 'состояния': 'states',
    'делегат': 'delegate', 'делегаты': 'delegates',
    'тип': 'type', 'типы': 'types', 'типа': 'type',
    'класс': 'class', 'классы': 'classes',
    'структура': 'struct',
    'метод': 'method', 'методы': 'methods', 'метода': 'method',
    'функция': 'function', 'функции': 'functions',
    'интерфейс': 'interface', 'интерфейсы': 'interfaces',
    'ключ': 'key', 'ключа': 'key',
    'значение': 'value', 'значения': 'values',
    'свойство': 'property', 'свойства': 'properties',
    'поле': 'field', 'поля': 'fields',
    'атрибут': 'attribute',
    'параметр': 'parameter', 'параметры': 'parameters',
    'аргумент': 'argument',
    'коллекция': 'collection', 'коллекции': 'collections',
    'таблица': 'table', 'таблицы': 'tables',
    'колонка': 'column', 'колонки': 'columns',
    'индекс': 'index', 'индексы': 'indexes',
    'транзакция': 'transaction', 'транзакции': 'transactions',
    'блокировка': 'lock', 'блокировки': 'locks',
    'семафор': 'semaphore',
    'соединение': 'connection', 'соединения': 'connections',
    'клиент': 'client', 'клиента': 'client',
    'сервер': 'server',
    'брокер': 'broker',
    'партиция': 'partition', 'партиции': 'partitions',
    'очередь': 'queue', 'очереди': 'queues',
    'пароль': 'password',
    'сертификат': 'certificate',
    'сессия': 'session',
    'строка': 'row', 'строки': 'rows',
    'узел': 'node', 'узлы': 'nodes',
    'провайдер': 'provider', 'провайдеры': 'providers',
    'фабрика': 'factory', 'фабрики': 'factories',
    'регистрация': 'registration',
    'конфигурация': 'configuration',
    'проверка': 'check', 'проверки': 'checks', 'проверку': 'check',
    'тест': 'test', 'тесты': 'tests', 'теста': 'test',
    'замер': 'measurement', 'замеры': 'measurements',
    'прогон': 'run', 'прогоны': 'runs',
    'сценарий': 'scenario', 'сценарии': 'scenarios',
    'результат': 'result', 'результаты': 'results', 'результате': 'result',
    'сравнение': 'comparison', 'сравнения': 'comparisons',
    'нагрузка': 'load', 'нагрузки': 'load',
    'хвост': 'tail', 'хвосты': 'tails',
    'память': 'memory',
    'удержание': 'retention',
    'throughput': 'throughput',
    'масштабирование': 'scaling',
    'параллелизм': 'parallelism',
    'конкуренция': 'contention',
    'дедупликация': 'deduplication',
    'идемпотентность': 'idempotency',
    'попытка': 'attempt', 'попытки': 'attempts', 'попыток': 'attempts',
    'отравление': 'poison', 'отравления': 'poison',
    'поворот': 'retry', 'повороты': 'retries',
    'причина': 'reason', 'причины': 'reasons',
    'подробности': 'details',
    'описание': 'description', 'описании': 'description',
    'назначение': 'purpose',
    'область': 'scope',
    'граница': 'boundary', 'границы': 'boundaries',
    'семантика': 'semantics',
    'контракт': 'contract', 'контракты': 'contracts',
    'реализация': 'implementation', 'реализации': 'implementations',
    'расширение': 'extension', 'расширения': 'extensions',
    'выражение': 'expression', 'выражения': 'expressions',
    'лямбда': 'lambda',
    # Adjectives
    'общий': 'shared', 'общая': 'shared', 'общее': 'shared', 'общие': 'shared',
    'единый': 'unified', 'единая': 'unified', 'единое': 'unified',
    'внешний': 'external', 'внешняя': 'external', 'внешние': 'external',
    'внутренний': 'internal', 'внутренняя': 'internal', 'внутренние': 'internal',
    'публичный': 'public',
    'защитный': 'guard', 'защитная': 'guard', 'защитные': 'guard',
    'идемпотентный': 'idempotent',
    'иммутабельный': 'immutable', 'иммутабельная': 'immutable',
    'мутабельный': 'mutable',
    'асинхронный': 'async', 'асинхронная': 'async', 'асинхронное': 'async',
    'синхронный': 'sync', 'синхронная': 'sync', 'синхронное': 'sync',
    'оптимизированный': 'optimized',
    'скомпилированный': 'compiled',
    'потокобезопасный': 'thread-safe',
    'стабильный': 'stable',
    'детерминированный': 'deterministic',
    'линнейный': 'linear',
    'бесконечный': 'infinite', 'бесконечная': 'infinite', 'бесконечное': 'infinite',
    'ограниченный': 'bounded', 'ограниченная': 'bounded',
    'неблокирующий': 'non-blocking', 'неблокирующая': 'non-blocking',
    'захардкоженный': 'hardcoded',
    'типизированный': 'typed',
    'нетипизированный': 'untyped',
    'обобщённый': 'generic',
    'закрытый': 'closed',
    'открытый': 'open',
    'статический': 'static',
    'динамический': 'dynamic',
    'первый': 'first', 'первая': 'first', 'первое': 'first',
    'второй': 'second',
    'последний': 'last', 'последняя': 'last', 'последнее': 'last',
    'новый': 'new', 'новая': 'new', 'новое': 'new',
    'старый': 'old',
    'другой': 'other', 'другая': 'other', 'другое': 'other', 'другие': 'other',
    'остальные': 'remaining', 'оставшиеся': 'remaining',
    'каждый': 'each', 'каждая': 'each', 'каждое': 'each',
    'любой': 'any', 'любая': 'any', 'любое': 'any',
    'все': 'all', 'всё': 'all', 'вся': 'all',
    'некоторые': 'some',
    'такой': 'such', 'такая': 'such', 'такое': 'such',
    'тот': 'that', 'та': 'that', 'то': 'that',
    'этот': 'this', 'эта': 'this', 'это': 'this', 'эти': 'this',
    # Verbs
    'возвращает': 'returns', 'возвращать': 'return', 'возвращается': 'is returned', 'возвращается': 'returns',
    'принимает': 'accepts', 'принимать': 'accept',
    'создаёт': 'creates', 'создавать': 'create', 'создаётся': 'is created', 'создан': 'created',
    'удаляет': 'deletes', 'удалить': 'to delete', 'удаляется': 'is deleted', 'удалён': 'deleted',
    'проверяет': 'checks', 'проверить': 'to check', 'проверяется': 'is checked',
    'выбирает': 'selects', 'выбрать': 'to select',
    'обрабатывает': 'processes', 'обработать': 'to process', 'обрабатывается': 'is processed',
    'передаёт': 'passes', 'передавать': 'pass', 'передаётся': 'is passed',
    'копирует': 'copies', 'копируется': 'is copied',
    'читает': 'reads', 'читаться': 'to be read', 'читается': 'is read',
    'пишет': 'writes', 'пишется': 'is written',
    'блокирует': 'blocks', 'блокироваться': 'to block',
    'поддерживает': 'supports', 'поддерживаться': 'to be supported', 'поддерживается': 'is supported',
    'обеспечивает': 'ensures', 'обеспечиваться': 'to ensure',
    'гарантирует': 'guarantees', 'гарантироваться': 'to guarantee', 'гарантируется': 'is guaranteed',
    'позволяет': 'allows', 'позволять': 'allow',
    'запрещает': 'prohibits',
    'ограничивает': 'limits', 'ограничиваться': 'to be limited',
    'расширяет': 'extends', 'расширяться': 'to extend',
    'оптимизирует': 'optimizes', 'оптимизировать': 'to optimize',
    'ускоряет': 'accelerates',
    'уменьшает': 'reduces', 'уменьшаться': 'to reduce',
    'увеличивает': 'increases', 'увеличиваться': 'to increase',
    'сохраняет': 'preserves', 'сохраняться': 'to preserve', 'сохраняется': 'is preserved',
    'нарушает': 'breaks', 'нарушаться': 'to break',
    'чинит': 'fixes', 'исправить': 'to fix', 'исправлен': 'fixed',
    'закрывает': 'closes', 'закрыть': 'to close', 'закрывается': 'is closed',
    'открывает': 'opens', 'открыть': 'to open', 'открывается': 'is opened',
    'начинает': 'starts', 'начать': 'to start', 'начинается': 'starts',
    'завершает': 'completes', 'завершить': 'to complete', 'завершается': 'completes',
    'останавливает': 'stops', 'остановить': 'to stop', 'останавливается': 'stops',
    'возобновляет': 'resumes',
    'выполняется': 'is executed', 'выполняться': 'to execute',
    'происходит': 'occurs', 'происходить': 'to occur',
    'используется': 'is used', 'использовать': 'to use', 'использоваться': 'to be used',
    'применяется': 'is applied', 'применяться': 'to be applied',
    'добавляется': 'is added', 'добавить': 'to add', 'добавлен': 'added',
    'удаляется': 'is removed', 'удалён': 'removed',
    'кэшируется': 'is cached',
    'компилируется': 'is compiled', 'компилироваться': 'to compile',
    'регистрируется': 'is registered', 'регистрировать': 'to register',
    'резолвится': 'is resolved', 'резолвиться': 'to resolve',
    'диспатчится': 'is dispatched',
    'помечается': 'is marked',
    'устанавливается': 'is set',
    'сбрасывается': 'is reset',
    'вычисляется': 'is computed', 'вычислять': 'to compute',
    'генерируется': 'is generated', 'генерировать': 'to generate',
    'сериализуется': 'is serialized', 'сериализовать': 'to serialize',
    'десериализуется': 'is deserialized',
    'кодируется': 'is encoded',
    'декодируется': 'is decoded',
    'хранится': 'is stored', 'хранить': 'to store', 'храниться': 'to be stored',
    # Prepositions/conjunctions (word-boundary only)
    'для': 'for', 'при': 'on', 'без': 'without', 'из': 'from', 'в': 'in',
    'на': 'on', 'по': 'by', 'от': 'from', 'до': 'to',
    'не': 'not', 'и': 'and', 'или': 'or', 'но': 'but',
    'если': 'if', 'также': 'also', 'только': 'only',
    'уже': 'already', 'ещё': 'still', 'всегда': 'always', 'никогда': 'never',
    'иногда': 'sometimes', 'часто': 'often',
    'может': 'may', 'должен': 'must', 'должна': 'must', 'должно': 'must',
    'будет': 'will be', 'был': 'was', 'была': 'was', 'были': 'were',
    'есть': 'is', 'нет': 'no',
    'см': 'see', 'т.д.': 'etc.', 'напр.': 'e.g.',
    # Benchmarks
    'операций': 'operations', 'удержано': 'retained',
    'аллокировано': 'allocated', 'прогрев': 'warmup',
    'полная': 'full',
}

# Build regex pattern for word-boundary replacement
# Sort by length descending so longer phrases match first
sorted_words = sorted(WORD_MAP.keys(), key=len, reverse=True)
# Escape special regex chars
escaped = [re.escape(w) for w in sorted_words]
# Pattern: word boundary + any of the words + word boundary
pattern = re.compile(r'\b(' + '|'.join(escaped) + r')\b', re.IGNORECASE)

def translate_text(text):
    """Replace Russian words with English using word boundaries."""
    def replacer(m):
        word = m.group(0)
        # Try exact match first
        if word in WORD_MAP:
            return WORD_MAP[word]
        # Try lowercase
        lower = word.lower()
        if lower in WORD_MAP:
            return WORD_MAP[lower]
        # Return original if no match
        return word
    return pattern.sub(replacer, text)

def process_file(path):
    try:
        content = io.open(path, encoding='utf-8').read()
    except:
        return 0
    lines = content.split('\n')
    changed = 0
    for i, line in enumerate(lines):
        if RUSSIAN.search(line):
            translated = translate_text(line)
            if translated != line:
                lines[i] = translated
                changed += 1
    if changed > 0:
        io.open(path, 'w', encoding='utf-8', newline='').write('\n'.join(lines))
    return changed

if __name__ == '__main__':
    total = 0
    files_changed = 0
    for root in ('src', 'tests', 'benchmarks'):
        full = os.path.join(BASE, root)
        for dirpath, _, fnames in os.walk(full):
            if 'obj' in dirpath or 'bin' in dirpath:
                continue
            for fn in fnames:
                if fn.endswith('.cs'):
                    p = os.path.join(dirpath, fn)
                    n = process_file(p)
                    if n > 0:
                        total += n
                        files_changed += 1
    print(f'Translated {total} lines in {files_changed} files')
