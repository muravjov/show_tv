
#
# Код работы с временем
# Название выбрано аналогично s_.py
#

def ts2str(ts):
    """Формат передачи timestamp - строка в ISO 8601 (в UTC)"""
    res = ""
    if ts:
        # если нет tzinfo, то Py ничего не добавляет, а по стандарту надо
        suffix = "" if ts.tzinfo else "Z"
        res = ts.isoformat() + suffix
    return res

import timeit
def timer():
    # :TRICKY:
    # - под Unix time.clock() считает только CPU-время текущего потока,
    #   а нам нужно обычное время => time.time(); Windows-вариант же time.clock() равен,
    #   однако, time.clock(), а вот аналога time.clock() в офиц. библиотеке вообще нет,
    #   см. в сторону биндинга для GetThreadTimes() => потому используем таймер по умолчанию
    #   из timeit (для обычного времени)
    # - timeit излишне усложнен (в частности, там отключается сборщик
    #   мусора на время работы алгоритма, а мы не хотим менять поведение),
    #   поэтому только используем их выбор таймера
    tmr = timeit.default_timer # time.clock
    return tmr()

def measure_time(profiled_func, *args, **kw):
    dur = timer()
    profiled_func(*args, **kw)
    return timer() - dur

import datetime
def utcnow():
    return datetime.datetime.utcnow()

def utcnow_str():
    return ts2str(utcnow())


