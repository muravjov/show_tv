#!/usr/bin/env python3
# coding: utf-8
#
# Copyright Bradbury Lab, 2013
# Авторы: 
#  Илья Муравьев
#

import os
import o_p, s_
import math
import list_bl_tv
import logging
import re
import datetime
import functools # functools.partial

import tornado.process
import tornado.web
import tornado.ioloop
IOLoop = tornado.ioloop.IOLoop.instance()

#import configuration
from configuration import make_struct, environment, get_env_value 
import api

#import getpass
#is_test = getpass.getuser() in ["muravyev", "ilya", "vany"]
cast_one_source = get_env_value("cast_one_source", None)
is_test = not cast_one_source and environment.is_test

PORT = 8910

def int_ceil(float_):
    """ Округлить float в больщую сторону """
    return int(math.ceil(float_))

class StreamType:
    HLS = 0
    HDS = 1

def enum_values(enum):
    return tuple(v for k, v in vars(enum).items() if k.upper() == k)
    
# используем namedtuple в качестве ключей, так как 
# они порождены от tuple => имеют адекватное упорядочивание
import collections
RTClass = collections.namedtuple('RTClass', ['refname', 'typ', 'bitrate'])
def r_t_b_key(refname, typ, bitrate):
    return RTClass(refname, typ, bitrate)

def r_t_b_iter(iteratable):
    """ Итератор всех пар=ключей (имя канала, тип вещания) по списку
        каналов iteratable"""
    for refname in iteratable:
        for typ in enum_values(StreamType):
            for bitrate, bandwidth in environment.bitrates.items():
                yield r_t_b_key(refname, typ, bitrate)

# в Bradbury обычно 3 секунду GOP, а фрагмент:
# - HLS: 9 секунд
# - HDS: 6 секунд
std_chunk_dur = 6
# формат фрагментов
NUM_FORMAT_SIZE = 8
chunk_tmpl = "out%%0%sd.ts" % NUM_FORMAT_SIZE
def hls_chunk_name(i):
    """ Имя i-го фрагмента/чанка """
    return chunk_tmpl % i

OUT_DIR = environment.out_dir

def out_fpath(chunk_dir, *fname):
    """ Вернуть полный путь до файла в OUT_DIR """
    return o_p.join(OUT_DIR, chunk_dir, *fname)

def remove_chunks(rng, cr):
    r_t_b = cr.r_t_b
    typ = r_t_b.typ
    for i in rng:
        if typ == StreamType.HLS:
            fname = hls_chunk_name(i)
        elif typ == StreamType.HDS:
            fname = hds_chunk_name(cr.frg_tbl, i)

        fname = out_fpath(r_t_b.refname, str(r_t_b.bitrate), fname)
        os.unlink(fname)

def ready_chk_end(chunk_range):
    """ Конец списка готовых, полностью записанных фрагментов """
    return chunk_range.end-1
def ready_chunks(chunk_range):
    """ Кол-во дописанных до конца фрагментов """
    return ready_chk_end(chunk_range) - chunk_range.beg
def written_chunks(chunk_range):
    """ Range готовых чанков """
    return range(chunk_range.beg, ready_chk_end(chunk_range))

def may_serve_pl(chunk_range):
    """ Можно ли вещать канал = достаточно ли чанков для выдачи в плейлисте """
    # :TEMP: везде перейти на std_chunk_dur
    this_chunk_dur = {
        StreamType.HLS: std_chunk_dur,
        StreamType.HDS: 3,
    }[chunk_range.r_t_b.typ] # :REFACTOR:
    
    min_total = 12 # максимум столько секунд храним
    min_cnt = int_ceil(float(min_total) / this_chunk_dur) # :REFACTOR:
    
    return ready_chunks(chunk_range) >= min_cnt

def emulate_live():
    return is_test and get_env_value("emulate_live", True)

def run_chunker(src_media_path, chunk_dir, on_new_chunk, on_stop_chunking, bitrate, is_batch=False):
    """ Запустить ffmpeg для фрагментирования файла/исходника src_media_path для
        вещания канала chunk_dir; 
        - on_new_chunk - что делать в момент начала создания нового фрагмента
        - on_stop_chunking - что делать, если ffmpeg закончил работу
        - is_batch - не эмулировать вещание, только для VOD-файлов """
    o_p.force_makedirs(out_fpath(chunk_dir, bitrate))
    
    ffmpeg_bin = environment.ffmpeg_bin
    
    # :TRICKY: так отлавливаем сообщение от segment.c вида "starts with packet stream"
    log_type = "debug"
    in_opts = "-i " + src_media_path
    if emulate_live() and not is_batch:
        # эмулируем выдачу видео в реальном времени
        in_opts = "-re " + in_opts
    bl_options = "-segment_time %s" % std_chunk_dur
    cmd = "%(ffmpeg_bin)s -v %(log_type)s %(in_opts)s -map 0 -codec copy -f ssegment %(bl_options)s" % locals()
    if is_test:
        #cmd += " -segment_list %(out_dir)s/playlist.m3u8" % locals()
        pass

    cmd += " %s" % out_fpath(chunk_dir, bitrate, chunk_tmpl)
    #print(cmd)
    
    Subprocess = tornado.process.Subprocess

    STREAM = Subprocess.STREAM
    ffmpeg_proc = Subprocess(cmd, stdout=STREAM, stderr=STREAM, shell=True)
 
    segment_sign = api.segment_sign
    def on_line(line):
        m = segment_sign.search(line)
        if m:
            #print("new segment:", line)
            chunk_ts = float(m.group("pt"))
            on_new_chunk(chunk_ts)

    line_sep = re.compile(br"(\n|\r\n?).", re.M)
    errdat = make_struct(txt = b'')
    def process_lines(dat):
        errdat.txt += dat
        
        line_end = 0
        while True:
            m = line_sep.search(errdat.txt, line_end)
            if m:
                line_beg = line_end
                line_end = m.end(1)
                on_line(errdat.txt[line_beg:line_end])
            else:
                break
            
        if line_end:
            errdat.txt = errdat.txt[line_end:]
    
    def on_stderr(dat):
        #print("data:", dat)
        process_lines(dat)
            
    # фиксируем прекращение активности транскодера после
    # двух событий
    end_set = set([True, False])
    def set_stop(is_stderr):
        end_set.discard(is_stderr)
        # оба события прошли
        if not end_set:
            on_stop_chunking()
    
    # все придет в on_stderr, сюда только - факт того, что файл
    # закрыли с той стороны (+ пустая строка)
    def on_stderr_end(dat):
        process_lines(dat)
        # последняя строка - может быть без eol
        if errdat.txt:
            on_line(errdat.txt)

        set_stop(True)
    # в stdout ffmpeg ничего не пишет
    #ffmpeg_proc.stdout.read_until_close(on_data, on_data)
    ffmpeg_proc.stderr.read_until_close(on_stderr_end, on_stderr)

    def on_proc_exit(_exit_code):
        #print("exit_code:", _exit_code)
        set_stop(False)
    ffmpeg_proc.set_exit_callback(on_proc_exit)
    
    return ffmpeg_proc.pid

def test_src_fpath(fname):
    return out_fpath(o_p.join('../test_src', fname))

def test_media_path(bitrate):
    # return list_bl_tv.make_path("pervyj.ts")+
    # return test_src_fpath("pervyj-720x406.ts")
    return test_src_fpath("pervyj-{0}.ts".format(bitrate))

Globals = make_struct(
    stop_streaming = False
)

def start_chunking(chunk_range):
    """ Запустить процесс вещания канала chunk_range.r_t_b.refname c типом
        вещания chunk_range.r_t_b.typ (HLS или HDS) """
    if Globals.stop_streaming:
        return
    
    # инициализация
    chunk_range.is_started = True
    chunk_range.beg = 0
    # номер следующего за пишущимся чанком
    chunk_range.end = 0
    
    # :TODO: переделать через полиморфизм
    typ = chunk_range.r_t_b.typ
    if typ == StreamType.HLS:
        start_hls_chunking(chunk_range)
    elif typ == StreamType.HDS:
        start_hds_chunking(chunk_range)
        
def do_stop_chunking(chunk_range):
    """ Функция, которую нужно выполнить по окончанию вещания (когда закончил
        работу chunker=ffmpeg """
    chunk_range.is_started = False
    remove_chunks(range(chunk_range.beg, chunk_range.end), chunk_range)

    may_restart = not chunk_range.stop_signal
    if chunk_range.stop_signal:
        chunk_range.stop_signal = False

    if Globals.stop_streaming:
        stop_lst = Globals.stop_lst
        stop_lst.discard(chunk_range.r_t_b)
        if not stop_lst:
            IOLoop.stop()
    else:
        if may_restart:
            start_chunking(chunk_range)

#
# HLS
#
from tornado import gen
from app.models.dvr_writer import DVRWriter

dvr_writer = DVRWriter(host=environment.dvr_host)


def start_hls_chunking(chunk_range):
    chunk_range.start_times = []

    @gen.engine
    def on_new_chunk(chunk_ts):
        if chunk_range.end == 0:
            chunk_range.start = datetime.datetime.utcnow()

        chunk_range.start_times.append(chunk_ts)
        chunk_range.end += 1

        if may_serve_pl(chunk_range):
            hdls = chunk_range.on_first_chunk_handlers
            chunk_range.on_first_chunk_handlers = []
            for hdl in hdls:
                hdl()

        max_total = 72 # максимум столько секунд храним
        max_cnt = int_ceil(float(max_total) / std_chunk_dur)
        diff = ready_chunks(chunk_range) - max_cnt

        # <DVR writer> --------------------------------------------------------
        if (
            environment.do_write_dvr == True and
            chunk_range.end > 1
        ):
            # индекс того чанка, который готов
            i = chunk_range.end-2
            # время в секундах от начала создания первого чанка
            start_seconds = chunk_range.start_times[i-chunk_range.beg]
            # путь до файла с готовым чанком
            fname = hls_chunk_name(i)
            path_payload = out_fpath(
                chunk_range.r_t_b.refname,
                str(chunk_range.r_t_b.bitrate),
                fname
            )
            # длина чанка
            duration = chunk_duration(i, chunk_range)
            dvr_writer.write(
                name=chunk_range.r_t_b.refname,
                bitrate=chunk_range.r_t_b.bitrate,
                start_utc=chunk_range.start,
                start_seconds=start_seconds,
                duration=duration,
                is_pvr=True,
                path_payload=path_payload,
                metadata=b'{"metadata_key": "metadata_value"}',
            )
        # ------------------------------------------------------- </DVR writer>

        if diff > 0:
            old_beg = chunk_range.beg
            chunk_range.beg += diff
            del chunk_range.start_times[:diff]
            remove_chunks(range(old_beg, chunk_range.beg), chunk_range)

    def on_stop_chunking():
        do_stop_chunking(chunk_range)

    # :REFACTOR:
    refname = chunk_range.r_t_b.refname
    if cast_one_source:
        src_media_path = cast_one_source
    elif is_test:
        src_media_path = test_media_path(chunk_range.r_t_b.bitrate)
    else:
        src_media_path = RefnameDict[refname]

    chunk_range.pid = run_chunker(src_media_path, refname, on_new_chunk, on_stop_chunking, str(chunk_range.r_t_b.bitrate))

def chunk_duration(i, chunk_range):
    """ Длительность фрагмента в секундах, float """
    i -= chunk_range.beg
    st = chunk_range.start_times
    return st[i+1] - st[i]

def serve_hls_pl(hdl, chunk_range):
    """ Выдача плейлиста HLS, .m3u8; 
        type(hdl) == tornado.web.RequestHandler """
    # :TRICKY: по умолчанию tornado выставляет
    # "text/html; charset=UTF-8", и вроде как по 
    # документации HLS, http://tools.ietf.org/html/draft-pantos-http-live-streaming-08 ,
    # такое возможно, если путь оканчивается на .m3u8 , но в реальности
    # Safari/IPad такое не принимает (да и Firefox/Linux тоже)
    hdl.set_header("Content-Type", "application/vnd.apple.mpegurl")
    bitrate = chunk_range.r_t_b.bitrate

    write = hdl.write
    # EXT-X-TARGETDURATION - должен быть, и это
    # должен быть максимум
    max_dur = 0
    chunk_lst = []
    for i in written_chunks(chunk_range):
        dur = chunk_duration(i, chunk_range)
        name = hls_chunk_name(i)

        max_dur = max(dur, max_dur)

        # используем %f (6 знаков по умолчанию) вместо %s, чтобы на 
        # '%s' % 0.0000001 не получать '1e-07'
        chunk_lst.append("""#EXTINF:{dur:f},
{bitrate}/{name}
""".format(**locals()))

        # по спеке это должно быть целое число, иначе не работает (IPad)
        max_dur = int_ceil(max_dur)
        
    # EXT-X-MEDIA-SEQUENCE - номер первого сегмента,
    # нужен для указания клиенту на то, что список живой,
    # т.е. его элементы будут добавляться/исчезать по FIFO
    write("""#EXTM3U
#EXT-X-VERSION:3
#EXT-X-ALLOW-CACHE:NO
#EXT-X-TARGETDURATION:%(max_dur)s
#EXT-X-MEDIA-SEQUENCE:%(chunk_range.beg)s
""" % s_.EvalFormat())
                
    for s in chunk_lst:
        write(s)
        
    # а вот это для live не надо
    #write("#EXT-X-ENDLIST")
    
    hdl.finish()

#
# HDS
#

def hds_ts(chunk):
    return chunk[1]
def hds_duration(chunk):
    return chunk[2]

def hds_chunk_name(frg_tbl, i):
    return "Seg1-Frag%s" % frg_tbl[i][0]

def start_hds_chunking(chunk_range):
    #chunk_range.pid = -1 # эмуляция сущeствования процесса
    import abst
    import test_hds
    chunk_range.frg_tbl = frg_tbl = test_hds.get_frg_tbl()

    # не принимаем пустые таблицы
    assert frg_tbl
    first_chunk = frg_tbl[0]

    import timeit
    timer_func = timeit.default_timer
    streaming_start = hds_ts(first_chunk) - timer_func()

    def on_new_chunk():
        chunk_range.end += 1

        # копируем готовый фрагмент
        # (заранее, хоть он пока и "не готов")
        done_chunk_idx = chunk_range.end-1
        fname = hds_chunk_name(frg_tbl, done_chunk_idx)
        src_fname = o_p.join(test_hds.get_frg_test_dir(), fname)
        dst_fname = o_p.join(out_fpath(chunk_range.r_t_b.refname), fname)
        import shutil
        shutil.copyfile(src_fname, dst_fname)
            
        # :REFACTOR: on_first_chunk_handlers + удаление старых
        if may_serve_pl(chunk_range):
            hdls = chunk_range.on_first_chunk_handlers
            chunk_range.on_first_chunk_handlers = []
            for hdl in hdls:
                hdl()

        max_total = 72 # максимум столько секунд храним
        #max_cnt = int_ceil(float(max_total) / std_chunk_dur)
        max_cnt = int_ceil(float(max_total) / 3)
        diff = ready_chunks(chunk_range) - max_cnt
        if diff > 0:
            old_beg = chunk_range.beg
            chunk_range.beg += diff
            #del chunk_range.start_times[:diff]
            remove_chunks(range(old_beg, chunk_range.beg), chunk_range)

        # эмуляция получения следующего фрагмента
        if chunk_range.stop_signal or (chunk_range.end >= len(frg_tbl)):
            do_stop_chunking(chunk_range)
        else:
            next_idx = chunk_range.end
            frg = frg_tbl[next_idx]

            if not is_test or emulate_live():
                timeout = hds_ts(frg) - streaming_start - timer_func()
                IOLoop.add_timeout(datetime.timedelta(seconds=timeout), on_new_chunk)
            else:
                IOLoop.add_callback(on_new_chunk)

    on_new_chunk()

import gen_hds

def disable_caching(hdl):
    # судя по всему, для live нужно ставить эти заголовки, иначе плейер
    # решит, что список не изменяется (так делает 1tv (не всегда) и wowza)
    no_cache = [
        ('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0'),
        ("Pragma",        "no-cache"),
    ]
    for header in no_cache:
        hdl.set_header(*header)
        
    # :TRICKY: 1tv ставит еще Expires, Last-Modified, Vary, Accept-Ranges,
    # Age, но полагаю, что это к делу не относится (OSMFу все равно)
    
def serve_hds_pl(hdl, chunk_range):
    hdl.set_header("Content-Type", "binary/octet")
    disable_caching(hdl)
    
    lst = chunk_range.frg_tbl[chunk_range.beg:ready_chk_end(chunk_range)]
    abst = gen_hds.gen_abst(lst, True)
    
    hdl.write(abst)
    hdl.finish()

def get_f4m(hdl, refname):
    # согласно FlashMediaManifestFileFormatSpecification.pdf
    hdl.set_header("Content-Type", "application/f4m+xml")
    # :TRICKY: не нужно вроде
    disable_caching(hdl)
    
    is_live = True
    f4m = gen_hds.gen_f4m(refname, gen_hds.PLAYLIST_ABST, is_live)
    hdl.write(f4m)
    
#
# выдача
#

def get_channels():
    """ Прочитать информацию о мультикаст-каналах Bradbury из tv_bl.csv"""
    # 1 - наилучшее качество, 3 - наихудшее
    num = 1 # 2
    def mc_out(suffix):
        return "mc_%s_out_%s" % (num, suffix)
    req_clns = ["refname", mc_out("address"), mc_out("port")]

    rn_dct = {}
    names_dct = {}
    
    # :REFACTOR: all_channels()
    with list_bl_tv.make_tbl_clns(req_clns) as (tbl, clns):
        for row in tbl:
            def get_val(name):
                return row[clns[name]]
            def mc_out_val(name):
                return get_val(mc_out(name))
            
            refname = get_val("refname")
            if refname and list_bl_tv.is_streaming(row, clns):
                addr = "udp://%s:%s" % (mc_out_val("address"), mc_out_val("port"))
                rn_dct[refname] = addr
                
                name = list_bl_tv.channel_name(row, clns)
                names_dct[name] = refname
                
    return rn_dct, names_dct

# словарь "имя канала" => адрес входящий адрес вещания
RefnameDict = get_channels()[0]

# словарь (имя канала, тип вещания) => chunk_range
ChunkRangeDict = {}
def init_crd():
    # :TODO: создавать по требованию (ленивая инициализация)
    for r_t_b in r_t_b_iter(RefnameDict):
        ChunkRangeDict[r_t_b] = make_struct(
            is_started = False,
            on_first_chunk_handlers = [],
            r_t_b = r_t_b,
            
            stop_signal = False
        )
init_crd()

ActivitySet = set()

def raise_error(status):
    raise tornado.web.HTTPError(status)
def make_get_handler(match_pattern, get_handler, is_async=True):
    """ Альтернатива созданию торнадовского обработчика: get_handler - обработка
        GET-запроса """
    class Handler(tornado.web.RequestHandler):
        pass
    
    Handler.get = tornado.web.asynchronous(get_handler) if is_async else get_handler
    return match_pattern, Handler

def get_cr(r_t_b):
    chunk_range = ChunkRangeDict.get(r_t_b)
    if not chunk_range:
        raise_error(404)
    return chunk_range

def force_chunking(chunk_range):
    """ Начать вещание канала по требованию """
    if not chunk_range.is_started:
        start_chunking(chunk_range)
        
    return chunk_range.is_started

from app.models.dvr_reader import DVRReader
dvr_reader = DVRReader(host=environment.dvr_host)

@tornado.web.asynchronous
@gen.engine
def get_dvr(hdl, asset, startstamp, duration, bitrate):
    startstamp = int(startstamp)
    bitrate = int(bitrate)
    payload = yield gen.Task(
        dvr_reader.load,
        asset=asset,
        bitrate=bitrate,
        startstamp=startstamp,
    )
    hdl.finish(payload)

from tornado import template

@tornado.web.asynchronous
def get_playlist_multibitrate(hdl, asset, startstamp=None, duration=None):
    hdl.set_header("Content-Type", "application/vnd.apple.mpegurl")

    if (
        startstamp is None and duration is not None or
        startstamp is not None and duration is None
    ):
        raise_error(400)

    loader = template.Loader(os.path.join(
        os.path.abspath(os.path.dirname(__file__)),
        'app',
        'templates',
    ))
    playlist = loader.load('multibitrate/playlist.m3u8').generate(
        asset=asset,
        bitrates=environment.bitrates,
    )

    hdl.finish(playlist)

@tornado.web.asynchronous
@gen.engine
def get_playlist_dvr(hdl, asset, bitrate, extension, startstamp, duration):
    hdl.set_header('Content-Type', 'application/vnd.apple.mpegurl')
    playlist_data = yield gen.Task(
        dvr_reader.range,
        asset=asset,
        bitrate=bitrate,
        startstamp=startstamp,
        duration=duration,
    )

    if playlist_data:
        playlist = dvr_reader.generate_playlist(
            host=hdl.request.host,
            asset=asset,
            startstamps_durations=[
                (r['startstamp'], r['duration'])
                for r in playlist_data
            ],
            bitrate=bitrate,
        )
        hdl.finish(playlist)
    else:
        raise_error(404)

Fmt2Typ = {
    "m3u8": StreamType.HLS,
    gen_hds.PLAYLIST_ABST: StreamType.HDS,
}

def get_playlist_singlebitrate(hdl, asset, bitrate, extension, startstamp=None, duration=None):
    """ Обработчик выдачи плейлистов playlist.m3u8 и manifest.f4m """
    bitrate = int(bitrate)

    if (
        startstamp is None and duration is not None or
        startstamp is not None and duration is None
    ):
        raise_error(400)

    if startstamp is not None and duration is not None:
        get_playlist_dvr(hdl, asset, bitrate, extension, startstamp, duration)
        return

    typ = Fmt2Typ[extension]
    
    r_t_b = r_t_b_key(asset, typ, bitrate)
    chunk_range = get_cr(r_t_b)
    if not force_chunking(chunk_range):
        # например, из-за сигнала остановить сервер
        raise_error(503)

    serve_pl = {
        StreamType.HLS: serve_hls_pl,
        StreamType.HDS: serve_hds_pl,
    }[typ]
    if may_serve_pl(chunk_range):
        serve_pl(hdl, chunk_range)
    else:
        chunk_range.on_first_chunk_handlers.append(functools.partial(serve_pl, hdl, chunk_range))
        
    ActivitySet.add(r_t_b)

#
# остановка сервера
#
import signal

def kill_cr(cr):
    """ Послать сигнал chunker'у (ffmpeg) прекратить работу """
    # HDS пока не порождает процесс, так что 
    # stop_signal ему нужен всегда
    cr.stop_signal = True
    
    if cr.r_t_b.typ == StreamType.HLS:
        os.kill(cr.pid, signal.SIGTERM)

def on_signal(_signum, _ignored_):
    """ Прекратить работу сервера show_tv по Ctrl+C """
    print("Request to stop ...")
    # :TRICKY: вариант с ожиданием завершения оставшихся работ
    # есть на http://tornadogists.org/4643396/ , нам пока не нужен
    Globals.stop_streaming = True
    
    stop_lst = []
    for cr in ChunkRangeDict.values():
        if cr.is_started:
            kill_cr(cr)
            stop_lst.append(cr.r_t_b)
    
    if stop_lst:
        Globals.stop_lst = set(stop_lst)
    else:
        IOLoop.stop()
    
#
# вещание по запросу
#

if is_test:
    stream_always_lst = ['pervyj']
else:
    #stream_always_lst = ['pervyj', 'rossia1', 'ntv', 'rossia24', 'peterburg5', 'rbktv']
    stream_always_lst = ['pervyj']

def stop_inactives():
    """ Прекратить вещание каналов, которые никто не смотрит в течении STOP_PERIOD=10 минут """
    for r_t_b, cr in ChunkRangeDict.items():
        if cr.is_started and r_t_b not in ActivitySet and r_t_b.refname not in stream_always_lst:
            print("Stopping inactive:", r_t_b)
            kill_cr(cr)
            
    ActivitySet.clear()
    set_stop_timer()

STOP_PERIOD = 600 # 10 минут
def set_stop_timer():
    period = datetime.timedelta(seconds=STOP_PERIOD)
    IOLoop.add_timeout(period, stop_inactives)

def main():
    logger = logging.getLogger('stream')
    logger.info(
        '\n'
        'Fahrenheit 451 mediaserver. Frontend OTT server.\n'
        'Copyright Bradbury Lab, 2013\n'
        'Listens at 0.0.0.0:{0}\n'
        .format(PORT)
    )
    
    # if is_test:
    #     # для Tornado, чтоб на каждый запрос отчитывался
    #     logger = logging.getLogger()
    #     logger.setLevel(logging.INFO)
        
    for r_t_b in r_t_b_iter(stream_always_lst):
        # :TODO: по умолчанию HDS пока не готово
        use_hds = get_env_value("use_hds", False)
        if use_hds or (r_t_b.typ != StreamType.HDS):
            start_chunking(ChunkRangeDict[r_t_b])
             
    for sig in [signal.SIGTERM, signal.SIGINT]:
        signal.signal(sig, on_signal)
    set_stop_timer()
    
    # обработчики
    handlers = [
        make_get_handler(r"^/(?P<asset>\w+)/?(?P<startstamp>\d+)/?(?P<duration>\d+)?/smil.m3u8", get_playlist_multibitrate),
        make_get_handler(r"^/(?P<asset>[-\w]+)/?(?P<startstamp>\d+)?/(?P<duration>\d+)/?(?P<bitrate>\d+)?\.(?P<extension>m3u8|abst)", get_playlist_singlebitrate),
        # HDS: выдаем плейлист отдельно от манифеста, иначе клиент плейлист не обновляет => live не играет
        # :TODO: нужна проверка существования канала такая же, что и в get_playlist()
        make_get_handler(r"/(?P<refname>[-\w]+)/manifest.f4m", get_f4m, False),
        make_get_handler(r"^/(?P<asset>\w+)/(?P<startstamp>\d+)/(?P<duration>\d+)/(?P<bitrate>\d+)\.ts", get_dvr),
    ]
    def make_static_handler(chunk_dir):
        return r"/%s/(.*)" % chunk_dir, tornado.web.StaticFileHandler, {"path": out_fpath(chunk_dir)}
        
    for refname in RefnameDict:
        handlers.append(
            make_static_handler(refname),
        )
    
    # всеразрешающий crossdomain.xml для HDS
    def get_cd_xml(hdl):
        hdl.write(gen_hds.least_restrictive_cd_xml())
    handlers.append(make_get_handler(r"/crossdomain.xml", get_cd_xml, False))
    
    application = tornado.web.Application(handlers)
    application.listen(PORT)

    IOLoop.start()

def get_channel_addr(req_channel):
    rn_dct, names_dct = get_channels()
    
    res = names_dct.get(req_channel)
    if res:
        res = rn_dct[res]
    return res
    
if __name__ == "__main__":
    if True:
        main()
        
    if False:
        print(get_channel_addr("Первый канал"))
