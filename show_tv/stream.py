#!/usr/bin/env python3
# coding: utf-8

import os
import o_p, s_
import math
import list_bl_tv

import getpass
IsTest = getpass.getuser() in ["muravyev", "ilya"]

PORT = 8910

def int_ceil(float_):
    return int(math.ceil(float_))

# объект самого класса object минималистичен, поэтому не содержит
# __dict__ (который и дает функционал атрибутов); а вот наследники
# получают __dict__ по умолчанию, если только в их описании нет __slots__ - 
# явного списка атрибутов, которые должен иметь класс
class Struct(object):
    pass

def make_struct(**kwargs):
    """ Сделать объект с атрибутами """
    # вообще, для спец. случаев, требующих оптимизации по памяти, можно
    # установить __slots__ равным kwargs.keys()
    stct = Struct()
    stct.__dict__.update(kwargs)
    return stct

def main():
    rn_dct = get_channels()[0]
    # в Bradbury обычно 12 секунд GOP
    std_chunk_dur = 12
    # формат фрагментов
    num_sz = 8
    chunk_tmpl = "out%%0%sd.ts" % num_sz
    def chunk_name(i):
        return chunk_tmpl % i

    def make_cr(rname):
        # :REFACTOR: make_struct()
        class chunk_range:
            is_started = False
            on_first_chunk_handlers = []
            refname = rname
            
            stop_signal = False
        return chunk_range
        
    cr_dct = {}
    for refname in rn_dct:
        cr_dct[refname] = make_cr(refname)
        # не стартуем автоматом
        #start_chunking()

    if IsTest:
        prefix_dir = os.path.expanduser("~/opt/bl/f451")
        out_dir = o_p.join(prefix_dir, 'tmp/out_dir')
    else:
        out_dir = "/home/ilil/show_tv/out_dir"

    def chunk_fpath(chunk_dir, *fname):
        return o_p.join(out_dir, chunk_dir, *fname)

    def remove_chunks(rng, cr):
        for i in rng:
            fname = chunk_fpath(cr.refname, chunk_name(i))
            os.unlink(fname)
            
    def ready_chunks(chunk_range):
        """ Кол-во дописанных до конца фрагментов """
        return chunk_range.end - chunk_range.beg - 1
    
    def may_serve_pl(cnt):
        return cnt >= 1

    def channel_dir(chunk_dir):
        return chunk_fpath(chunk_dir)
    
    def run_chunker(src_media_path, chunk_dir, on_new_chunk, on_stop_chunking, is_batch=False):
        o_p.force_makedirs(channel_dir(chunk_dir))
        
        if IsTest:
            ffmpeg_bin = os.path.expanduser("~/opt/src/ffmpeg/git/ffmpeg/objs/inst/bin/ffmpeg")
        else:
            ffmpeg_bin = "/home/ilil/show_tv/ffmpeg/objs/inst/bin/ffmpeg"
            
        # :TRICKY: так отлавливаем сообщение от segment.c вида "starts with packet stream"
        log_type = "debug"
        in_opts = "-i " + src_media_path
        emulate_live  = False # True # 
        if IsTest and emulate_live and not is_batch:
            # эмулируем выдачу видео в реальном времени
            in_opts = "-re " + in_opts
        bl_options = "-segment_time %s" % std_chunk_dur
        cmd = "%(ffmpeg_bin)s -v %(log_type)s %(in_opts)s -map 0 -codec copy -f ssegment %(bl_options)s" % locals()
        if IsTest:
            #cmd += " -segment_list %(out_dir)s/playlist.m3u8" % locals()
            pass

        cmd += " %s" % chunk_fpath(chunk_dir, chunk_tmpl)
        #print(cmd)
        
        import tornado.process
        Subprocess = tornado.process.Subprocess
    
        STREAM = Subprocess.STREAM
        ffmpeg_proc = Subprocess(cmd, stdout=STREAM, stderr=STREAM, shell=True)
     
        import re
        segment_sign = re.compile(b"segment:'(.+)' starts with packet stream:.+pts_time:(?P<pt>[\d,\.]+)")
        def on_line(line):
            m = segment_sign.search(line)
            if m:
                #print("new segment:", line)
                chunk_dur = float(m.group("pt"))
                on_new_chunk(chunk_dur)
    
        line_sep = re.compile(br"(\n|\r\n?).", re.M)
        class errdat:
            txt = b''
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
    
        def on_proc_exit(exit_code):
            #print("exit_code:", exit_code)
            set_stop(False)
        ffmpeg_proc.set_exit_callback(on_proc_exit)
        
        return ffmpeg_proc.pid

    def test_src_path():
        return list_bl_tv.make_path("pervyj.ts") # o_p.join(prefix_dir, 'show_tv/pervyj.ts')

    main.stop_streaming = False
    def start_chunking(chunk_range):
        if main.stop_streaming:
            return
        
        # инициализация
        chunk_range.is_started = True
        
        chunk_range.beg = 0
        chunk_range.end = 0
        chunk_range.start_times = []
        chunk_range.ad = None

        def on_new_chunk(chunk_dur):
            chunk_range.start_times.append(chunk_dur)
            chunk_range.end += 1
    
            cnt = ready_chunks(chunk_range)
            if may_serve_pl(cnt):
                hdls = chunk_range.on_first_chunk_handlers
                chunk_range.on_first_chunk_handlers = []
                for hdl in hdls:
                    hdl()
            
            max_total = 72 # максимум столько секунд храним
            max_cnt = int_ceil(float(max_total) / std_chunk_dur)
            diff = cnt - max_cnt
            if diff > 0:
                old_beg = chunk_range.beg
                chunk_range.beg += diff
                del chunk_range.start_times[:diff]
                remove_chunks(range(old_beg, chunk_range.beg), chunk_range)
     
        def on_stop_chunking():
            chunk_range.is_started = False
            remove_chunks(range(chunk_range.beg, chunk_range.end), chunk_range)

            may_restart = not chunk_range.stop_signal
            if chunk_range.stop_signal:
                chunk_range.stop_signal = False

            if main.stop_streaming:
                pids = main.stop_pids
                pids.discard(chunk_range.pid)
                if not pids:
                    ioloop.stop()
            else:
                if may_restart:
                    start_chunking(chunk_range)
                    
        if IsTest:
            src_media_path = test_src_path()
        else:
            src_media_path = rn_dct[refname]
        
        chunk_range.pid = run_chunker(src_media_path, chunk_range.refname, on_new_chunk, on_stop_chunking)
    
    #
    # выдача
    #
    def written_chunks(chunk_range):
        return range(chunk_range.beg, chunk_range.end-1)
    def chunk_duration(i, chunk_range):
        i -= chunk_range.beg
        st = chunk_range.start_times
        return st[i+1] - st[i]

    def serve_pl(hdl, chunk_range):
        # :TRICKY: по умолчанию tornado выставляет
        # "text/html; charset=UTF-8", и вроде как по 
        # документации HLS, http://tools.ietf.org/html/draft-pantos-http-live-streaming-08 ,
        # такое возможно, если путь оканчивается на .m3u8 , но в реальности
        # Safari/IPad такое не принимает (да и Firefox/Linux тоже)
        hdl.set_header("Content-Type", "application/vnd.apple.mpegurl")
        
        write = hdl.write
        # EXT-X-TARGETDURATION - должен быть, и это
        # должен быть максимум
        max_dur = 0
        for i in written_chunks(chunk_range):
            max_dur = max(chunk_duration(i, chunk_range), max_dur)
        # по спеке это должно быть целое число, иначе не работает (IPad)
        max_dur = int_ceil(max_dur)
        
        # EXT-X-MEDIA-SEQUENCE - номер первого сегмента,
        # нужен для указания клиенту на то, что список живой,
        # т.е. его элементы будут добавляться/исчезать по FIFO
        #
        # если chunk_range не использовать здесь, то EvalFormat 
        # его не найдет; неиспользуемые неглобальные функции - аналогично
        beg = chunk_range.beg
        write("""#EXTM3U
#EXT-X-VERSION:3
#EXT-X-ALLOW-CACHE:NO
#EXT-X-TARGETDURATION:%(max_dur)s
#EXT-X-MEDIA-SEQUENCE:%(beg)s
""" % s_.EvalFormat())
                
        for i in written_chunks(chunk_range):
            name = chunk_name(i)
            # используем %f (6 знаков по умолчанию) вместо %s, чтобы на 
            # '%s' % 0.0000001 не получать '1e-07'
            write("""#EXTINF:%(chunk_duration(i, chunk_range))f,
%(name)s
""" % s_.EvalFormat())
            
        # а вот это для live не надо
        #write("#EXT-X-ENDLIST")
        
        hdl.finish()
    
    activity_set = set()
    
    import tornado.web
    import functools
    
    def raise_error(status):
        raise tornado.web.HTTPError(status)
    def make_get_handler(match_pattern, get_handler):
        class Handler(tornado.web.RequestHandler):
            pass
        
        Handler.get = tornado.web.asynchronous(get_handler)
        return match_pattern, Handler

    def get_cr(refname):
        chunk_range = cr_dct.get(refname)
        if not chunk_range:
            raise_error(404)
        return chunk_range
    
    def force_chunking(chunk_range):
        if not chunk_range.is_started:
            start_chunking(chunk_range)
            
        return chunk_range.is_started
    
    def get_playlist(hdl, refname):
        chunk_range = get_cr(refname)
        if not force_chunking(chunk_range):
            # например, из-за сигнала остановить сервер
            raise_error(503)
            
        if may_serve_pl(ready_chunks(chunk_range)):
            serve_pl(hdl, chunk_range)
        else:
            chunk_range.on_first_chunk_handlers.append(functools.partial(serve_pl, hdl, chunk_range))
            
        activity_set.add(chunk_range.refname)

    handlers = [
        make_get_handler(r"/([-\w]+)/playlist.m3u8", get_playlist),
    ]
    def make_static_handler(chunk_dir):
        return r"/%s/(.*)" % chunk_dir, tornado.web.StaticFileHandler, {"path": channel_dir(chunk_dir)}
        
    for refname in rn_dct:
        handlers.append(
            make_static_handler(refname),
        )
    
    #
    # вставка рекламы
    #
    def remove_ad(chunk_dir):
        o_p.del_any_fpath(channel_dir(chunk_dir))
    
    ad_s_prefix = "ad/static"
    import t_
    def get_run_ad(hdl):
        refname = hdl.get_argument("channel")
        chunk_range = get_cr(refname)
        
        pr = ad_s_prefix
        ts = t_.utcnow_str().replace(":", "-") # чтоб с портом никто не спутал
        dir_prefix = "%(pr)s/%(ts)s_%(refname)s" % locals()
        
        idx = 0
        while True:
            chunk_dir = "%(dir_prefix)s-%(idx)s" % locals() if idx else dir_prefix
            if o_p.exists(channel_dir(chunk_dir)):
                idx += 1
            else:
                break
        
        # :TRICKY: длительность последнего сегмента невозможно вычислить, потому что
        # ffmpeg не выводит это в stderr - последний chunk не показываем
        # :TRICKY: если "посередине" остановить сервер, то не будут удалены временные
        # файлы рекламы - фиг с ними, тем более не решено пока, один и тот же процесс
        # будет управлять и каналами, и контентом рекламы, или нет
        start_times = []
        def on_new_chunk(chunk_dur):
            start_times.append(chunk_dur)
        def on_stop_chunking():
            res, err_msg = False, "Uknown error"
            if not start_times:
                err_msg = "Bad ad source"
            elif not force_chunking(chunk_range):
                err_msg = "force_chunking() failed"
            else:
                res = True
                chunk_range.ad = make_struct(
                    chunk_dir   = chunk_dir,
                    start_times = start_times,
                    idx         = chunk_range.end-1
                )
            
            hdl.write("Ok" if res else err_msg)
            hdl.finish()
            if not res:
                remove_ad(chunk_dir)
            
        src_media_path = test_src_path()
        run_chunker(src_media_path, chunk_dir, on_new_chunk, on_stop_chunking, is_batch=True)
    
    handlers.extend([
        make_get_handler(r"/ad/run", get_run_ad),
        make_static_handler(ad_s_prefix), # пример: /ad/static/2013-09-08T17:24:38.383278Z_ntv/out00000000.ts
    ])
    
    application = tornado.web.Application(handlers)
    application.listen(PORT)

    import tornado.ioloop
    ioloop = tornado.ioloop.IOLoop.instance()
    import signal

    def kill_cr(cr):
        os.kill(cr.pid, signal.SIGTERM)
    
    def on_signal(signum, _ignored_):
        print("Request to stop ...")
        # :TRICKY: вариант с ожиданием завершения оставшихся работ
        # есть на http://tornadogists.org/4643396/ , нам пока не нужен

        main.stop_streaming = True
        
        stop_lst = []
        for cr in cr_dct.values():
            if cr.is_started:
                kill_cr(cr)
                stop_lst.append(cr.pid)
        
        if stop_lst:
            main.stop_pids = set(stop_lst)
        else:
            ioloop.stop()
        
    for sig in [signal.SIGTERM, signal.SIGINT]:
        signal.signal(sig, on_signal)

    def stop_inactives():
        for refname, cr in cr_dct.iteritems():
            if cr.is_started and refname not in activity_set:
                print("Stopping inactive:", refname)
                cr.stop_signal = True
                kill_cr(cr)
                
        activity_set.clear()
        set_stop_timer()

    stop_period = 600 # 10 минут
    import datetime
    period = datetime.timedelta(seconds=stop_period)
    def set_stop_timer():
        ioloop.add_timeout(period, stop_inactives)
    set_stop_timer()

    ioloop.start()

def get_channels():
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
