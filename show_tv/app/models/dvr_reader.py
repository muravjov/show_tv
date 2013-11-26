# coding: utf-8

from .dvr_base import DVRBase
import api

import struct
from io import BytesIO
from tornado import gen

@gen.engine
def call_dvr_cmd(dvr_reader, func, *args, callback, **kwargs):
    stream = yield gen.Task(api.connect, dvr_reader.host, dvr_reader.port)
    if stream:
        def on_result(data):
            callback((True, data))
            stream.close()
        func(*args, stream=stream, callback=on_result, **kwargs)
    else:
        dvr_reader.l.debug('[DVRReader] failed to connect')
        callback((False, None))

class DVRReader(DVRBase):
    commands = {
        'load': 0x02,
        'range': 0x04,
    }

    def __init__(self, cfg, host='127.0.0.1', port=7451):
        super().__init__(cfg, host, port)

    @gen.engine
    def request_range(self, asset, profile, startstamp, duration, stream, callback):
        '''
        '''
        self.l.debug('[DVRReader] range start >>>>>>>>>>>>>>>')

        if isinstance(asset, str):
            asset = asset.encode()
        if isinstance(startstamp, str):
            startstamp = int(startstamp)
        if isinstance(duration, str):
            duration = int(duration)
        endstamp = startstamp + duration

        self.l.debug('[DVRReader] => asset = {0}'.format(asset))
        self.l.debug('[DVRReader] => profile = {0}'.format(profile))
        self.l.debug('[DVRReader] => start = {0}'.format(startstamp))
        self.l.debug('[DVRReader] => end = {0}'.format(endstamp))

        pack = struct.pack(
            "=B32s6sQQ",
            # (1) (B) Команда
            self.commands['range'],
            # (2) (32s) Имя ассета
            asset,
            # (3) (6s) Профиль
            profile,
            # (4) (Q) Время начала
            startstamp,
            # (5) (Q) Время окончания
            endstamp,
        )

        yield gen.Task(stream.write, pack)
        data = yield gen.Task(stream.read_bytes, 8, streaming_callback=None)
        length = struct.unpack('=Q', data)[0]
        self.l.debug('[DVRReader]')
        self.l.debug('[DVRReader] <= length = {0}'.format(length))

        chunks_data = yield gen.Task(stream.read_bytes, length, streaming_callback=None)
        self.l.debug('[DVRReader] <= chunks_data_len = {0}'.format(len(chunks_data)))

        io = BytesIO(chunks_data)
        playlist = []
        while True:
            chunk_data = io.read(16)
            if len(chunk_data) != 16:
                break

            self.l.debug('[DVRReader]')

            (
                startstamp,
                duration,
            ) = struct.unpack('=QQ', chunk_data)
            self.l.debug('[DVRReader] <= startstamp = {0}'.format(startstamp))
            self.l.debug('[DVRReader] <= duration = {0}'.format(duration))

            playlist.append({
                'startstamp': startstamp,
                'duration': duration,
            })

        self.l.debug('[DVRReader] range finish <<<<<<<<<<<<<<<\n')

        callback(playlist)

    @gen.engine
    def load(self, asset, profile, startstamp, stream, callback):
        '''
        '''
        self.l.debug('[DVRReader] load start >>>>>>>>>>>>>>>')

        if isinstance(asset, str):
            asset = asset.encode()
        if isinstance(startstamp, str):
            startstamp = int(startstamp)

        self.l.debug('[DVRReader] => asset = {0}'.format(asset))
        self.l.debug('[DVRReader] => profile = {0}'.format(profile))
        self.l.debug('[DVRReader] => startstamp = {0}'.format(startstamp))

        pack = struct.pack(
            "=B32s6sQ",
            # (1) (B) Команда
            self.commands['load'],
            # (2) (32s) Имя ассета
            asset,
            # (3) (6s) Профиль
            profile,
            # (4) (Q) Время начала
            startstamp,
        )

        yield gen.Task(stream.write, pack)
        data = yield gen.Task(stream.read_bytes, 8, streaming_callback=None)
        length = struct.unpack('=Q', data)[0]
        self.l.debug('[DVRReader]')
        self.l.debug('[DVRReader] <= length = {0}'.format(length))

        payload = yield gen.Task(stream.read_bytes, length, streaming_callback=None)
        self.l.debug('[DVRReader] <= payloadlen = {0}'.format(len(payload)))

        self.l.debug('[DVRReader] load finish <<<<<<<<<<<<<<<\n')

        callback(payload)
