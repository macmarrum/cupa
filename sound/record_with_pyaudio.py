#!/usr/bin/python3
# Copyright 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: Apache-2.0
import tomllib
from pathlib import Path

import pyaudio
import wave

me = Path(__file__)

with me.with_suffix('.toml').open('br') as fi:
    conf = tomllib.load(fi)

FORMAT = pyaudio.paInt16
CHANNELS = 1
RATE = 44100
CHUNK = 1024
RECORD_SECONDS = conf.get('record_seconds', 5)
OUTPUT_FILENAME = conf.get('output_filename', 'output.wav')

audio = pyaudio.PyAudio()
stream = audio.open(format=FORMAT, channels=CHANNELS, rate=RATE, input=True, frames_per_buffer=CHUNK)
print(f"Recording for {RECORD_SECONDS} seconds...")
frames = []
for _ in range(0, int(RATE / CHUNK * RECORD_SECONDS)):
    data = stream.read(CHUNK)
    frames.append(data)
print('Finished recording')
stream.stop_stream()
stream.close()
audio.terminate()

with wave.open(OUTPUT_FILENAME, 'wb') as wf:
    wf.setnchannels(CHANNELS)
    wf.setsampwidth(audio.get_sample_size(FORMAT))
    wf.setframerate(RATE)
    wf.writeframes(b''.join(frames))
