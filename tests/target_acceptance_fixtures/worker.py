"""Local synthetic workers; no network, credentials or publication capability."""

import os
import sys
import time

sys.stdin.buffer.read()
mode = sys.argv[1]
if mode == "clean":
    os.write(1, b'{"ok":true}\n')
elif mode == "duplicate":
    os.write(1, b'{"ok":true}\n{"ok":true}\n')
elif mode == "oversize":
    os.write(1, b"x" * (256 * 1024 + 1))
elif mode == "bad_exit":
    os.write(1, b'{"ok":true}\n')
    sys.exit(1)
else:
    if mode == "frame_without_eos":
        os.write(1, b'{"ok":true}\n')
    while True:
        if mode == "trickle":
            os.write(1, b" ")
        time.sleep(0.01)
