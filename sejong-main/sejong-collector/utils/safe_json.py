"""Atomic JSON write 유틸리티 — 중단 시에도 원본 보존"""

import json
import os
import tempfile


def atomic_json_write(path: str, data, ensure_ascii=False, indent=2):
    """임시파일에 쓴 후 rename — 중단 시에도 원본 보존"""
    dir_name = os.path.dirname(os.path.abspath(path))
    with tempfile.NamedTemporaryFile(
        "w", dir=dir_name, suffix=".tmp",
        delete=False, encoding="utf-8"
    ) as f:
        json.dump(data, f, ensure_ascii=ensure_ascii, indent=indent)
        tmp_path = f.name
    os.replace(tmp_path, path)  # atomic on POSIX
