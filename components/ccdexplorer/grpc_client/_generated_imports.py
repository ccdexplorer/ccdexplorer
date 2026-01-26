from __future__ import annotations

from pathlib import Path
import sys


def ensure_grpc_gen_on_path() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    gen_dir = repo_root / "development" / "grpc_gen"
    gen_dir_str = str(gen_dir)
    if gen_dir.is_dir() and gen_dir_str not in sys.path:
        sys.path.insert(0, gen_dir_str)
