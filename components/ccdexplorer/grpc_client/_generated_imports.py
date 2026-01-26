from __future__ import annotations

from pathlib import Path
import os
import sys


def ensure_grpc_gen_on_path() -> None:
    env_path = os.environ.get("CCD_GRPC_GEN_PATH")
    if env_path:
        gen_dir = Path(env_path)
    else:
        gen_dir = None
        for base in (Path.cwd(), Path(__file__).resolve()):
            for parent in [base] + list(base.parents):
                candidate = parent / "development" / "grpc_gen"
                if candidate.is_dir():
                    gen_dir = candidate
                    break
            if gen_dir is not None:
                break

    if gen_dir is None:
        return
    gen_dir_str = str(gen_dir)
    if gen_dir.is_dir() and gen_dir_str not in sys.path:
        sys.path.insert(0, gen_dir_str)
