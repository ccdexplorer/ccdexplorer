"""verify_module hands cargo the module it saved, and says what cargo said.

A third party's testnet module (built with the wasm32v1 image) was shown as
"Unverifiable: the source does not correspond to the module". It never got
compared: verify-build was given /app/bases/ccdexplorer/ms_modules/tmp/<ref>.out
-- a path worked out from the module's own location, which stopped being where
anything is written when the code moved into the Polylith layout -- and died
with "Failed reading file". Every non-zero exit was then reported as a
mismatch, so the one line that would have said so was thrown away.
"""

import io
import os
import tarfile
from types import SimpleNamespace

import pytest

from ccdexplorer.domain.generic import NET
from ccdexplorer.ms_modules.subscriber.module import (
    Module,
    cargo_concordium_for,
    verify_failure_explanation,
)

REF = "196930b8de58a1ef92a97480322cd0c0051c99be3fdb2f459567e9c426e6fdde"
WASM32V1_IMAGE = "concordium/verifiable-sc-wasm32v1:1"
OLD_IMAGE = "concordium/verifiable-sc:1.85.0"


def _tar_with(*names: str) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as tar:
        for name in names:
            data = b"fn main() {}"
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


class _Verifier(Module):
    """Module's verify_module with the outside world replaced.

    Nothing here knows where the module file should be: that is what the
    tests are checking, so the fakes only record what they were asked.
    """

    def __init__(self, saved: list):
        self.motor_mainnet = self.motor_testnet = {}
        self.saved = saved

    async def save_and_send(self, net, module_ref, db_to_use, verification):
        self.saved.append(verification)


class _Client:
    """save_module writes where the real one does: tmp/<ref>.out in the cwd."""

    def save_module(self, net, module_ref):
        os.makedirs("tmp", exist_ok=True)
        with open(f"tmp/{module_ref}.out", "wb") as f:
            f.write(b"module")


class _Response:
    def __init__(self, content: bytes):
        self.content = content

    def raise_for_status(self):
        pass


@pytest.fixture
def world(tmp_path, monkeypatch):
    """A working directory, a recording subprocess.run, and a tarball to serve."""
    monkeypatch.chdir(tmp_path)
    calls: list[dict] = []
    state = SimpleNamespace(
        calls=calls,
        build_info=(
            f"Build image used: {WASM32V1_IMAGE}\n"
            "Build command used: cargo build\n"
            "Hash of the archive: abc\n"
            "Link to source code: https://example.test/src.tar\n"
        ),
        verify_returncode=0,
        verify_stderr="Source and module match.\n",
        tarball=_tar_with("pkg-0.1.0/src/lib.rs"),
    )

    def fake_run(args, **kwargs):
        record = {
            "args": list(args),
            "cwd": kwargs.get("cwd"),
            "module_exists": None,
        }
        if "verify-build" in args:
            module_arg = args[args.index("--module") + 1]
            record["module_exists"] = os.path.exists(module_arg)
            record["module_arg"] = module_arg
            calls.append(record)
            return SimpleNamespace(
                returncode=state.verify_returncode, stderr=state.verify_stderr, stdout=""
            )
        calls.append(record)
        return SimpleNamespace(returncode=0, stderr=state.build_info, stdout="")

    class _Http:
        async def get(self, url, follow_redirects=False):
            return _Response(state.tarball)

    monkeypatch.setattr("ccdexplorer.ms_modules.subscriber.module.subprocess.run", fake_run)
    monkeypatch.setattr(
        "ccdexplorer.ms_modules.subscriber.module.httpx.AsyncClient", lambda: _Http()
    )
    return state


async def _verify(world) -> list:
    saved: list = []
    await _Verifier(saved).verify_module(NET.TESTNET, _Client(), REF)  # type: ignore[arg-type]
    return saved


# --- the module file cargo is pointed at ------------------------------------


async def test_verify_build_is_given_the_file_that_was_saved(world):
    await _verify(world)

    verify = next(c for c in world.calls if "module_arg" in c)
    assert verify["module_exists"] is True


async def test_the_module_path_does_not_depend_on_the_working_directory_of_cargo(world, tmp_path):
    """cargo runs inside the unpacked source, so a relative path would break."""
    await _verify(world)

    verify = next(c for c in world.calls if "module_arg" in c)
    assert os.path.isabs(verify["module_arg"])
    assert verify["module_arg"] == str(tmp_path / "tmp" / f"{REF}.out")


# --- what is reported when cargo fails --------------------------------------


async def test_a_cargo_failure_reports_what_cargo_said(world):
    world.verify_returncode = 1
    world.verify_stderr = "Error: Failed reading file: No such file or directory\n"

    (verification,) = await _verify(world)

    assert verification.verified is False
    assert "Failed reading file" in verification.explanation
    assert "does not correspond" not in verification.explanation


async def test_a_real_mismatch_is_still_reported_as_one(world):
    world.verify_returncode = 1
    world.verify_stderr = "Error: Source and module do not match.\n"

    (verification,) = await _verify(world)

    assert "do not match" in verification.explanation


async def test_a_match_is_verified(world):
    (verification,) = await _verify(world)

    assert verification.verified is True
    assert verification.explanation == "Source and module match."


def test_the_explanation_is_the_end_of_stderr_without_colour_codes():
    noisy = "\x1b[31mnoise\x1b[0m\n" * 200 + "\x1b[1;31mError:\x1b[0m the real reason\n"

    explanation = verify_failure_explanation(noisy)

    assert "\x1b" not in explanation
    assert explanation.endswith("the real reason")
    assert len(explanation) <= 600


def test_an_empty_stderr_still_says_something():
    assert verify_failure_explanation("") != ""


# --- the archive is somebody else's -------------------------------------------


async def test_an_archive_cannot_write_outside_its_folder(world, tmp_path):
    """The tarball comes from a link anyone can embed in a module."""
    world.tarball = _tar_with("pkg-0.1.0/src/lib.rs", "../../escaped.txt")

    saved = await _verify(world)

    assert not (tmp_path / "escaped.txt").exists()
    assert not (tmp_path.parent / "escaped.txt").exists()
    assert saved  # the run ended with a verdict, whichever it was


# --- which cargo-concordium builds which image ---------------------------------


def test_the_wasm32v1_image_is_built_by_cargo_concordium_5():
    """4.x and 5.x images are not interchangeable in either direction."""
    assert cargo_concordium_for(WASM32V1_IMAGE) == "cargo-concordium-5"


def test_older_images_stay_on_the_cargo_concordium_they_were_built_with():
    assert cargo_concordium_for(OLD_IMAGE) == "cargo-concordium"


def test_an_unknown_image_falls_back_to_the_default():
    assert cargo_concordium_for(None) == "cargo-concordium"


async def test_verify_build_runs_the_cargo_that_matches_the_image(world):
    await _verify(world)

    verify = next(c for c in world.calls if "module_arg" in c)
    assert verify["args"][0] == "cargo-concordium-5"


# --- the explanation is cargo's last error, not the end of a page of warnings -----

REAL_MISMATCH_STDERR = (
    "   Compiling concordium v0.1.0 (/b)\n"
    "warning: unused variable: `state`\n"
    "   --> src/lib.rs:466:9\n"
    "    |\n"
    "466 |         state: &mut State,\n"
    "    |         ^^^^^ help: if this is intentional, prefix it with an underscore\n"
    "    Finished release [optimized] target(s) in 47.72s\n"
    "Error: \n"
    "The source does not correspond to the module.\n"
)


def test_a_real_mismatch_is_reported_as_the_verdict_cargo_gave():
    """The first version cut the last 500 characters wherever they began, which
    on a build with warnings started mid-way through a source snippet."""
    explanation = verify_failure_explanation(REAL_MISMATCH_STDERR)

    assert explanation == "The source does not correspond to the module."


def test_another_error_is_cargos_last_error_not_the_noise_before_it():
    stderr = (
        "warning: unused variable\n    |\n    ^^^^\n"
        "Error: Failed reading file: No such file or directory (os error 2)\n\n"
        "Caused by:\n    No such file or directory (os error 2)\n"
    )

    explanation = verify_failure_explanation(stderr)

    assert explanation.startswith("Verification did not complete: Failed reading file")
    assert "unused variable" not in explanation
    assert "\n" not in explanation
