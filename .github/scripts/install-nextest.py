#!/usr/bin/env python3
"""Install the repository's checksum-pinned Linux nextest in an explicit directory."""

import hashlib
import io
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tarfile
import urllib.request


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: install-nextest.py <installation-directory>")
    root = Path(__file__).resolve().parents[2]
    pin = json.loads((root / ".config/test-tools.json").read_text())["nextest"]
    architecture = {"aarch64": "aarch64", "x86_64": "x86_64"}.get(platform.machine())
    if platform.system() != "Linux" or architecture is None:
        raise SystemExit("the qualified runner requires Linux aarch64 or x86_64")
    target = f"{architecture}-unknown-linux-gnu"
    version = pin["version"]
    archive = f"cargo-nextest-{version}-{target}.tar.gz"
    url = f"https://github.com/nextest-rs/nextest/releases/download/cargo-nextest-{version}/{archive}"
    with urllib.request.urlopen(url, timeout=60) as response:
        data = response.read()
    if hashlib.sha256(data).hexdigest() != pin["archives"][target]:
        raise SystemExit("nextest archive checksum mismatch")
    destination = Path(sys.argv[1]).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as bundle:
        binary = bundle.extractfile("cargo-nextest")
        if binary is None:
            raise SystemExit("nextest archive has no executable")
        temporary = destination / f".cargo-nextest-{os.getpid()}"
        temporary.write_bytes(binary.read())
    temporary.chmod(0o755)
    os.replace(temporary, destination / "cargo-nextest")
    subprocess.run([destination / "cargo-nextest", "nextest", "--version"], check=True)
    print(f"Installed checksum-verified cargo-nextest {version} in {destination}")


if __name__ == "__main__":
    main()
