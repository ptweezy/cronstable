"""Check an installed wheel in isolation from the source checkout."""

import subprocess
import tempfile
import venv
from pathlib import Path


def main():
    wheels = list(Path("dist").glob("*.whl"))
    if len(wheels) != 1:
        raise ValueError("Expected exactly one wheel")
    wheel = wheels[0].resolve()
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        venv.create(root / "venv", with_pip=True)
        python = root / "venv/bin/python"
        subprocess.run(
            [python, "-m", "pip", "install", wheel], cwd=root, check=True
        )
        subprocess.run(
            [python, "-I", "-m", "cronstable", "--version"],
            cwd=root,
            check=True,
        )
        subprocess.run(
            [
                python,
                "-I",
                "-c",
                """
from importlib.resources import files
import cronstable
assert 'site-packages' in cronstable.__file__, cronstable.__file__
assert files('cronstable').joinpath('web/index.html').is_file()
notice = files('cronstable').joinpath('licenses/THIRD-PARTY-NOTICES.txt')
assert notice.is_file()
""",
            ],
            cwd=root,
            check=True,
        )


if __name__ == "__main__":
    main()
