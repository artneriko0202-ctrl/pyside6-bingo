import sys
from cx_Freeze import setup, Executable

build_exe_options = {
    "packages": [
        "PySide6.QtWidgets",
        "PySide6.QtGui",
        "PySide6.QtCore",
        "sqlite3",
        "json",
        "random",
        "pathlib",
        "collections",
        "datetime",
    ],
    "include_files": [
        ("config.json", "config.json"),
    ],
    "excludes": [
        "tkinter",
        "unittest",
        "email",
        "http",
        "xml",
        "pydoc",
    ],
}

base = "gui" if sys.platform == "win32" else None

setup(
    name="wakattenai-bingo",
    version="1.0.0",
    description="分かってない勢ビンゴ - PySide6 ビンゴアプリケーション",
    options={"build_exe": build_exe_options},
    executables=[
        Executable(
            "main.py",
            base=base,
            target_name="分かってない勢ビンゴ.exe",
        )
    ],
)
