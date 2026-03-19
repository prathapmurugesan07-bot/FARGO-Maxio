from pathlib import Path
import sys


CURRENT_DIR = Path(__file__).resolve().parent
SRC_DIR = CURRENT_DIR.parent

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from extract.utils import run_client_test  # noqa: E402


def main():
    run_client_test()


if __name__ == "__main__":
    main()
