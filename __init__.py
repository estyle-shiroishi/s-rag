import sys
from pathlib import Path

root_dir = Path(__file__).parent.absolute()

if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))
