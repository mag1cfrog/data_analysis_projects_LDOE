import logging
import json
import os
from datetime import datetime


def setup_logging(log_dir, level=logging.INFO):
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file_path = os.path.join(log_dir, f"log_{current_time}.txt")

    logging.basicConfig(
        filename=log_file_path,
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler()
    console.setLevel(level)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console.setFormatter(formatter)
    logging.getLogger("").addHandler(console)

def load_config(config_file):
    with open(config_file, "r") as f:
        return json.load(f)
