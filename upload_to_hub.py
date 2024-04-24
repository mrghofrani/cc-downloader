import logging
from datasets import load_dataset
from configparser import ConfigParser


secrets = ConfigParser()
with open("secrets.ini") as f:
    secrets.read_file(f)

CC_VERSION = "2023-23"
OUTPUT_FOLDER = f"OUTPUT-CC-{CC_VERSION}"
LOG_FILENAME = "upload_to_hub.log"
HUGGING_FACE_PATH = f"mrghofrani/cc-{CC_VERSION}-fa"
HUGGING_FACE_TOKEN = secrets["huggingface"]["token"]

logging.basicConfig(
    format="%(asctime)s,%(msecs)d %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.DEBUG,
    handlers=[logging.FileHandler(LOG_FILENAME), logging.StreamHandler()],
)


dataset = load_dataset("json", data_files=f"{OUTPUT_FOLDER}/*.jsonl")
dataset.push_to_hub(HUGGING_FACE_PATH, token=HUGGING_FACE_TOKEN)
logging.info("Hola!")
