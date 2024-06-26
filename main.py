import os
import gc
import re
import gzip
import json
import logging
import requests
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm
import concurrent.futures
from pymongo import MongoClient
from configparser import ConfigParser


from newspaper import Article
from warcio.archiveiterator import ArchiveIterator


from log import RollingFileHandler


secrets = ConfigParser()
with open("secrets.ini") as f:
    secrets.read_file(f)

START_PROCESSING_FROM_INDEX = 0
MAX_MANAGER_NUMS = 2
EACH_MANAGER_WORKERS = 2
MIN_DOCUMENT_LENGTH = 500
LOG_FILENAME = "file.log"
LOG_MAX_BYTES_TO_ROTATE = 30_000_000
CC_VERSION = "2023-23"
TARGET_LANGUAGE = "fas"
CC_BASE_URL = "https://data.commoncrawl.org"
CC_INDEX_URL = f"{CC_BASE_URL}/crawl-data/CC-MAIN-{CC_VERSION}/cc-index.paths.gz"
OUTPUT_FOLDER = f"OUTPUT-CC-{CC_VERSION}"
WARC_OUTPUT_FOLDER = f"WARC-CC-{CC_VERSION}"
INDEX_FOLDER = f"INDEX-CC-{CC_VERSION}"
DATABASE_NAME = CC_VERSION
DATABASE_USERNAME = secrets["database"]["username"]
DATABASE_PASSWORD = secrets["database"]["password"]
DATABASE_URL = "cluster0.ewzdedv.mongodb.net"
DATABASE_URI = f"mongodb+srv://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_URL}/"
BACKOFF_FACTOR = 0.1
NUMBER_OF_REDIRECTS = 5
NUMBER_OF_CONNECTION_RELATED_ERRORS = 5
NUMBER_OF_READ_RELATED_ERRORS = 2
CHUNK_SIZE = 8192
logging.basicConfig(
    format="%(asctime)s,%(msecs)d %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
    handlers=[RollingFileHandler(LOG_FILENAME, maxBytes=LOG_MAX_BYTES_TO_ROTATE, encoding='utf-8'),
              logging.StreamHandler()],
)

try:
    os.mkdir(INDEX_FOLDER)
except FileExistsError:
    logging.warning(f"INDEX_FOLDER={INDEX_FOLDER} already exists, continue?(y/n)")
    _input = input()
    if _input != "y":
        exit(1)

try:
    os.mkdir(OUTPUT_FOLDER)
except FileExistsError:
    logging.warning(f"OUTPUT_FOLDER={OUTPUT_FOLDER} already exists, continue?(y/n)")
    _input = input()
    if _input != "y":
        exit(1)

try:
    os.mkdir(WARC_OUTPUT_FOLDER)
except FileExistsError:
    logging.warning(f"WARC_OUTPUT_FOLDER={WARC_OUTPUT_FOLDER} already exists, continue?(y/n)")
    _input = input()
    if _input != "y":
        exit(1)

client = MongoClient(DATABASE_URI)
db = client[DATABASE_NAME]


def download_url(url, path, headers=None):
    session = requests.Session()
    retries = Retry(connect=NUMBER_OF_CONNECTION_RELATED_ERRORS,
                    read=NUMBER_OF_READ_RELATED_ERRORS,
                    redirect=NUMBER_OF_REDIRECTS,
                    backoff_factor=BACKOFF_FACTOR,
                    status_forcelist=[ 500, 502, 503, 504 ])

    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))

    with session.get(url, headers=headers, stream=True) as req:
        req.raise_for_status()
        with open(path, 'wb') as f:
            for chunk in req.iter_content(chunk_size=CHUNK_SIZE):
                f.write(chunk)


def get_cc_indices():
    index_compressed_name = "index.paths.gz"
    download_url(CC_INDEX_URL, "index.paths.gz")
    with gzip.open(index_compressed_name, "rb") as f:
        paths = list(
            filter(
                lambda link: "metadata.yaml" not in link
                and "cluster.idx" not in link
                and len(link) > 0,
                f.read().decode().split("\n"),
            )
        )
    return paths


def extract_entries(index_segment):
    entries = list()
    with gzip.open(index_segment, "r") as fp:
        while True:
            line = fp.readline()
            if not line:
                break
            json_part = line.split(maxsplit=2)[-1]
            data = json.loads(json_part)
            if "languages" in data and TARGET_LANGUAGE in data["languages"]:
                entries.append(data)
    return entries


def cc_entry_downloader(entry):
    offset, length = int(entry["offset"]), int(entry["length"])
    dirname = entry["filename"].split("/")[-1].split(".")[0]
    filename = entry["digest"] + ".warc.gz"
    os.makedirs(f"{WARC_OUTPUT_FOLDER}/{dirname}", exist_ok=True)
    path = f"{WARC_OUTPUT_FOLDER}/{dirname}/{filename}"
    download_url(
        f"{CC_BASE_URL}/{entry['filename']}",
        path=path,
        headers={"Range": f"bytes={offset}-{offset+length}"},
    )
    content = ""
    with open(path, "rb") as f:
        i = 0
        for record in ArchiveIterator(f):
            i += 1
            if i > 1:
                logging.warning(f"{entry['digest']} has more than one entry!")
            if record.rec_headers["WARC-Type"] != "response":
                logging.warning(f"{entry['digest']} is not of type response!")
            content = record.raw_stream.read().decode("utf-8", errors="ignore")
    os.remove(path)
    return content


def content_extractor(entry, html):
    article: Article = Article(entry["url"], fetch_images=False)
    article.download(input_html=html)
    article.parse()
    return article.text


def save_content(entry, content):
    entry["content"] = content
    filename = entry["filename"].split("/")[-1].split(".")[0] + ".jsonl"
    entry = {key: entry[key] for key in
             ['url', 'status', 'digest', 'length', 'offset', 'filename', 'languages', 'content']}
    with open(f"{OUTPUT_FOLDER}/{filename}", "a") as f:
        f.write(json.dumps(entry, ensure_ascii=False))
        f.write("\n")


def entry_exists_in_db(entry):
    return db.collection.count_documents({ 'digest': entry["digest"] }, limit = 1) != 0


def add_entry_to_db(entry):
    return db.collection.insert_one({"digest": entry["digest"]}).inserted_id


def worker(manager_id, worker_id, entry):
    try:
        logging.info(
            f"manager #{manager_id}, worker #{worker_id}: Processing {entry['digest']}"
        )
        logging.debug(
            f"manager #{manager_id}, worker #{worker_id}: Downloading {entry['digest']}"
        )
        html = cc_entry_downloader(entry)
        logging.debug(
            f"manager #{manager_id}, worker #{worker_id}: Extracting content for {entry['digest']}"
        )
        content = content_extractor(entry, html)
        logging.debug(
            f"manager #{manager_id}, worker #{worker_id}: Saving content for {entry['digest']}"
        )
        content_length = len(re.sub(r"\n+", "\n", content))
        if content_length > MIN_DOCUMENT_LENGTH:
            if not entry_exists_in_db(entry):
                save_content(entry, content)
                inserted_id = add_entry_to_db(entry)
                logging.info(f"manager #{manager_id}, worker #{worker_id}: Add entry with digest {entry['digest']} into database with id {inserted_id}.")
            else:
                logging.info(f"manager #{manager_id}, worker #{worker_id}: Entry with digest {entry['digest']} already exists.")
        else:
            logging.info(f"manager #{manager_id}, worker #{worker_id}: Content length is {content_length} which is less than {MIN_DOCUMENT_LENGTH}")
        logging.info(f"manager #{manager_id}, worker #{worker_id}: Done.")
    except Exception:
        logging.exception(
            f"manager #{manager_id}, worker #{worker_id}: Worker terminated with exception"
        )


def manager(manager_id, index_path):
    try:
        logging.info(f"manager #{manager_id}: Getting Started to work on {index_path}")
        index_filename = index_path.split("/")[-1]
        download_url(f"{CC_BASE_URL}/{index_path}", f"{INDEX_FOLDER}/{index_filename}")
        logging.info(f"manager #{manager_id}: Downloaded index file, extracting the entries.")
        entries = extract_entries(f"{INDEX_FOLDER}/{index_filename}")
        logging.info(f"manager #{manager_id}: Extracted entries, running workers.")
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=EACH_MANAGER_WORKERS
        ) as executor:
            future2path = {
                executor.submit(worker, manager_id, _id, entry): entry
                for _id, entry in enumerate(entries)
            }
            for future in concurrent.futures.as_completed(future2path):
                entry = future2path[future]
                future2path.pop(future)
                gc.collect()
                logging.info(f"The entry {entry['digest']} is processed.")
        logging.info(f"manager #{manager_id}: Done extracting entries removing index_path")
        os.remove(f"{INDEX_FOLDER}/{index_filename}")
        logging.info(f"manager #{manager_id}: Done.")
    except Exception:
        logging.exception(
            f"manager #{manager_id}: Manager terminated with exception"
        )


def main():
    cc_index_paths = get_cc_indices()

    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_MANAGER_NUMS) as executor:
        future2path = {
            executor.submit(manager, _id, file): file
            for _id, file in enumerate(cc_index_paths[START_PROCESSING_FROM_INDEX:])
        }
        for future in tqdm(
            concurrent.futures.as_completed(future2path), total=len(future2path)
        ):
            index = future2path[future]
            future2path.pop(future)
            gc.collect()
            logging.info(f"The index file {index} is processed.")


if __name__ == "__main__":
    main()