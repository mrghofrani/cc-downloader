import os
import re
import gzip
import json
import logging
import urllib3
from tqdm import tqdm
import concurrent.futures

from newspaper import Article
from warcio.archiveiterator import ArchiveIterator


MAX_MANAGER_NUMS = 4
EACH_MANAGER_WORKERS = 3
MIN_DOCUMENT_LENGTH = 500
LOG_FILENAME = "file.log"
CC_VERSION = "2023-23"
TARGET_LANGUAGE = "fas"
CC_BASE_URL = "https://data.commoncrawl.org"
CC_INDEX_URL = f"{CC_BASE_URL}/crawl-data/CC-MAIN-{CC_VERSION}/cc-index.paths.gz"
OUTPUT_FOLDER = f"OUTPUT-CC-{CC_VERSION}"
WARC_OUTPUT_FOLDER = f"WARC-CC-{CC_VERSION}"
INDEX_FOLDER = f"INDEX-CC-{CC_VERSION}"
BACKOFF_FACTOR = 0.1
NUMBER_OF_REDIRECTS = 5
NUMBER_OF_CONNECTION_RELATED_ERRORS = 5
NUMBER_OF_READ_RELATED_ERRORS = 2
CHUNK_SIZE = 10_000
os.mkdir(INDEX_FOLDER)
os.mkdir(OUTPUT_FOLDER)
os.mkdir(WARC_OUTPUT_FOLDER)
logging.basicConfig(
    format="%(asctime)s,%(msecs)d %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.DEBUG,
    handlers=[logging.FileHandler(LOG_FILENAME), logging.StreamHandler()],
)


def download_url(url, path, headers=None):
    retries = urllib3.Retry(
        connect=NUMBER_OF_CONNECTION_RELATED_ERRORS,
        read=NUMBER_OF_READ_RELATED_ERRORS,
        redirect=NUMBER_OF_REDIRECTS,
        backoff_factor=BACKOFF_FACTOR,
    )
    http = urllib3.PoolManager(retries=retries)
    request = http.request("GET", url, headers=headers, preload_content=False)

    with open(path, "wb") as out:
        while True:
            data = request.read(CHUNK_SIZE)
            if not data:
                break
            out.write(data)
    request.release_conn()


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
    with open(path, "rb") as f:
        i = 0
        for record in ArchiveIterator(f):
            i += 1
            if i > 1:
                logging.warning(f"{entry['digest']} has more than one entry!")
            if record.rec_headers["WARC-Type"] != "response":
                logging.warning(f"{entry['digest']} is not of type response!")
            return record.raw_stream.read().decode("utf-8", errors="ignore")


def content_extractor(entry, html):
    article: Article = Article(entry["url"], fetch_images=False)
    article.download(input_html=html)
    article.parse()
    return article.text


def save_content(entry, content):
    entry["content"] = content
    filename = entry["filename"].split("/")[-1].split(".")[0] + ".jsonl"
    with open(f"{OUTPUT_FOLDER}/{filename}", "a") as f:
        f.write(json.dumps(entry, ensure_ascii=False))
        f.write("\n")


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
        if len(re.sub(r"\n+", "\n", content)) > MIN_DOCUMENT_LENGTH:
            save_content(entry, content)
        logging.info(f"manager #{manager_id}, worker #{worker_id}: Done.")
    except Exception:
        logging.exception(
            f"manager #{manager_id}, worker #{worker_id}: Terminated with exception"
        )


def manager(manager_id, index_path):
    logging.info(f"worker #{manager_id}: Getting Started.")
    index_filename = index_path.split("/")[-1]
    download_url(f"{CC_BASE_URL}/{index_path}", f"{INDEX_FOLDER}/{index_filename}")
    entries = extract_entries(f"{INDEX_FOLDER}/{index_filename}")
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=EACH_MANAGER_WORKERS
    ) as executor:
        future2path = {
            executor.submit(worker, manager_id, _id, entry): entry
            for _id, entry in enumerate(entries)
        }
        for future in concurrent.futures.as_completed(future2path):
            entry = future2path[future]
            logging.info(f"The entry {entry['digest']} is processed.")


def main():
    cc_index_paths = get_cc_indices()

    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_MANAGER_NUMS) as executor:
        future2path = {
            executor.submit(manager, _id, file): file
            for _id, file in enumerate(cc_index_paths)
        }
        for future in tqdm(
            concurrent.futures.as_completed(future2path), total=len(future2path)
        ):
            index = future2path[future]
            logging.info(f"The index file {index} is processed.")


if __name__ == "__main__":
    main()