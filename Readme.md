# CommonCrawl Downloader

This script downloads *only* the segments from the giant dataset CommonCrawl for the given specific language. CommonCrawl dataset archives and publishes internet content every few months, making it a valuable resource for extracting content to train a language model (or Large language model these days).

**NOTE:** Here is a sample output of this project in Huggingface ([link](https://huggingface.co/datasets/mrghofrani/cc-2023-23-fa)). I hope in the future I can release a whole dataset via completing running this code on a CommonCrawl index.

## Features

✅ Download *only* the target language segments

✅ Parallel CC index processing

✅ Parallel CC segment downloading

✅ Support download continuing

## Architecture

The script begins by downloading the index file of the CommonCrawl's crawl. This file contains the relative path of each CommonCrawl's index file. These paths should be concated to `https://data.commoncrawl.org/` or their s3 alternative `s3://commoncrawl/`. Due to limitations, this script uses the HTTP download method to download and not the s3 alternative. Then, based on `MAX_MANAGER_NUMS` configs, creates processes (which are known as `manager` in the script) and assigns the processing job of each index file. 

Below steps are followed by each manager:

1. Download the given index file
2. Identify entries that are labeled as containing target language content
3. Create threads (or `worker` in the script literature) and assign the processing work of each entry

Each worker is responsible for completing the following tasks in advance:

1. Download the WARC file associated with the entry based on the byte range
2. Extract text out of the WARC file using the newspaper3k library
3. Check whether the text file has been saved before, if not save the text.

## Some Tips

I have done this project to fill out my leisure time. Here I wanted to share some insights that may help other people who might be interested.

- Every index of CommonCrawl has a bit of content in every language.

## Contributions

The following components could be improved:

- [ ] Better parsing the CommonCrawl index files. There exist some tools such as [pywb](https://github.com/webrecorder/pywb) that seem to fit CommonCrawl parsing, however, these tools are cumbersome and lack good documentation. Therefore, I decided to manually parse CC's index files.

- [ ] Adding the option to download files via s3. Because of my country's limitations, I don't have access to s3 so sadly I can't implement this feature.
