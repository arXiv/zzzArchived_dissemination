"""Uplaods from CIT SFS to Google Storage based on a publish log.

ex.
```sh
python sync_to_arxiv_produciton.py /data/new/logs/publish_221101.log
```

The PUBLISHLOG fiels can be found on the legacy FS at
/data/new/logs/publish_YYMMDD.log

This works by parsing the PUBLISHLOG file for new and rep entries,
those are put in the `todo_q` queue.

Then for each of these `arxiv_id`s it will check that the PDF file for
the `arxiv_id` exists in the `/data/ps_cache`. If it does not it will
request the `arxiv_id` via HTTP from the arxiv.org site and wait until
the `/data/ps_cache` file exists.

Once that returns the PDF will be uploaded to the GS bucket.

# Alternative

This uses the SFS but there is a technique to get the files in a
manner similar to the mirrors. If we did this we could copy the
publish file to GS and then kick off a CloudRun job to do the sync.
"""

# pylint: disable=locally-disabled, line-too-long, logging-fstring-interpolation, global-statement

import sys
import argparse
import re
import threading
from threading import Thread
from queue import Queue, Empty
import requests
from time import sleep, perf_counter
from datetime import datetime
import signal
import json
from typing import List, Tuple

from pathlib import Path

from tenacity.wait import wait_exponential_jitter

from identifier import Identifier
from make_todos import make_todos

overall_start = perf_counter()

from google.cloud import storage

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_random_exponential

import logging
logging.basicConfig(level=logging.WARNING, format='%(message)s (%(threadName)s)')
logger = logging.getLogger(__file__)
logger.setLevel(logging.WARN)

GS_BUCKET= 'arxiv-production-data'
GS_KEY_PREFIX = '/ps_cache'

PS_CACHE_PREFIX= '/cache/ps_cache/'

ENSURE_UA = 'periodic-rebuild'

ENSURE_HOSTS = [
    #('web2.arxiv.org', 40),
    #('web3.arxiv.org', 40),
    #('web4.arxiv.org', 40),
    ('web5.arxiv.org', 3),
    ('web6.arxiv.org', 3),
    ('web7.arxiv.org', 3),
    ('web8.arxiv.org', 3),
    ('web9.arxiv.org', 3),
    ('web10.arxiv.org', 3),
]
"""Tuples of form HOST, THREADS_FOR_HOST"""

ENSURE_CERT_VERIFY=False

PDF_WAIT_SEC = 60 * 10
"""Maximum sec to wait for a PDF to be created"""

UPLOAD_WAIT = 0.1
UPLOAD_THREADS = 4
UPLOAD_RETRYS=4

# Thread safe queues
build_pdf_q: Queue = Queue() # queue for building pdfs
upload_q: Queue = Queue() # queue of files to upload
summary_q: Queue = Queue() # queue for reporting completed jobs

def enqueue_todos(todos:List[Tuple])->None:
    """Puts the todo work items in the correct queues"""
    for item in todos:
        action = item[3]
        if action == 'upload':
            upload_q.put(item)
        elif action == 'build+upload':
            build_pdf_q.put(item)
        else:
            logger.error(f"Skipping todo item with invalid action {item}")

RUN = True
DONE = False

def handler_stop_signals(signum, frame):
    """Stop threads on ctrl-c, mostly useful during testing"""
    global RUN
    RUN = False

signal.signal(signal.SIGINT, handler_stop_signals)
signal.signal(signal.SIGTERM, handler_stop_signals)

class EnsurePdfException(Exception):
    """Indicate a problem during ensuring the PDF file exists"""
    pass

def ms_since(start:float) -> int:
    return int((perf_counter() - start) * 1000)


def path_to_bucket_key(pdf) -> str:
    """Handels both source and cache files. Should handle pdfs, abs, txt
    and other types of files under these directories. Bucket key should
    not start with a /"""
    if str(pdf).startswith('/cache/'):
        return str(pdf).replace('/cache/','')
    elif str(pdf).startswith('/data/'):
        return str(pdf).replace('/data/','')
    else:
        raise ValueError(f"Cannot convert PDF path {pdf} to a GS key")

def ensure_pdf(session, host, arxiv_id):
    """Ensures PDF exits for arxiv_id.

    Check on the ps_cache.  If it does not exist, request it and wait
    for the PDF to be built.

    TODO Not sure if it is possible to have a paper that was a TeX
    source on version N but then is PDF Source on version N+1.

    Returns tuple with pdf_file, url, msec

    arxiv_id must have a version.

    This does not check if the arxiv_id is PDF source.
    """
    def pdf_cache_path(arxiv_id) -> Path:
        """Gets the PDF file in the ps_cache. Returns Path object."""
        archive = ('arxiv' if not arxiv_id.is_old_id else arxiv_id.archive)
        return Path(f"{PS_CACHE_PREFIX}/{archive}/pdf/{arxiv_id.yymm}/{arxiv_id.filename}v{arxiv_id.version}.pdf")

    def arxiv_pdf_url(host, arxiv_id) -> str:
        """Gets the URL that would be used to request the pdf for the arxiv_id"""
        return f"https://{host}/pdf/{arxiv_id.filename}v{arxiv_id.version}.pdf?nocdn=1"

    pdf_file, url = pdf_cache_path(arxiv_id), arxiv_pdf_url(host, arxiv_id)

    start = perf_counter()

    if not pdf_file.exists():
        headers = { 'User-Agent': ENSURE_UA }
        logger.debug("Getting %s", url)
        resp = session.get(url, headers=headers, stream=True, verify=ENSURE_CERT_VERIFY)
        [line for line in resp.iter_lines()]  # Consume resp in hopes of keeping alive session
        if resp.status_code != 200:
            raise(EnsurePdfException(f"ensure_pdf: GET status {resp.status_code} {url}"))

        start_wait = perf_counter()
        while not pdf_file.exists():
            if perf_counter() - start_wait > PDF_WAIT_SEC:
                raise(EnsurePdfException(f"No PDF, waited longer than {PDF_WAIT_SEC} sec {url}"))
            else:
                sleep(0.2)
        if pdf_file.exists():
            return (pdf_file, url, None, ms_since(start))
        else:
            raise(EnsurePdfException(f"ensure_pdf: Could not create {pdf_file}. {url} waited {ms_since(start)} ms"))
    else:
        return (pdf_file, url, "already exists", ms_since(start))


def ensure_pdf_build_thread_target(build_q, host):
    """Target for theads that gets jobs off of the queue and ensures the PDF is built."""
    tl_data=threading.local()
    tl_data.session = requests.Session()

    @retry(stop=stop_after_attempt(3),
           wait=wait_exponential_jitter(),
           retry=(retry_if_exception_type(EnsurePdfException)|retry_if_exception_type(requests.RequestException)),
           reraise=True)
    def tl_ensure_pdf(session, host, aid):
        return ensure_pdf(session, host, aid)

    while RUN:
        start = perf_counter()
        try:
            job = build_q.get(block=False)
            if not job or not type(job) == tuple or not len(job)==5:
                logger.error("build_q.get() returned invalid {job}")
                continue
            subid, subtype, idv, action, item = job
            if action != 'build+upload':
                logger.warning("unexpected {action} job for ensure_pdf_build_thread_target")
        except Empty:  # queue is empty and thread is done
            break

        try:
            #logger.debug("doing pdf build for %s", idv)
            pdf_file, url, status, duration = tl_ensure_pdf(tl_data.session, host, Identifier(item))
            summary_q.put((idv, 'ensure_pdf',status, ms_since(start), url))
            #logger.debug("pdf built for %s", idv)
            upload_q.put( (subid, subtype, idv, 'upload', pdf_file) )
            #logger.debug("enqueued for upload %s", idv)
        except Exception as ex:
            logger.exception(f"Problem during {idv} {action} {item}")
            summary_q.put((idv, 'ensure_pdf', 'failed',  ms_since(start), str(ex)))
        build_q.task_done()



def upload_thread_target(upload_q):
    tl_data=threading.local()
    tl_data.gs_client = storage.Client()

    def mime_from_fname(filepath):
        if filepath.suffix == '.pdf':
            return 'application/pdf'
        if filepath.suffix == '.gz':
            return 'application/gzip'
        if filepath.suffix == '.abs':
            return 'text/plain'
        else:
            return ''

    def upload_pdf(gs_client, ensure_tuple):
        """Uploads a PDF from ps_cache to GS_BUCKET"""
        return upload(gs_client, ensure_tuple[0], path_to_bucket_key(ensure_tuple[0])) + ensure_tuple

    @retry(stop=stop_after_attempt(UPLOAD_RETRYS),
           wait=wait_exponential_jitter(),
           reraise=True)
    def upload(gs_client, localpath, key):
        """Upload a file to GS_BUCKET."""

        bucket = gs_client.bucket(GS_BUCKET)
        blob = bucket.get_blob(key)
        if blob is None or blob.size != localpath.stat().st_size:
            with open(localpath, 'rb') as fh:
                bucket.blob(key).upload_from_file(fh, content_type=mime_from_fname(localpath))
                logger.debug(f"upload: completed upload of {localpath} to gs://{GS_BUCKET}/{key} of size {localpath.stat().st_size}")
            return ("uploaded", localpath.stat().st_size)
        else:
            logger.debug(f"upload: Not uploading {localpath}, gs://{GS_BUCKET}/{key} already on gs")
            return ("already_on_gs", 0)

    while RUN:
        start = perf_counter()
        try:
            job = upload_q.get(block=False)
            if not job or not type(job) == tuple or not len(job)==5:
                logger.error("upload_q.get() returned invalid {job}")
                continue
            subid, subtype, idv, action, localpath = job
            if action != 'upload':
                logger.warning("unexpected {action} job for upload_thread_target")
        except Empty:  # queue is empty and thread is done
            break

        try:
            logger.debug("uploading for %s", idv)
            gsurl = path_to_bucket_key(localpath)
            status, size = upload(tl_data.gs_client, Path(localpath), gsurl)
            summary_q.put((idv, 'upload', status, ms_since(start), f"gs://{GS_BUCKET}/{gsurl}", size))
        except Exception as ex:
            logger.exception(f"Problem during {idv} {action} {localpath}")
            summary_q.put((idv, 'upload', 'failed', ms_since(start), str(ex)))
        upload_q.task_done()


# #################### MAIN #################### #
if __name__ == "__main__":
    ad = argparse.ArgumentParser(epilog=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ad.add_argument('-v', help='verbse', action='store_true')
    ad.add_argument('-d', help="Dry run no action", action='store_true')
    ad.add_argument('filename')
    args = ad.parse_args()

    if not args.d:
        storage.Client() # will fail if no auth setup
    if args.v:
        logger.setLevel(logging.INFO)

    print(f"Starting at {datetime.now().isoformat()}")
    todo = make_todos(args.filename)

    sub_count = len(set([item[0] for item in todo]))
    if args.d:
        print(json.dumps(todo, indent=2))
        print(f"{sub_count} submissions (some may be test submissions)")
        logger.info("Dry run no changes made")
        sys.exit(1)

    enqueue_todos(todo)
    pdf_threads = []
    for host, n_th in ENSURE_HOSTS:
        ths = [Thread(target=ensure_pdf_build_thread_target, args=(build_pdf_q, host)) for _ in range(0, n_th)]
        pdf_threads.extend(ths)
        [t.start() for t in ths]

    logger.debug("started %d pdf threads", len(pdf_threads))

    upload_threads = []
    for i in range(0,UPLOAD_THREADS):
        th = Thread(target=upload_thread_target, args=(upload_q,))
        th.start()
        upload_threads.append(th)

    logger.debug("started %d upload threads", len(pdf_threads))

    while RUN and not build_pdf_q.empty():
        sleep(0.2)

    logger.debug("build_pdf_q is now empty")

    while RUN and not upload_q.empty():
        sleep(0.2)

    logger.debug("uplaod_q is now empty")

    DONE=True
    RUN=False
    logger.debug("wating to join threads")
    [th.join() for th in pdf_threads + upload_threads]
    logger.debug("Threads done joining")

    for row in sorted(list(summary_q.queue), key=lambda tup: tup[0]):
         print(','.join(map(str, row)))

    print(f"Done at {datetime.now().isoformat()}")
    print(f"Overall time: {(perf_counter()-overall_start):.2f} sec for {sub_count} submissions")
