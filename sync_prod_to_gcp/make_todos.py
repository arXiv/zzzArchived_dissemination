"""Make the todo list for sync_published_to_gcp.py"""
from typing import List, Tuple

import re
from identifier import Identifier

import logging
logging.basicConfig(level=logging.WARNING, format='%(message)s (%(threadName)s)')
logger = logging.getLogger(__file__)
logger.setLevel(logging.WARNING)

FTP_PREFIX = '/data/ftp/'


def make_todos(filename) -> List[Tuple]:
    """Reades `filename` and figures out what work needs to be done for the sync.
    This only uses data from the publish file.

    It returns a list of work to do as tuples like:
    [ [47883, 'new', 2202.00235, 'upload', '/some/dir/2202.00234.abs']
    ...]
    """

    # These regexs should do both legacy ids and modern ids
    new_r = re.compile(r"^.* new submission\n.* paper_id: (.*)$", re.MULTILINE)
    abs_r = re.compile(r"^.* absfile: (.*)$", re.MULTILINE)
    src_pdf_r = re.compile(r"^.* Document source: (.*.pdf)$", re.MULTILINE)
    src_html_r = re.compile(r"^.* Document source: (.*.html.gz)$", re.MULTILINE)
    src_tex_r = re.compile(r"^.* Document source: (.*.gz)$", re.MULTILINE)

    rep_r = re.compile(r"^.* replacement for (.*)\n.*\n.* old version: (\d*)\n.* new version: (\d*)", re.MULTILINE)
    wdr_r = re.compile(r"^.* withdrawal of (.*)\n.*\n.* old version: (\d*)\n.* new version: (\d*)", re.MULTILINE)
    cross_r = re.compile(r"^.* cross for (.*)$")
    cross_r = re.compile(r" cross for (.*)")
    jref_r = re.compile(r" journal ref for (.*)")
    test_r = re.compile(r" Test Submission\. Skipping\.")

    todo = []

    def upload_abs_acts(subid:int, arxiv_id: Identifier, subtype:str):
        """Makes upload actions for abs when only an id is available, ex cross or jref"""
        archive = ('arxiv' if not arxiv_id.is_old_id else arxiv_id.archive)
        return [(subid, subtype, arxiv_id.idv, 'upload', f"{FTP_PREFIX}/{archive}/papers/{arxiv_id.yymm}/{arxiv_id.filename}.abs")]

    def upload_abs_src_acts(subid:int, arxiv_id, subtype:str, txt):
        """Makes upload actions for abs and source"""
        absm = abs_r.search(txt)
        pdfm = src_pdf_r.search(txt)
        texm = src_tex_r.search(txt)
        htmlm = src_html_r.search(txt)

        actions:List[Tuple[str,str]] = []
        if absm:
             actions = [(subid, subtype, arxiv_id.idv, 'upload', absm.group(1))]

        if pdfm:
            actions.append((subid, subtype, arxiv_id.idv, 'upload', pdfm.group(1)))
        elif htmlm: #must be before tex due to pattern overlap
            actions.append((subid, subtype, arxiv_id.idv, 'upload', htmlm.group(1)))
        elif texm:
            actions.append((subid, subtype, arxiv_id.idv, 'upload', texm.group(1)))
            actions.append((subid, subtype, arxiv_id.idv, 'build+upload', f"{arxiv_id.id}v{arxiv_id.version}"))
        else:
            logger.error(f"Could not determin source for submission {arxiv_id.idv}")

        return actions

    def rep_version_acts(subid: int, arxiv_id: Identifier, subtype: str, txt):
        """Makes actions for replacement.

        Don't try to move on the GCP, just sync to GCP so it is idempotent."""
        move_r = re.compile(r"^.* Moved (.*) => (.*)$", re.MULTILINE)
        return [(subid, subtype, arxiv_id.idv, 'upload', m.group(2)) for m in move_r.finditer(txt)]


    sub_start_r = re.compile(r".* submission (\d*)$")
    sub_end_r = re.compile(r".*Finished processing submission ")
    subs, in_sub, txt, sm =[], False, '', None
    with open(filename) as fh:
        for line in fh.readlines():
            if in_sub:
                if sm is not None and sub_end_r.match(line):
                    subs.append((sm.group(1), txt + line))
                    txt, sm, in_sub = '', None, False
                else:
                    txt = txt + line
            else:
                sm = sub_start_r.match(line)
                if sm:
                    in_sub=True

    for subid, txt in subs:
        m = test_r.search(txt)
        if m:
             continue
        m = new_r.search(txt)
        if m:
            arxiv_id = Identifier(f"{m.group(1)}v1")
            todo.extend(upload_abs_src_acts(subid, arxiv_id, 'new', txt))
            continue
        m = rep_r.search(txt)
        if m:
            arxiv_id = Identifier(f"{m.group(1)}v{m.group(3)}")
            todo.extend(rep_version_acts(subid, arxiv_id, 'rep', txt))
            todo.extend(upload_abs_src_acts(subid, arxiv_id, 'rep', txt))
            continue
        m = wdr_r.search(txt)
        if m:
            arxiv_id = Identifier(f"{m.group(1)}v{m.group(3)}")
            # withdrawls don't need the pdf synced since they should lack source
            actions = list(filter(lambda tt: tt[3] != 'build+upload', rep_version_acts(subid, arxiv_id, 'wdr', txt) +
                                  upload_abs_src_acts(subid, arxiv_id, 'wdr', txt)))
            todo.extend(actions)
            continue
        m = cross_r.search(txt)
        if m:
            arxiv_id = Identifier(m.group(1))
            todo.extend(upload_abs_acts(subid, arxiv_id, 'cross'))
            continue
        m = jref_r.search(txt)
        if m:
            arxiv_id = Identifier(m.group(1))
            todo.extend(upload_abs_acts(subid, arxiv_id, 'jref'))
            continue

    return todo
