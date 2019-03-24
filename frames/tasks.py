# tasks.py
from fames import LOG
from frames.db import Hashes, session_scope
from frames.hashing import hash_file


@LOG.catch
def add_to_db(path, tvdbid, season, episode,
              addimg=False, dur=600, step=1, end=None):
    """Sync method to add to a file to the db"""
    hashes = []

    for imghash, frame, pos in hash_file(path, frame_range=False,
                                         dur=dur, step=step):
        row = Hashes(hash=str(imghash),
                     season=season,
                     episode=episode,
                     tvdbid=tvdbid)
        hashes.append(row)
        if addimg:
            pass

    with session_scope() as se:
        se.add_all(hashes)
