from frames import LOG
from frames.db import Hashes, Images, session_scope
from frames.hashing import hash_file


@LOG.catch
def add_to_db(path, tvdbid, season, episode,
              addimg=False, step=1, end=600):
    """Sync method to add to a file to the db"""
    hashes = []

    for imghash, frame, pos in hash_file(path, frame_range=False,
                                         end=end, step=step):
        row = Hashes(hash=str(imghash),
                     season=season,
                     episode=episode,
                     tvdbid=tvdbid)
        hashes.append(row)

        if addimg:
            pass
            i = Images(hash=str(imghash),
                       tvdbid=tvdbid,
                       imb=frame)  # Frame should be resized?
            # We want to add this on every image as keeping ~ 15k images in memory is bad.
            # assuming 24 fps * dur
            with session_scope() as zomg:
                zomg.add(i)

    with session_scope() as se:
        se.add_all(hashes)
