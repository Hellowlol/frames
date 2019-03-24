


import sys
import os
from multiprocessing.pool import ThreadPool



import click
import uvicorn


from frames import LOG, init_frames
from frames.db import init_db, Hashes, session_scope
from frames.hashing import hash_file
from frames.misc import find_all_movies_shows, choose, extract_id, check_file_access
from frames.app import app


@click.group(help='frames []')
@click.option('-du', '--db_url', default=None)
@click.option('-df', '--default_folder', default=None)
@click.pass_context
def cli(ctx, db_url, default_folder):
    pass
    #init_frames(default_folder, db_url)
    #ctx.obj = init_db(dburl)


@cli.command()
@click.option('--host', default='0.0.0.0')
@click.option('--port', default=8888)
@click.option('--debug', default=False)
@click.pass_context
def serve(ctx, host, port, debug):
    uvicorn.run(app, host=host, port=port)


@cli.command()
@click.option('--name', default=None)
@click.option('--dur', default=600, type=int)
@click.option('--sample', default=None, type=int)
@click.option('--step', default=1, type=int)
@click.option('--threads', default=4, type=int)
@click.pass_context
def add_hash(ctx, name, dur, sample, step, threads):
    from plexapi.server import PlexServer
    # Add this to config.
    pool = ThreadPool(threads)
    PMS = PlexServer()
    all_items = []
    medias = find_all_movies_shows(PMS)
    if sample is None:
        if name:
            medias = [s for s in medias if s.title.lower().startswith(name.lower())]
        else:
            medias = [s for s in medias if s.TYPE == 'show']

        medias = choose('Select what item to process', medias, 'title')

        for media in medias:
            if media.TYPE == 'show':
                eps = media.episodes()
                eps = choose('Select episodes', eps, lambda x: '%s %s' % (x._prettyfilename(), x.title))
                all_items += eps
    else:
        for show in [s for s in medias if s.TYPE == 'show']:
            for season in show.seasons():
                try:
                    eps = season.episodes()
                    eps = eps[::2]
                    if len(eps) >= sample:
                        all_items.extend(eps)
                    else:
                        LOG.debug('Skipping %s season %s are there are not enough eps to get '
                                  'every n episode with sample %s' %
                                  (show.title, season.index, sample))
                except: # pragma: no cover
                    pass

    def nothing(media):
        LOG.debug(media)


    def to_db(media):
        """ Just we can do the processing in a thread pool"""
        # extract the tvdb and the season
        media_key = extract_id(media.guid)

        # TODO remove with job from task.
        @LOG.catch
        def zomg():
            hashed_file = []
            for imghash, frame, pos in hash_file(check_file_access(media, PMS),
                                                 frame_range=False,
                                                 end=dur,
                                                 step=step
                                                 ):
                img = Hashes(hash=str(imghash),
                             season=media_key['season'],
                             episode=media_key['episode'], 
                             offset=pos,
                             tvdbid=media_key['show'],
                             )
                hashed_file.append(img)

            with session_scope() as ssee:
                ssee.add_all(hashed_file)

            LOG.debug('Added %s %s to db' % (media.grandparentTitle, media.seasonEpisode))

        try:
            zomg()
        except KeyboardInterrupt:
            raise

    try:
        pool.map(to_db, all_items, 1)
    except KeyboardInterrupt:
        raise


@cli.command()
def add_ref():
    pass


@cli.command()
@click.argument('tvdbid')
@click.argument('season')
@click.argument('episode')
def check_db(tvdbid, season, episode):
    # Remove later
    r = []
    P = 0.7
    conf = 7

    full_c = P * conf

    from collections import defaultdict

    d = defaultdict(set)
    with session_scope() as se:
        r = se.query(Hashes).filter(Hashes.tvdbid==tvdbid, Hashes.season==season)
        for res in r:
            d[res.hash].add(res.episode)

    hh = []
    for k, v in d.items():
        if len(v) > full_c:
            print(k, v)
            hh.append(k)

    print('hh', len(hh))
    print(full_c)
    # 1238





if __name__ == '__main__':
    print('use fake main')