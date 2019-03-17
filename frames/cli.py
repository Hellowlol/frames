


import sys
import os
from multiprocessing.pool import ThreadPool

# Remove this shit later.
fp = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, fp)

import click
import uvicorn


from frames import LOG
from frames.db import init_db, Hashes
from frames.misc import find_all_movies_shows, choose, extract_id
from frames.app import app



DB = None


@click.group(help='frames []')
@click.option('-dburl')
@click.pass_context
def cli(ctx, dburl):
    ctx.obj = init_db(dburl)


@cli.command()
@click.option('--host', default='0.0.0.0')
@click.option('--port', default=8080)
@click.option('--debug', default=False)
@click.pass_context
def serve(ctx, host, port, debug):
    uvicorn.run(app, host=host, port=port)


@cli.command()
@click.option('--name', default=None)
@click.option('--dur', default=600, type=int)
@click.option('--sample', default=None, type=int)
@click.option('--step', default=None, type=int)
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
                        LOG.debug('Skipping %s season %s are there are not enough eps to get every n episode with sample %s' % 
                                 (show.title, season.index, sample))
                except: # pragma: no cover
                    pass


    def to_db(media):
        """ Just we can do the processing in a thread pool"""
        # extract the tvdb and the season


        @LOG.catch
        def zomg():
            imgz = []
            for imghash, frame, pos in hash_file(check_file_access(media),
                                                 frame_range=True,
                                                 end=dur,
                                                 step=step
                                                 ):
                img = Hashes(ratingKey=media.ratingKey,
                             hex=str(imghash),
                             hash=imghash.hash.tostring(),
                             
                             offset=pos,
                             tvdbid=None,
                             time=to_time(pos / 1000))
                imgz.append(img)

            with session_scope() as ssee:
                ssee.add_all(imgz)

        try:
            zomg():
        except:
            pass

    pool.map(to_db, all_items, 1)

    """
    Hashes = sa.Table(
    "hashes",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("tvdbid_season", sa.Text(length=100)),
    sa.Column("tvdbid", sa.Text(length=100)),
    sa.Column('hash', sa.Text(length=16))
)

    """



@cli.command()
def add_ref():
    pass




if __name__ == '__main__':
    cli()