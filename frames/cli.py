import click


import sys
import os

# Remove this shit later.
fp = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, fp)
print(fp)

from frames import LOG
from frames.db import init_db
from frames.misc import find_all_movies_shows, choose



DB = None


@click.group(help='frames []')
@click.option('-dburl')
@click.pass_context
def cli(ctx, dburl):
    ctx.DB = init_db(dburl)


@cli.command()
def serve():
    pass


@cli.command()
@click.option('--name', default=None)
@click.option('--dur', default=600, type=int)
@click.option('--sample', default=None, type=int)
@click.option('--step', default=None, type=int)
@click.pass_context
def add_hash(ctx, name, dur, sample, step):
    from plexapi.server import PlexServer
    # Add this to config.
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


    print(all_items)
    print(ctx.DB) 



@cli.command()
def add_ref():
    pass




if __name__ == '__main__':
    cli()