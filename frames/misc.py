
import click

from frames import LOG
from frame import CONFIG



def check_file_access(m, PMS):
    """Check if we can reach the file directly
       or if we have to download it via PMS.

       Args:
            m (plexapi.video.Episode)

       Return:
            filepath or http to the file.

    """
    LOG.debug('Checking if we can reach %s directly', m._prettyfilename())

    files = list(m.iterParts())
    # Now we could get the "wrong" file here.
    # If the user has duplications we might return the wrong file
    # CBA with fixing this as it requires to much work :P
    # And the use case is rather slim, you should never have dupes.
    # If the user has they can remove them using plex-cli.
    for file in files:
        if os.path.exists(file.file):
            LOG.debug('Found %s', file.file)
            return file.file
        elif CONFIG.get('remaps', []):
            for key, value in CONFIG.get('remaps').items():
                fp = file.file.replace(key, value)
                if os.path.exists(fp):
                    LOG.debug('Found %s', fp)
                    return fp
        else:
            LOG.warning('Downloading from pms..')
            try:
                # for plexapi 3.0.6 and above.
                return PMS.url('%s?download=1' % file.key, includeToken=True)
            except TypeError:
                return PMS.url('%s?download=1' % file.key)


def extract_id(s):
    d = {'type': '',
         'show': '',
         'season': '',
         'episode': ''}
    agent, ident = s.split('://')


    if agent.endswith('thetvdb'):
        show, season, ep = ident.split('?')[0].split('/')
        d['type'] = 'thetvdb'
        d['season'] = season
        d['show'] = show
        d['ep'] = ep
    
    return d



def find_all_movies_shows(pms):  # pragma: no cover
    """ Helper of get all the shows on a server.

        Args:
            func (callable): Run this function in a threadpool.

        Returns: List

    """
    all_shows = []

    for section in pms.library.sections():
        if section.TYPE in ('movie', 'show'):
            all_shows += section.all()

    return all_shows


def choose(msg, items, attr):
    result = []

    if not len(items):
        return result

    click.echo('')
    for i, item in reversed(list(enumerate(items))):
        name = attr(item) if callable(attr) else getattr(item, attr)
        click.echo('%s %s' % (i, name))

    click.echo('')

    while True:
        try:
            inp = click.prompt('%s' % msg)
            if any(s in inp for s in (':', '::')):
                idx = slice(*map(lambda x: int(x.strip()) if x.strip() else None, inp.split(':')))
                result = items[idx]
                break
            elif ',' in inp:
                ips = [int(i.strip()) for i in inp.split(',')]
                result = [items[z] for z in ips]
                break

            else:
                result = items[int(inp)]
                break

        except(ValueError, IndexError):
            pass

    if not isinstance(result, list):
        result = [result]

    return result