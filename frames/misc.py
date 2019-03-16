import click

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