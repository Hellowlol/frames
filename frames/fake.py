import os
import sys


# Remove this shit later.
fp = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, fp)


def fake_main():
    default_folder = None
    db_url = None
    args = sys.argv[:]
    for i, arg in enumerate(args):
        if arg == ('-df', '--default_folder'):
            default_folder = args[i + 1]
        if arg in ('-du', '--db_url'):
            db_url = args[i + 1]

    import frames

    # Bootstrap the shit.
    frames.init_frames(default_folder, db_url)

    # Needs import later so we set the correct paths etc.
    from frames.cli import cli

    cli()



if __name__ == '__main__':
    fake_main()