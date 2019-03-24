import os
import configobj
from validate import Validator

vtor = Validator()

spec = """
[general]
debug = boolean(default=False)
db = string(default=media.db)
[remaps]
[hashing]
season_treshold = float(default=0.8)
""".splitlines()


def migrate(default):
    # Add migrates here.
    return default


def read_or_make(fp):
    default = configobj.ConfigObj(None, configspec=spec,
                                  write_empty_values=True,
                                  create_empty=True,
                                  list_values=True)

    # Overwrite defaults options with what the user has given.
    if os.path.isfile(fp):
        config = configobj.ConfigObj(fp,
                                     write_empty_values=True,
                                     create_empty=True,
                                     list_values=True)
        default.merge(config)

    default.validate(vtor, copy=True)

    default = migrate(default)

    default.filename = fp
    default.write()
    return default
