import base64
import cv2

import sqlalchemy as sa
from sqlalchemy.sql import text

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.middleware.gzip import GZipMiddleware
from starlette.authentication import requires
from starlette.background import BackgroundTask

import numpy as np


from frames import DB, LOG
from frames.db import HASHES_T, showsql, RFT, IMAGES_T, USER_T
from frames.hashing import create_imghash, ImageHash
from frames.misc import from_dataurl_to_cvimage, resize, IMG_TYPES, decode_base64


app = Starlette(debug=True)
app.add_middleware(GZipMiddleware, minimum_size=500)
# TODO Add auth middlewhere.


def api_response(message='', status='success', data=None, task=None):

    if data is None:
        data = []

    output = {"status": status,
              "data": data,
              "message": message
              }

    return JSONResponse(output, background=task)


@app.on_event("startup")
async def startup():
    await DB.connect()


@app.on_event("shutdown")
async def shutdown():
    await DB.disconnect()


@app.route('/')
async def homepage(request):
    # Add some bootstrap page? See https://github.com/encode/starlette-example for ideas
    return JSONResponse({'hello': 'world'})


@app.route('/task/{name:str}')
async def test_task(request):
    name = request.path_params['name']

    def t(n):
        print('kek %s' % n)
        return '%s' % n

    tt = BackgroundTask(t, name)

    return api_response(message='working', task=tt)


@app.route('/api/dump/{name:str}')
async def dump_hashes_table(request):
    name = request.path_params['name']
    result = []
    if name == 'hashes':
        async for row in DB.iterate(HASHES_T.select()):
            result.append(dict(row))

    return api_response(data=result)


@app.route('/sql/{tvdbid:str}/{season:int}/{episode:int}')
async def sql(request):
    # http://localhost:8888/sql/248742/1/1

    tvdbid = request.path_params['tvdbid']
    season = request.path_params['season']
    episode = request.path_params['episode']

    conf = 0.7
    # this should probably be changed as we should just remove enogh episodes.
    episode_statement = text("SELECT COUNT(DISTINCT episode) FROM frames.hashes "
                             "WHERE tvdbid = :tvdbid and season = :season").bindparams(tvdbid=tvdbid,
                                                                                       season=season)
    # showsql(episode_statement)
    num_epiodes = await DB.fetch_one(episode_statement)
    num_epiodes = num_epiodes[0]
    req_eps = num_epiodes * conf
    LOG.debug('%s season %s got %s episodes' % (tvdbid, season, num_epiodes))

    if num_epiodes < 3:
        return api_response(message="To few episodes in the db")

    statement = text("SELECT hash FROM (SELECT * FROM frames.hashes "
                     "WHERE frames.hashes.tvdbid = :tvdbid "
                     "AND frames.hashes.season = :season "
                     "GROUP by hash, episode) as f "
                     "GROUP by hash "
                     "having count(hash) > :req_eps "
                     ).bindparams(tvdbid=tvdbid,
                                  season=season,
                                  req_eps=req_eps)

    # showsql(statement)

    result = []
    async for row in DB.iterate(statement):
        result.append(row.hash)

    LOG.debug('%s season %s got %s hashes' % (tvdbid, season, len(result)))

    return api_response(status='success', data=result)


@app.route('/api/upload', methods=['POST'])
async def upload(request):
    """ 
    the form should contain.
    id, show, episode, season, file and type

    """

    # Lets start with some housekeeping first.
    five_mb = 5242880
    if int(request.headers["content-length"]) > five_mb:
        LOG.debug('File size to big')
        return api_response(status='error', message='The request was to large, try reduzing the image size.')

    form = await request.form()

    if form['file'].content_type not in IMG_TYPES:
        message = 'The file type was %s expected %s' % (form['file'].content_type, IMG_TYPES)
        LOG.debug(message)
        return api_response(status='error', message=message)

    if not form.get('type') or form.get('type') in ('intro', 'outro'):
        message = 'Missing or invalid type, expected values was intro or outro'
        return api_response(status='error', message=message)

    file_ob = form.get('file')
    content = ''
    if file_ob is None:
        LOG.debug('The user forgot to add the damn file.')
        return api_response(status='error', message='Missing file')
    else:
        content = await file_ob.read()

    image = from_dataurl_to_cvimage(content)
    # This is blocking, should we do this in a bgtask?
    ih = ImageHash(cv2.img_hash.pHash(image))
    ih_str = str(ih)

    # Images are stored in binary form.
    query = USER_T.insert().values(hash=ih_str,
                                   tvdbid=form['id'],
                                   img=content,
                                   season=int(form['season']),
                                   episode=int(form['episode']),
                                   show=form['show'],
                                   type=form['type'])
    await DB.execute(query)

    LOG.debug('Added %s %s %s with hash %s to the db', form['show'],
              form['season'], form['episode'], ih_str)

    return api_response(status='success')


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
