import base64
import cv2

import sqlalchemy as sa
from sqlalchemy.sql import text

from starlette.applications import Starlette
from starlette.responses import JSONResponse, Response
from starlette.middleware.gzip import GZipMiddleware
from starlette.concurrency import run_in_threadpool

from frames import DB, LOG
from frames.hashing import ImageHash
from frames.misc import from_dataurl_to_cvimage, IMG_TYPES

from frames.db import (HASHES_T, showsql, RFT,
                       IMAGES_T, USER_T, UserImage,
                       Reference_Frame, Hashes, Images)


app = Starlette(debug=True)
app.add_middleware(GZipMiddleware, minimum_size=500)
# TODO Add auth middlewhere.


def api_response(message='', status='success', data=None, task=None):
    """Skeleton for the api response"""

    if data is None:
        data = []

    output = {"status": status,
              "data": data,
              "message": message
              }

    if status == 'success':
        status_code = 200
    else:
        status_code = 500

    return JSONResponse(output, status_code=status_code, background=task)


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


@app.route('/api/sql/dump/{name:str}')
async def dump_hashes_table(request):
    """Simple way to dump the tables to json."""
    name = request.path_params['name']
    result = []

    if name == 'hashes':
        table = HASHES_T.select()

    elif name == 'userimage':
        # We skip the img key as this is the binary data.
        # We dont want to dump this info as they are huge.
        table = sa.select([col for col in USER_T.columns if col.key != 'img'])
    elif name == 'reference_frame':
        table = RFT.select()

    else:
        return api_response(status='error', message='Invalid table')

    async for row in DB.iterate(table):
        result.append(dict(row))

    return api_response(data=result)


@app.route('/api/sql/{tvdbid:str}/{season:int}/{episode:int}')
async def sql(request):
    # http://localhost:8888/sql/248742/1/1

    tvdbid = request.path_params['tvdbid']
    season = request.path_params['season']
    # Do we really need this?
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


@app.route('/api/image/{hash:str}')
async def show_image(request):
    """Show a image to a user using the api."""
    img_hash = request.path_params['hash']
    q = USER_T.select().where(UserImage.hash == img_hash)
    print(showsql(q))
    result = await DB.execute(q)

    if result is None:
        return api_response(status='error', message='No images match %s' % img_hash)

    if len(result) > 1:
        LOG.debug('We have more then one one image with this hash')

    if request.query_params.get('base64', '') in ('true', True):
        # This should probably be a better method
        # to do this but it should be enough for now.
        bs = base64.b64encode(result[0].img)
        # All the images in the db should be stored as
        # png anyway, at least if they are upload using
        # the browser extension.
        img = 'data:image/png;base64,%s' % bs
        return Response(img)

    return Response(result[0].img, media_type="image/png")


@app.route('/api/upload', methods=['POST'])
async def upload(request):
    """
    Upload command for the api.

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

    if form.get('type', '') not in ('intro', 'outro'):
        message = 'Missing or invalid type, expected values was intro or outro'
        return api_response(status='error', message=message)

    file_ob = form.get('file')
    content = ''
    if file_ob is None:
        LOG.debug('The user forgot to add the damn file.')
        return api_response(status='error', message='Missing file')
    else:
        content = await file_ob.read()

    # Need to run this in the threadpool
    def tb():
        image = from_dataurl_to_cvimage(content)
        ih = ImageHash(cv2.img_hash.pHash(image))
        return ih

    ih = await run_in_threadpool(tb)
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
