import sqlalchemy as sa
from sqlalchemy.sql import text

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.middleware.gzip import GZipMiddleware
from starlette.authentication import requires
from starlette.background import BackgroundTask


from frames import DB, LOG
from frames.db import HASHES_T, showsql


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


@app.route('/api/add')
async def add(request):
    """ Add a video path, tvdbid, season and episode and frames will handle the rest

        never intended for public use.

    """
    # untested.
    def nothing(zomg):  # Replace with the one from tasks.py
        return 'working'

    bg = BackgroundTask(nothing)

    return api_response(message='working on it', task=bg)


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


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
