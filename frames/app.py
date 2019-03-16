from starlette.applications import Starlette
from starlette.responses import JSONResponse
import uvicorn




app = Starlette(debug=True)



@app.on_event("startup")
async def startup():
    pass#await database.connect()

@app.on_event("shutdown")
async def shutdown():
    pass#await database.disconnect()


@app.route('/')
async def homepage(request):
    return JSONResponse({'hello': 'world'})



if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)