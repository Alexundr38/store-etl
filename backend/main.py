import uvicorn
from fastapi import FastAPI

from config import get_backend_port
from routers import consumer_router, category_router, item_router, cart_router

app = FastAPI()

app.include_router(consumer_router.router)
app.include_router(category_router.router)
app.include_router(item_router.router)
app.include_router(cart_router.router)

@app.get("/")
async def root():
    return {"message": "API started"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=get_backend_port())