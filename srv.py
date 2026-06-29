from fastapi import FastAPI
import uvicorn

app = FastAPI()


@app.get("/payment")
def payment(payment: str):
    return {}



if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9000)
