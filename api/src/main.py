from fastapi import FastAPI

app = FastAPI(title="Reto Chapter Lead Data Engineer")

@app.get("/health")
def health():
    return {"status": "ok"}
