from fastapi import FastAPI

app = FastAPI(
    title="Ice-Stream Alert Server",
    description="Backend service for streaming data quality alerts",
    version="0.1.0",
)


@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "service": "alert-server"
    }


@app.get("/")
def root():
    return {
        "message": "Ice-Stream Alert Server is running"
    }