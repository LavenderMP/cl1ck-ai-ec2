# main.py
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from celery import Celery
from leadprocessing import processLeads

app = FastAPI()

# Celery configuration
celery = Celery("tasks", broker="pyamqp://guest:guest@localhost//")


class TaskInput(BaseModel):
    exp_name: str
    receivedEmail: str = ''


@celery.task
def perform_background_task(task_name: str, message: str):
    # Simulate a time-consuming task
    print(f"Executing background task ({task_name}): {message}")


@app.post("/run-background-task")
async def run_background_task(input_data: TaskInput, background_tasks: BackgroundTasks):
    exp_name = input_data.exp_name
    receivedEmail = input_data.receivedEmail
    # Enqueue the background task
    background_tasks.add_task(processLeads, exp_name, receivedEmail)

    return {"message": f"Background task ({exp_name}) enqueued successfully."}
