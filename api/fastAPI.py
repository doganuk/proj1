import os
import time
import boto3
from typing import Optional
from uuid import uuid4
from fastapi import FastAPI, HTTPException
from mangum import Mangum
from pydantic import BaseModel
from boto3.dynamodb.conditions import Key
import json
from random import randint, random

app = FastAPI()
handler = Mangum(app)


class PutTaskRequest(BaseModel):
    source: str
    sink: Optional[str] = None
    airline: Optional[str] = None
    departure_dt: Optional[str] = None
    arrival_dt: Optional[str] = None
    number_of_stops: Optional[str] = None
    emissions: Optional[str] = None
    price: Optional[str] = None


@app.get("/")
async def root():
    return {"message": "Hello from fastAPI API!"}




@app.get("/")
async def root():
    return {"message": "Hello from fastAPI API!"}

@app.put("/seed112")
async def list_flights(put_task_request: PutTaskRequest):
    with open('generatedData.json', 'r') as myfile:
        data=myfile.read()
    # parse file
    objects = json.loads(data)

    #instance_id and cluster_id is the Key in dynamodb table 

    # for object in objects:
    item = {
    # "task_id":"k1",
    # "user_id":"k1",
    "source": f"task_{randint(0, 2400)}",
    "sink": "35",
    "airline": "pegasus",
    "departure_dt": "2024/17/01",
    "arrival_dt": f"task_{1}",
    "number_of_stops": int(86400),  # Expire after 24 hours.
    "emissions": 1,
    "price": f"task_{uuid4().hex}",
    }
                
    table = _get_table()
    table.put_item(Item=item)
    
    return {"task": objects[0].task.source}
    
@app.get("/list-flights/{source}")
async def list_flights(source: str):
    # List the top N tasks from the table, using the user index.
    table = _get_table()
    response = table.query(
        KeyConditionExpression=Key("source").eq(source),
        Limit=10,
    )
    flights = response.get("Items")
    return {"flights": flights}


# @app.put("/create-task")
# async def create_task(put_task_request: PutTaskRequest):
#     created_time = int(time.time())
#     item = {
#         "user_id": put_task_request.user_id,
#         "content": put_task_request.content,
#         "is_done": False,
#         "created_time": created_time,
#         "task_id": f"task_{uuid4().hex}",
#         "ttl": int(created_time + 86400),  # Expire after 24 hours.
#     }

#     # Put it into the table.
#     table = _get_table()
#     table.put_item(Item=item)
#     return {"task": item}


# @app.get("/get-task/{task_id}")
# async def get_task(task_id: str):
#     # Get the task from the table.
#     table = _get_table()
#     response = table.get_item(Key={"task_id": task_id})
#     item = response.get("Item")
#     if not item:
#         raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
#     return item




# @app.put("/update-task")
# async def update_task(put_task_request: PutTaskRequest):
#     # Update the task in the table.
#     table = _get_table()
#     table.update_item(
#         Key={"task_id": put_task_request.task_id},
#         UpdateExpression="SET content = :content, is_done = :is_done",
#         ExpressionAttributeValues={
#             ":content": put_task_request.content,
#             ":is_done": put_task_request.is_done,
#         },
#         ReturnValues="ALL_NEW",
#     )
#     return {"updated_task_id": put_task_request.task_id}


# @app.delete("/delete-task/{task_id}")
# async def delete_task(task_id: str):
#     # Delete the task from the table.
#     table = _get_table()
#     table.delete_item(Key={"task_id": task_id})
#     return {"deleted_task_id": task_id}


def _get_table():
    table_name = os.environ.get("TABLE_NAME")
    return boto3.resource("dynamodb").Table(table_name)
