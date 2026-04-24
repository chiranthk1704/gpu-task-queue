from fastapi import FastAPI
from sqlalchemy import create_engine, Column, String, Integer, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

app = FastAPI()
engine = create_engine('sqlite:///tasks.db', connect_args={"check_same_thread": False})
Base = declarative_base()

class Task(Base):
    __tablename__ = "tasks"
    id = Column(Integer, primary_key=True)
    task_data = Column(JSON)
    status = Column(String, default="pending")
    result = Column(JSON, nullable=True)
    worker_id = Column(String, nullable=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

@app.get('/health')
def health():
    return {"status": "ok"}

@app.post('/add_task')
def add_task(data: dict):
    session = Session()
    task = Task(task_data=data)
    session.add(task)
    session.commit()
    task_id = task.id
    session.close()
    return {"task_id": task_id, "status": "queued"}

@app.get('/next_task')
def get_next_task():
    session = Session()
    task = session.query(Task).filter_by(status="pending").first()
    if task:
        task.status = "running"
        session.commit()
        response = {
            "task_id": task.id,
            "config": task.task_data.get('config', {}),
            "model_code": task.task_data.get('model_code', '')
        }
        session.close()
        return response
    session.close()
    return {"task_id": None}

@app.post('/complete_task')
def complete_task(task_id: int, result: dict):
    session = Session()
    task = session.query(Task).filter_by(id=task_id).first()
    if task:
        task.status = result.get('status', 'completed')
        task.result = result
        session.commit()
    session.close()
    return {"status": "saved"}

@app.get('/task/{task_id}')
def get_task(task_id: int):
    session = Session()
    task = session.query(Task).filter_by(id=task_id).first()
    if task:
        result = {
            "id": task.id,
            "status": task.status,
            "result": task.result,
            "worker": task.worker_id
        }
        session.close()
        return result
    session.close()
    return {}

@app.get('/tasks')
def get_tasks():
    session = Session()
    tasks = session.query(Task).all()
    result = [{"id": t.id, "status": t.status, "result": t.result} for t in tasks]
    session.close()
    return result