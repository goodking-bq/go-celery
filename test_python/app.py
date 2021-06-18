from celery import Celery
app = Celery(
    broker="redis://",
    backend="redis://"
)

app.conf.task_protocol = 2


@app.task(name="worker.add")
def add(a: int, b: int):
    raise Exception("test")
    return a+b


if __name__ == '__main__':
    t = add.apply_async(kwargs={"a": 5456, "b": 2878}, serializer='json')
    print(t.id, t.result)
    t.wait()
    print(t.id, t.result)
