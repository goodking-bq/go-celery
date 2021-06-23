from celery import Celery

app = Celery(
    broker="redis://",
    backend="redis://"
)

app.conf.task_protocol = 2


@app.task(bind=True, name="worker.add")
def add(self, a: int, b: int):
    print(dir(self))
    self.update_state("ddddddd")
    # time.sleep(60)
    return a + b


if __name__ == '__main__':
    t = add.apply_async(args=(1,), kwargs={"b": 2878}, serializer='json', delay=10, max_retries=10)
    # print(t.status)
    # t.wait()
    # print(t.id, t.result)
