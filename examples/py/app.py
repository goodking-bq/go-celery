import datetime

from celery import Celery
from kombu import Exchange, Queue

app = Celery(
    broker="amqp://",
    backend="redis://"
)

app.conf.task_protocol = 2
app.conf.task_queues = (
    Queue('default', Exchange('default'), routing_key='default'),
    Queue('schedule', Exchange('schedule'), routing_key='schedule'),
    Queue('for_task_backup', Exchange('for_task_backup'), routing_key='for_task_backup'),
    Queue('for_task_xiyouplat', Exchange('for_task_xiyouplat'), routing_key='for_task_xiyouplat'),
)


@app.task(bind=True, name="worker.add")
def add(self, a: int, b: int):
    print(dir(self))
    self.update_state("ddddddd")
    # time.sleep(60)
    return a + b


if __name__ == '__main__':
    t = add.apply_async(args=(1,), kwargs={"b": 2878}, serializer='json',
                        delay=10,
                        max_retries=10,
                        countdown=10,
                        expires=datetime.datetime.now(),
                        route_name="schedule")
    # print(t.status)
    # t.wait()
    # print(t.id, t.result)
