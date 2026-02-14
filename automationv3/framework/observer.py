from functools import partial


class Observer:
    pass


class ObserverManager:
    def __init__(self):
        self.observers = set()

    def add_observer(self, observer):
        self.observers.add(observer)

    def notify(self, event, *args, **kwargs):
        for observer in self.observers:
            if hasattr(observer, "on_" + event):
                getattr(observer, "on_" + event)(observer, *args, **kwargs)

    def __getattr__(self, name):
        if name.startswith("on_"):
            return partial(self.notify, name[3:])
        else:
            raise AttributeError

    def on_procedure_begin(self, *args, **kwargs):
        self.notify("procedure_begin", *args, **kwargs)

    def on_step_start(self, *args, **kwargs):
        self.notify("step_start", *args, **kwargs)

    def on_step_end(self, *args, **kwargs):
        self.notify("step_end", *args, **kwargs)

    def on_procedure_end(self, *args, **kwargs):
        self.notify("procedure_end", *args, **kwargs)

    def on_comment(self, *args, **kwargs):
        self.notify("comment", *args, **kwargs)
