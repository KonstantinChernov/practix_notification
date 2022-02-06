from senders.sender import Sender


class PushSender(Sender):
    def __init__(self, event_type: str, context: dict):
        self.event_type = event_type
        self.context = context

    async def send(self):
        pass
