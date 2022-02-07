from sentry_sdk import capture_exception

from senders.send_email import EmailSender
from senders.send_push import PushSender
from mongo_adapter import MongoAdapter
from tracer import tracer
import copy


class EventHandler:

    def __init__(self, info: dict, event_type: str, request_id: str, mongo_adapter: MongoAdapter):
        self.notification_types = info.pop('notification_types')
        self.context = info
        self.event_type = event_type
        self.request_id = request_id
        self.mongo_adapter = mongo_adapter

    async def send_email(self):
        email_sender = EmailSender(event_type=self.event_type, context=self.context)
        await email_sender.send()

    async def send_push(self):
        push_sender = PushSender(event_type=self.event_type, context=self.context)
        await push_sender.send()

    async def run(self):

        notification_mapping = {
            'email': self.send_email,
            'push': self.send_push
        }

        for notification in self.notification_types:
            with tracer.start_span('send-notification_by_event') as span:
                span.set_tag('http.request_id', self.request_id)
                span.set_tag('event_type', self.event_type)
                span.set_tag('notification_type', notification)
                try:
                    context = copy.deepcopy(self.context)
                    del context['receivers_emails']
                    await notification_mapping[notification]()
                    for email in self.context['receivers_emails']:
                        await self.mongo_adapter.add_object_to_db()
                    await self.mongo_adapter.add_object_to_db({"email": email,
                                                               "event_type": self.event_type,
                                                               "context": context})
                    span.set_tag('send_status:', 'success')
                except Exception as e:
                    capture_exception(e)
                    span.set_tag('send_status:', f'Error: {e}')
