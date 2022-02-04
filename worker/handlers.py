from sentry_sdk import capture_exception

from senders.send_email import EmailSender
from tracer import tracer


class EventHandler:
    def __init__(self, info: dict, event_type: str, request_id: str):
        self.receivers_emails = info.pop('receivers_emails')
        self.context = info
        self.event_type = event_type
        self.request_id = request_id

    async def send_email(self):

        with tracer.start_span('send-notification_by_event') as span:
            span.set_tag('http.request_id', self.request_id)
            span.set_tag('event_type', self.event_type)
            try:
                email_sender = EmailSender(event_type=self.event_type,
                                           receivers_emails=self.receivers_emails,
                                           context=self.context)
                await email_sender.send()
                span.set_tag('send_email_status:', 'success')
            except Exception as e:
                capture_exception(e)
                span.set_tag('send_email_status:', f'Error: {e}')

    async def send_push(self):
        pass

    async def run(self):

        await self.send_email()
        await self.send_push()
