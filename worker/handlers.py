import os

from senders.send_email import EmailSender


class UserRegistrationHandler:
    def __init__(self, info: dict):
        self.email = info.pop('email')
        self.context = info

    def run(self):
        email_sender = EmailSender(event_type=os.getenv('USER_REGISTRATION_QUEUE'),
                                   receivers_emails=[self.email],
                                   context=self.context)
        email_sender.send()
