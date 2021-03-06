import asyncio
import os
import aiosmtplib
from email.message import EmailMessage
from jinja2 import Environment, FileSystemLoader

from senders.sender import Sender

CHUNK_SIZE = 200
BASE_DIR = os.path.dirname(__file__)
TEMPLATE_DIR = os.path.join(BASE_DIR, '../templates')


class EmailSender(Sender):
    TEMPLATES = {
        os.getenv('USER_REGISTRATION_QUEUE'): 'welcome_mail.html',
        os.getenv('CUSTOM_NOTIFICATION_QUEUE'): 'mail.html',
        os.getenv('COMMON_WEEK_QUEUE'): 'new_films_mail.html',
        os.getenv('PERSONAL_WEEK_QUEUE'): 'recomendations_films_mail.html',
    }
    SUBJECTS = {
        os.getenv('USER_REGISTRATION_QUEUE'): 'WELCOME',
        os.getenv('CUSTOM_NOTIFICATION_QUEUE'): 'MASS MAILING',
        os.getenv('COMMON_WEEK_QUEUE'): 'NEW FILMS',
        os.getenv('PERSONAL_WEEK_QUEUE'): 'FILMS FOR YOU',
    }

    def __init__(self, event_type: str, context: dict):
        self.server = aiosmtplib.SMTP('smtp.yandex.ru', 465, use_tls=True)
        self.mail_user = os.getenv('SMTP_USER')
        self.mail_password = os.getenv('SMTP_PASSWORD')
        self.from_ = self.mail_user
        self.to = context.pop('receivers_emails')
        self.event_type = event_type
        self.context = context
        self.subject = self.SUBJECTS[self.event_type]
        self.template = self.set_template()

    def set_template(self):
        templates_env = Environment(loader=FileSystemLoader(f'{TEMPLATE_DIR}'), enable_async=True)
        template_html = self.TEMPLATES[self.event_type]
        return templates_env.get_template(template_html)

    async def prepare_message(self):
        message = EmailMessage()
        message["From"] = self.from_
        message["To"] = ",".join(self.to)
        message["Subject"] = self.subject

        output = await self.template.render_async(**self.context)

        message.add_alternative(output, subtype='html')
        return message

    async def send_message_by_chunks(self, message):
        amount_sent = 0
        while amount_sent < len(self.to):
            await self.server.sendmail(self.mail_user,
                                       self.to[amount_sent:amount_sent + CHUNK_SIZE],
                                       message.as_string())
            amount_sent += CHUNK_SIZE
            await asyncio.sleep(5)

    async def send(self):
        await self.server.connect()
        await self.server.login(self.mail_user, self.mail_password)

        message = await self.prepare_message()

        await self.send_message_by_chunks(message)

        await self.server.quit()
