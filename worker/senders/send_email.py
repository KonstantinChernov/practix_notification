import asyncio
import os
import aiosmtplib
from email.message import EmailMessage
from jinja2 import Environment, FileSystemLoader

CHUNK_SIZE = 200
BASE_DIR = os.path.dirname(__file__)
TEMPLATE_DIR = os.path.join(BASE_DIR, '../templates')


class EmailSender:
    TEMPLATES = {
        os.getenv('USER_REGISTRATION_QUEUE'): 'welcome_mail.html',
        os.getenv('CUSTOM_EMAIL_QUEUE'): 'mail.html',

    }
    SUBJECTS = {
        os.getenv('USER_REGISTRATION_QUEUE'): 'WELCOME',
        os.getenv('CUSTOM_EMAIL_QUEUE'): 'MASS MAILING',
    }

    def __init__(self, event_type: str, receivers_emails: list, context: dict):
        self.server = aiosmtplib.SMTP('smtp.yandex.ru', 465, use_tls=True)
        self.mail_user = os.getenv('SMTP_USER')
        self.mail_password = os.getenv('SMTP_PASSWORD')
        self.from_ = self.mail_user
        self.to = receivers_emails
        self.event_type = event_type
        self.context = context
        self.subject = self.SUBJECTS[self.event_type]
        self.template = self.set_template()
        self.message = self.prepare_message()

    def set_template(self):
        templates_env = Environment(loader=FileSystemLoader(f'{TEMPLATE_DIR}'), enable_async=True)
        template_html = self.TEMPLATES[self.event_type]
        return templates_env.get_template(template_html)

    def prepare_message(self):
        message = EmailMessage()
        message["From"] = self.from_
        message["To"] = ",".join(self.to)
        message["Subject"] = self.subject

        output = await self.template.render_async(**self.context)

        message.add_alternative(output, subtype='html')
        return message

    async def send_messages_by_chunks(self):
        letters_sent = 0
        while letters_sent < len(self.to):
            await self.server.sendmail(self.mail_user,
                                       self.to[letters_sent:letters_sent + CHUNK_SIZE],
                                       self.message.as_string())
            letters_sent += CHUNK_SIZE
            await asyncio.sleep(5)

    async def send(self):
        await self.server.connect()
        await self.server.login(self.mail_user, self.mail_password)

        await self.send_messages_by_chunks()

        await self.server.quit()