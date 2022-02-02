import os
import smtplib
from email.message import EmailMessage
from jinja2 import Environment, FileSystemLoader

BASE_DIR = os.path.dirname(__file__)
TEMPLATE_DIR = os.path.join(BASE_DIR, '../templates')


class EmailSender:
    TEMPLATES = {
        os.getenv('USER_REGISTRATION_QUEUE'): 'welcome_mail.html'
    }
    SUBJECTS = {
        os.getenv('USER_REGISTRATION_QUEUE'): 'WELCOME'
    }

    def __init__(self, event_type: str, receivers_emails: list, context: dict):
        self.mail_user = os.getenv('SMTP_USER')
        self.mail_password = os.getenv('SMTP_PASSWORD')

        self.from_ = self.mail_user
        self.to = receivers_emails
        self.event_type = event_type
        self.context = context
        self.subject = self.SUBJECTS[self.event_type]
        self.template = self.set_template()

    def set_template(self):
        templates_env = Environment(loader=FileSystemLoader(f'{TEMPLATE_DIR}'))
        template_html = self.TEMPLATES[self.event_type]
        return templates_env.get_template(template_html)

    def send(self):
        server = smtplib.SMTP_SSL('smtp.yandex.ru', 465)
        server.login(self.mail_user, self.mail_password)

        message = EmailMessage()
        message["From"] = self.from_
        message["To"] = ",".join(self.to)
        message["Subject"] = self.subject

        output = self.template.render(**self.context)

        message.add_alternative(output, subtype='html')
        server.sendmail(self.mail_user, self.to, message.as_string())
        server.close()
