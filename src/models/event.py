from pydantic.typing import Literal

from utils import CustomBaseModel


class Event(CustomBaseModel):
    receivers_emails: list[str]
    title: str
    text: str
    notification_types: list[Literal['email', 'push']]

