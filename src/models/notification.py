from typing import Any

from utils import CustomBaseModel


class Notification(CustomBaseModel):
    id: Any
    email: str
    event_type: str
    context: dict
