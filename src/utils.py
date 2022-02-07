import orjson as orjson
from pydantic import BaseModel
import jwt


def orjson_dumps(v, *, default):
    return orjson.dumps(v, default=default).decode()


class CustomBaseModel(BaseModel):
    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps


def get_user_login(request):
    token = request.headers.get('Authorization', None)
    token = token.replace('Bearer ', '')
    decoded_token = jwt.decode(token, options={"verify_signature": False})
    login = decoded_token.get('sub', None)
    return login
