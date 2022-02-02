from functools import wraps
from http import HTTPStatus

import grpc
from fastapi import HTTPException, Request

from auth_grpc.auth_pb2 import CheckRoleRequest
from auth_grpc.auth_pb2_grpc import AuthStub
from core import config
from tracer import tracer

auth_channel = grpc.insecure_channel(f'{config.AUTH_GRPC_HOST}:{config.AUTH_GRPC_PORT}')
auth_client = AuthStub(auth_channel)


def check_permission(roles: list):
    def check_user_role(func):
        @wraps(func)
        async def wrapper(*args, request: Request, **kwargs):
            token = request.headers.get('Authorization', None)
            if not token:
                if 'degrading' in kwargs:
                    kwargs['degrading'] = True
                    return await func(*args, request, **kwargs)
                else:
                    raise HTTPException(
                        status_code=HTTPStatus.BAD_REQUEST,
                        detail='Authorization token needed',
                    )

            token = token.replace('Bearer ', '')
            auth_request = CheckRoleRequest(access_token=token, roles=roles)

            with tracer.start_span('check-role') as span:
                request_id = request.headers.get('X-Request-Id')
                span.set_tag('http.request_id', request_id)
                span.set_tag('requested_API_endpoint', request.url.path)
                try:
                    auth_response = auth_client.CheckRole(auth_request)
                    span.set_tag('response_from_auth', auth_response)
                except Exception as e:
                    span.set_tag('response_from_auth', f'Error: {e}')
                    if 'degrading' in kwargs:
                        kwargs['degrading'] = True
                        return await func(*args, request, **kwargs)
                    else:
                        raise HTTPException(
                            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                            detail='Technical work is underway',
                        )

            if auth_response.result:
                if 'degrading' in kwargs:
                    kwargs['degrading'] = True
                    return await func(*args, request, **kwargs)
                else:
                    raise HTTPException(
                        status_code=HTTPStatus.FORBIDDEN, detail='Dont have an access'
                    )

            return await func(*args, request, **kwargs)

        return wrapper

    return check_user_role
