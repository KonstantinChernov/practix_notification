import asyncio
import os
from concurrent import futures

import grpc

import notify_registration_pb2
import notify_registration_pb2_grpc

from sentry_sdk import capture_exception
import sentry_sdk

from send_event import EventSender


sentry_sdk.init(
    os.getenv('SENTRY_DSN'),

    traces_sample_rate=1.0
)
loop = asyncio.get_event_loop()


class NotifyNewUser(notify_registration_pb2_grpc.NotifyRegisterServicer):
    def UserRegisterEvent(self, request, context):
        event_type = os.getenv('USER_REGISTRATION_QUEUE')
        send_context = {
            "email": request.email,
            "login": request.login,
            "password": request.password,
        }
        request_id_header = request.request_id,

        sender = EventSender(loop)
        try:
            sender.send_event(send_context, event_type, headers={'request_id': request_id_header})
        except Exception as e:
            capture_exception(e)
            return notify_registration_pb2.UserRegisteredResponse(result=False)

        return notify_registration_pb2.UserRegisteredResponse(result=True)


if __name__ == '__main__':
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    notify_registration_pb2_grpc.add_NotifyRegisterServicer_to_server(NotifyNewUser(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()
