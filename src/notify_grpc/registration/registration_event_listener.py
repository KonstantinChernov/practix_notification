from concurrent import futures

import grpc

import notify_registration_pb2
import notify_registration_pb2_grpc


from sentry_sdk import capture_exception

from services.send_event import EventSender


class NotifyNewUser(notify_registration_pb2_grpc.NotifyRegisterServicer):
    def UserRegisterEvent(self, request, context):
        event_type = "user_registration"
        send_context = {
            "email": request.email,
            "login": request.login,
            "password": request.password,
        }
        sender = EventSender(event_type=event_type, context=send_context)
        # try:
        #     sender.send()
        # except Exception as e:
        #     capture_exception(e)
        #     return notify_registration_pb2.UserRegisteredResponse(result=False)

        return notify_registration_pb2.UserRegisteredResponse(result=True)


if __name__ == '__main__':
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    notify_registration_pb2_grpc.add_NotifyRegisterServicer_to_server(NotifyNewUser(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()
