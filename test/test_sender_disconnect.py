"""Exercise the actual sender around reader-owned socket shutdown."""
import errno
import socket
import threading

from ros_tcp_endpoint.tcp_sender import UnityTcpSender


class Logs:
    def __init__(self):
        self.info=[];self.errors=[]
    def loginfo(self,message):self.info.append(message)
    def logerr(self,message):self.errors.append(message)


def test_reader_close_during_send_is_lifecycle_information():
    local,peer=socket.socketpair();entered=threading.Event();released=threading.Event();halt=threading.Event()
    logs=Logs();sender=UnityTcpSender(logs)
    class PausedSocket:
        def sendall(self,payload):
            entered.set()
            assert released.wait(2),'Reader did not release the paused send'
            local.sendall(payload)
    thread=threading.Thread(target=sender.sender_loop,args=(PausedSocket(),1,halt))
    thread.start()
    try:
        assert entered.wait(2),'Actual sender did not begin its handshake'
        # Mirror ClientThread.finally: publish the halt state before closing.
        halt.set();local.close();released.set();thread.join(2)
        assert not thread.is_alive()
        assert not logs.errors,logs.errors
        assert any('Connection closed' in line for line in logs.info)
        assert sender.queue is None
    finally:
        halt.set();released.set();local.close();peer.close();thread.join(2)


def test_closed_descriptor_while_active_remains_an_error():
    local,peer=socket.socketpair();local.close();logs=Logs();halt=threading.Event()
    try:
        sender=UnityTcpSender(logs);sender.sender_loop(local,1,halt)
        assert logs.errors and halt.is_set() and sender.queue is None
    finally:peer.close()


def test_other_io_errors_are_not_hidden_by_shutdown_state():
    logs=Logs();halt=threading.Event()
    class FailedSocket:
        def sendall(self,payload):
            halt.set();raise OSError(errno.EIO,'Unrelated I/O failure')
    UnityTcpSender(logs).sender_loop(FailedSocket(),1,halt)
    assert logs.errors
