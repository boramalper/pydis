from typing import *

import asyncio
import collections
import itertools
import sys
import time

import hiredis
import uvloop


expiration = {}  # type: Dict[bytes, float]
dictionary = {}  # type: Dict[bytes, Any]


class RedisProtocol(asyncio.Protocol):
    def __init__(self):
        self.parser = hiredis.Reader()
        self.transport = None  # type: asyncio.transports.Transport
        self.commands = {
            b"COMMAND": self.command,
            b"SET": self.set,
            b"GET": self.get,
            b"PING": self.ping,
            b"INCR": self.incr,
            b"LPUSH": self.lpush,
            b"RPUSH": self.rpush,
            b"LPOP": self.lpop,
            b"RPOP": self.rpop,
            b"SADD": self.sadd,
            b"HSET": self.hset,
            b"SPOP": self.spop,
            b"LRANGE": self.lrange,
            b"MSET": self.mset,
        }

    def connection_made(self, transport: asyncio.transports.Transport):
        self.transport = transport

    def resume_writing(self):
        print("RESUME!")

    def pause_writing(self):
        print("PAUSE!")

    def data_received(self, data: bytes):
        if data == b"PING\r\n":
            self.transport.write(b"+PONG\r\n")
            return

        self.parser.feed(data)
        while True:
            req = self.parser.gets()
            if req == False:  # Do NOT simplify!
                break

            self.commands[req[0]](*req[1:])

    def command(self):
        # Far from being a complete implementation of the `COMMAND` command of
        # Redis, yet sufficient for us to start using redis-cli.
        self.transport.write(b"+OK\r\n")

    def set(self, *args):
        # Defaults
        key = args[0]
        value = args[1]
        expires_at = None
        cond = b""

        if len(args) == 3:
            # SET key value [NX|XX]
            cond = args[2]
        elif len(args) >= 4:
            # SET key value [EX seconds | PX milliseconds] [NX|XX]
            try:
                if args[2] == b"EX":
                    duration = int(args[3])
                elif args[2] == b"PX":
                    duration = int(args[3]) / 1000
                else:
                    self.transport.write(b"-ERR syntax error\r\n")
                    return
            except ValueError:
                self.transport.write(b"-value is not an integer or out of range\r\n")
                return

            if duration <= 0:
                self.transport.write(b"-ERR invalid expire time in set\r\n")
                return

            expires_at = time.monotonic() + duration

            if len(args) == 5:
                cond = args[4]

        if cond == b"":
            pass
        elif cond == b"NX":
            if key in dictionary:
                self.transport.write(b"$-1\r\n")
                return
        elif cond == b"XX":
            if key not in dictionary:
                self.transport.write(b"$-1\r\n")
                return
        else:
            self.transport.write(b"-ERR syntax error\r\n")
            return

        if expires_at:
            expiration[key] = expires_at

        dictionary[key] = value
        self.transport.write(b"+OK\r\n")

    def get(self, key):
        try:
            value = dictionary[key]
        except KeyError:
            self.transport.write(b"$-1\r\n")

        try:
            if expiration[key] < time.monotonic():
                del dictionary[key]
                del expiration[key]
                self.transport.write(b"$-1\r\n")
        except KeyError:
            self.transport.write(b"$%d\r\n%s\r\n" % (len(value), value))

    def ping(self, message=b"PONG"):
        self.transport.write(b"$%d\r\n%s\r\n" % (len(message), message))

    def incr(self, key):
        value = dictionary.get(key, 0)
        if type(value) is str:
            try:
                value = int(value)
            except ValueError:
                self.transport.write(b"-value is not an integer or out of range\r\n")
                return
        value += 1
        dictionary[key] = str(value)
        self.transport.write(b":%d\r\n" % (value,))

    def lpush(self, key, *values):
        deque = dictionary.get(key, collections.deque())
        for value in values:
            deque.appendleft(value)
        dictionary[key] = deque
        self.transport.write(b":%d\r\n" % (len(deque),))

    def rpush(self, key, *values):
        deque = dictionary.get(key, collections.deque())
        for value in values:
            deque.append(value)
        dictionary[key] = deque
        self.transport.write(b":%d\r\n" % (len(deque),))

    def lpop(self, key):
        try:
            deque = dictionary[key]  # type: collections.deque
        except KeyError:
            self.transport.write(b"$-1\r\n")
            return
        value = deque.popleft()
        self.transport.write(b"$%d\r\n%s\r\n" % (len(value), value))

    def rpop(self, key):
        try:
            deque = dictionary[key]  # type: collections.deque
        except KeyError:
            self.transport.write(b"$-1\r\n")
            return
        value = deque.pop()
        self.transport.write(b"$%d\r\n%s\r\n" % (len(value), value))

    def sadd(self, key, *members):
        set_ = dictionary.get(key, set())
        prev_size = len(set_)
        for member in members:
            set_.add(member)
        dictionary[key] = set_
        self.transport.write(b":%d\r\n" % (len(set_) - prev_size,))

    def hset(self, key, field, value):
        hash_ = dictionary.get(key, {})
        ret = int(field in hash_)
        hash_[field] = value
        dictionary[key] = hash_
        self.transport.write(b":%d\r\n" % (ret,))

    def spop(self, key):  # TODO add `count`
        try:
            set_ = dictionary[key]  # type: set
            elem = set_.pop()
        except KeyError:
            self.transport.write(b"$-1\r\n")
            return
        self.transport.write(b"$%d\r\n%s\r\n" % (len(elem), elem))

    def lrange(self, key, start, stop):
        start = int(start)
        stop = int(stop)
        try:
            deque = dictionary[key]  # type: collections.deque
        except KeyError:
            self.transport.write(b"$-1\r\n")
            return
        l = list(itertools.islice(deque, start, stop))
        self.transport.write(b"*%d\r\n%s" % (len(l), b"".join([b"$%d\r\n%s\r\n" % (len(e), e) for e in l])))

    def mset(self, *args):
        for i in range(0, len(args), 2):
            key = args[i]
            value = args[i + 1]
            dictionary[key] = value
        self.transport.write(b"+OK\r\n")


def main() -> int:
    print("Hello, World!")

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    loop = asyncio.get_event_loop()
    # Each client connection will create a new protocol instance
    coro = loop.create_server(RedisProtocol, "127.0.0.1", 7878)
    server = loop.run_until_complete(coro)

    # Serve requests until Ctrl+C is pressed
    print('Serving on {}'.format(server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    # Close the server
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except:
        sys.exit(-1)
