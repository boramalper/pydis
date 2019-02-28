from typing import *

import asyncio
import collections
import itertools
import sys
import time

import hiredis
import uvloop


expiration = collections.defaultdict(lambda: float("inf"))  # type: Dict[bytes, float]
dictionary = {}  # type: Dict[bytes, Any]


class RedisProtocol(asyncio.Protocol):
    def __init__(self):
        self.response = collections.deque()
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
        """
        if data == b"PING\r\n":
            return b"+PONG\r\n"
            return
        """
        self.parser.feed(data)

        while True:
            req = self.parser.gets()
            if req == False:  # Do NOT simplify!
                break
            self.response.append(self.commands[req[0]](*req[1:]))
        self.transport.write(b"".join(self.response))
        self.response.clear()

    def command(self):
        # Far from being a complete implementation of the `COMMAND` command of
        # Redis, yet sufficient for us to start using redis-cli.
        return b"+OK\r\n"

    def set(self, *args) -> bytes:
        # Defaults
        key = args[0]
        value = args[1]
        expires_at = None
        cond = b""

        largs = len(args)
        if largs == 3:
            # SET key value [NX|XX]
            cond = args[2]
        elif largs >= 4:
            # SET key value [EX seconds | PX milliseconds] [NX|XX]
            try:
                if args[2] == b"EX":
                    duration = int(args[3])
                elif args[2] == b"PX":
                    duration = int(args[3]) / 1000
                else:
                    return b"-ERR syntax error\r\n"
            except ValueError:
                return b"-value is not an integer or out of range\r\n"

            if duration <= 0:
                return b"-ERR invalid expire time in set\r\n"

            expires_at = time.monotonic() + duration

            if largs == 5:
                cond = args[4]

        if cond == b"":
            pass
        elif cond == b"NX":
            if key in dictionary:
                return b"$-1\r\n"
        elif cond == b"XX":
            if key not in dictionary:
                return b"$-1\r\n"
        else:
            return b"-ERR syntax error\r\n"

        if expires_at:
            expiration[key] = expires_at

        dictionary[key] = value
        return b"+OK\r\n"

    def get(self, key: bytes) -> bytes:
        if key not in dictionary:
            return b"$-1\r\n"

        if key in expiration and expiration[key] < time.monotonic():
            del dictionary[key]
            del expiration[key]
            return b"$-1\r\n"
        else:
            value = dictionary[key]
            return b"$%d\r\n%s\r\n" % (len(value), value)

    def ping(self, message=b"PONG"):
        return b"$%d\r\n%s\r\n" % (len(message), message)

    def incr(self, key):
        value = dictionary.get(key, 0)
        if type(value) is str:
            try:
                value = int(value)
            except ValueError:
                return b"-value is not an integer or out of range\r\n"
        value += 1
        dictionary[key] = str(value)
        return b":%d\r\n" % (value,)

    def lpush(self, key, *values):
        deque = dictionary.get(key, collections.deque())
        deque.extendleft(values)
        dictionary[key] = deque
        return b":%d\r\n" % (len(deque),)

    def rpush(self, key, *values):
        deque = dictionary.get(key, collections.deque())
        deque.extend(values)
        dictionary[key] = deque
        return b":%d\r\n" % (len(deque),)

    def lpop(self, key):
        try:
            deque = dictionary[key]  # type: collections.deque
        except KeyError:
            return b"$-1\r\n"
        value = deque.popleft()
        return b"$%d\r\n%s\r\n" % (len(value), value)

    def rpop(self, key):
        try:
            deque = dictionary[key]  # type: collections.deque
        except KeyError:
            return b"$-1\r\n"
        value = deque.pop()
        return b"$%d\r\n%s\r\n" % (len(value), value)

    def sadd(self, key, *members):
        set_ = dictionary.get(key, set())
        prev_size = len(set_)
        for member in members:
            set_.add(member)
        dictionary[key] = set_
        return b":%d\r\n" % (len(set_) - prev_size,)

    def hset(self, key, field, value):
        hash_ = dictionary.get(key, {})
        ret = int(field in hash_)
        hash_[field] = value
        dictionary[key] = hash_
        return b":%d\r\n" % (ret,)

    def spop(self, key):  # TODO add `count`
        try:
            set_ = dictionary[key]  # type: set
            elem = set_.pop()
        except KeyError:
            return b"$-1\r\n"
        return b"$%d\r\n%s\r\n" % (len(elem), elem)

    def lrange(self, key, start, stop):
        start = int(start)
        stop = int(stop)
        try:
            deque = dictionary[key]  # type: collections.deque
        except KeyError:
            return b"$-1\r\n"
        l = list(itertools.islice(deque, start, stop))
        return b"*%d\r\n%s" % (len(l), b"".join([b"$%d\r\n%s\r\n" % (len(e), e) for e in l]))

    def mset(self, *args):
        for i in range(0, len(args), 2):
            key = args[i]
            value = args[i + 1]
            dictionary[key] = value
        return b"+OK\r\n"


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
    sys.exit(main())
