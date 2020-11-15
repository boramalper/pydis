import asyncio
import collections
import itertools
import sys
import time
from typing import Any, Dict, Optional

import hiredis
import uvloop


expiration = collections.defaultdict(lambda: float("inf"))  # type: Dict[bytes, float]
dictionary = {}  # type: Dict[bytes, Any]


class RedisProtocol(asyncio.Protocol):
    def __init__(self):
        self.response = collections.deque()
        self.parser = hiredis.Reader()
        self.transport = None  # type: Optional[asyncio.transports.Transport]
        self.commands = {
            b"COMMAND": self.com_command,
            b"SET": self.com_set,
            b"GET": self.com_get,
            b"PING": self.com_ping,
            b"INCR": self.com_incr,
            b"LPUSH": self.com_lpush,
            b"RPUSH": self.com_rpush,
            b"LPOP": self.com_lpop,
            b"RPOP": self.com_rpop,
            b"SADD": self.com_sadd,
            b"HSET": self.com_hset,
            b"SPOP": self.com_spop,
            b"LRANGE": self.com_lrange,
            b"MSET": self.com_mset,
        }

    def connection_made(self, transport: asyncio.transports.Transport):
        self.transport = transport

    def data_received(self, data: bytes):
        self.parser.feed(data)

        while True:
            req = self.parser.gets()
            if req is False:
                break
            else:
                self.response.append(self.commands[req[0].upper()](*req[1:]))

        self.transport.writelines(self.response)
        self.response.clear()

    def evict_if_expired(self, key):
        if key in expiration and expiration[key] < time.monotonic():
            del dictionary[key]
            del expiration[key]

    def get(self, key, default=None):
        self.evict_if_expired(key)
        return dictionary.get(key, default)

    def set(self, key, value, expires_at: Optional[float] = None):
        """
        Sets key to value and clears expiration.
        :param key:
        :param value:
        :param expires_at:
        :return:
        """
        dictionary[key] = value
        if expires_at is not None:
            expiration[key] = expires_at
        else:
            expiration.pop(key, None)

    def com_command(self):
        # Far from being a complete implementation of the `COMMAND` command of
        # Redis, yet sufficient for us to start using redis-cli.
        return b"+OK\r\n"

    def com_set(self, *args) -> bytes:
        # Defaults
        key = args[0]
        value = args[1]
        expires_at = None
        cond = b""

        # Do not forget to evict keys if expired, which matters for NX and XX
        # flags.
        self.evict_if_expired(key)

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

        self.set(key, value, expires_at)
        return b"+OK\r\n"

    def com_get(self, key: bytes) -> bytes:
        value = self.get(key)
        if not value:
            return b"$-1\r\n"
        if not isinstance(value, bytes):
            return b"-WRONGTYPE Operation against a key holding the wrong kind of value"
        return b"$%d\r\n%s\r\n" % (len(value), value)

    def com_ping(self, message=b"PONG"):
        return b"$%d\r\n%s\r\n" % (len(message), message)

    def com_incr(self, key):
        value = self.get(key) or 0
        if type(value) is bytes:
            try:
                value = int(value)
            except ValueError:
                return b"-value is not an integer or out of range\r\n"
        value += 1
        self.set(key, str(value).encode("ascii"))
        return b":%d\r\n" % (value,)

    def com_lpush(self, key, *values):
        deque = self.get(key, collections.deque())
        if not isinstance(deque, collections.deque):
            return b"-WRONGTYPE Operation against a key holding the wrong kind of value"
        deque.extendleft(values)
        self.set(key, deque)
        return b":%d\r\n" % (len(deque),)

    def com_rpush(self, key, *values):
        deque = self.get(key, collections.deque())
        if not isinstance(deque, collections.deque):
            return b"-WRONGTYPE Operation against a key holding the wrong kind of value"
        deque.extend(values)
        self.set(key, deque)
        return b":%d\r\n" % (len(deque),)

    def com_lpop(self, key):
        deque = self.get(key)
        if deque is None:
            return b"$-1\r\n"
        if not isinstance(deque, collections.deque):
            return b"-WRONGTYPE Operation against a key holding the wrong kind of value"
        value = deque.popleft()
        return b"$%d\r\n%s\r\n" % (len(value), value)

    def com_rpop(self, key):
        deque = self.get(key)
        if deque is None:
            return b"$-1\r\n"
        if not isinstance(deque, collections.deque):
            return b"-WRONGTYPE Operation against a key holding the wrong kind of value"
        value = deque.pop()
        return b"$%d\r\n%s\r\n" % (len(value), value)

    def com_sadd(self, key, *members):
        set_ = self.get(key, set())
        if not isinstance(set_, set):
            return b"-WRONGTYPE Operation against a key holding the wrong kind of value"
        prev_size = len(set_)
        for member in members:
            set_.add(member)
        self.set(key, set_)
        return b":%d\r\n" % (len(set_) - prev_size,)

    def com_hset(self, key, field, value):
        hash_ = self.get(key, {})
        if not isinstance(hash_, dict):
            return b"-WRONGTYPE Operation against a key holding the wrong kind of value"
        ret = int(field in hash_)
        hash_[field] = value
        self.set(key, hash_)
        return b":%d\r\n" % (ret,)

    def com_spop(self, key):  # TODO add `count`
        set_ = self.get(key)
        if set_ is None:
            return b"$-1\r\n"
        if not isinstance(set_, set):
            return b"-WRONGTYPE Operation against a key holding the wrong kind of value"
        if len(set_) == 0:
            return b"$-1\r\n"
        elem = set_.pop()
        return b"$%d\r\n%s\r\n" % (len(elem), elem)

    def com_lrange(self, key, start, stop):
        start = int(start)
        stop = int(stop)
        deque = self.get(key)
        if deque is None:
            return b"$-1\r\n"
        if not isinstance(deque, collections.deque):
            return b"-WRONGTYPE Operation against a key holding the wrong kind of value"
        l = list(itertools.islice(deque, start, stop + 1))
        return b"*%d\r\n%s" % (len(l), b"".join(b"$%d\r\n%s\r\n" % (len(e), e) for e in l))

    def com_mset(self, *args):
        for i in range(0, len(args), 2):
            key = args[i]
            value = args[i + 1]
            self.set(key, value)
        return b"+OK\r\n"


def main() -> int:
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    loop = asyncio.get_event_loop()
    # Each client connection will create a new protocol instance
    coro = loop.create_server(RedisProtocol, "127.0.0.1", 7878)
    server = loop.run_until_complete(coro)

    # Serve requests until Ctrl+C is pressed
    print("Serving on {}".format(server.sockets[0].getsockname()))
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
