# pydis
**pydis** is an experiment to disprove some of the falsehoods about performance
and optimisation regarding software and interpreted languages in particular.

Below you will find a [Redis](https://github.com/antirez/redis/) clone,
**pydis**, written *in ~250 lines of idiomatic Python code*, providing a
subset of redis' functionality for which there are
[official benchmarks](https://redis.io/topics/benchmarks).

Briefly, **pydis** is ~50% as fast as Redis measured in number operations per
second.

P.S. This is not a criticism of Redis, which is a brilliant project and a 
system-level software that powers thousands of infrastructures. It just happened
to be one of the fastest software I could imagine *and* clone the same day.

## Disclaimer
I have used the following libraries written in C for performance:

- [uvloop](https://github.com/MagicStack/uvloop)

  > uvloop is a fast, drop-in replacement of the built-in asyncio event loop. uvloop is implemented in Cython and uses libuv under the hood.
                                                    >
- [hiredis](https://pypi.org/project/hiredis/)

  > Python extension that wraps protocol parsing code in hiredis.

## Discussion
- Hacker News
  1. 2020-11-15 -- [Pydis – Redis clone in 250 lines of Python, for performance comparison ](https://news.ycombinator.com/item?id=25100218)
  1. 2019-03-02 -- [Pydis: Redis clone in Python 3 to make points about performance](https://news.ycombinator.com/item?id=19287717)
  

- Reddit
  1. 2019-03-01 -- [pydis - A redis clone in Python 3 to disprove some falsehoods about performance](https://www.reddit.com/r/Python/comments/awav6k/pydis_a_redis_clone_in_python_3_to_disprove_some/) 
  
## Results
```bash
redis-benchmark -q -t set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,lrange,mset -n 100000 -P 5 
```

  - 100,000 requests in total per command.
  - Requests are pipelined in groups of 5.

<center>
  <img src="plot.svg" alt="The Bar Graph">
</center>

Benchmark | pydis | redis | Ratio
--- | ---: | ---: | ---
SET | 271,947 | 467,361 | 0.582
GET | 274,283 | 467,237 | 0.587
INCR | 213,409 | 478,669 | 0.446
LPUSH | 216,082 | 381,033 | 0.567
RPUSH | 231,143 | 399,238 | 0.579
LPOP | 248,527 | 384,332 | 0.647
RPOP | 241,144 | 429,971 | 0.561
SADD | 219,475 | 434,257 | 0.505
HSET | 220,178 | 377,637 | 0.583
SPOP | 288,068 | 477,705 | 0.603
LRANGE (100) | 26,170 | 96,254 | 0.272
LRANGE (300) | 9,163 | 24,768 | 0.370
LRANGE (500) | 5,771 | 19,351 | 0.298
LRANGE (600) | 4,705 | 13,869 | 0.339
MSET | 125,215 | 195,121 | 0.642

### Host System
- Ubuntu 20.04
- Python 3.8.5 (GCC 9.3.0)
- Redis 5.0.7 `malloc=jemalloc-5.2.1 bits=64 build=636cde3b5c7a3923`

## Contributions
Contributions are very welcome, given that they fall into one of the following
categories:

- Those that improve the performance.
  - The aim of this exercise is to prove that interpreted languages can be just
    as fast as C. So whilst using a faster parser in C with Python bindings is
    okay, rewriting **pydis** in [Cython](https://cython.org/) is not.
  - I will accept "minor" deviations from idioms only if the performance gains
    are worth it; stick to idiomatic Python otherwise!
- Those to achieve feature parity with Redis *for which there are official
  benchmarks*.
  - We are not trying to develop a full-featured Redis clone here so please do 
    not implement commands for which there are no official benchmarks.
- Those that fix formatting etc.
  - Please do not invent your own style, use PEP 8.
