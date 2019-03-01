#!/usr/bin/env bash

for i in {1..10}; do
	echo pydis $i;
	redis-benchmark -t set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,lrange,mset -n 10000 -q -P 100 -p 7878 --csv > pydis_$i.csv
done

for i in {1..10}; do
        echo redis $i;
        redis-benchmark -t set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,lrange,mset -n 10000 -q -P 100 --csv > redis_$i.csv
done
