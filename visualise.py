#!/usr/bin/env python3

import csv
import collections
import math

import matplotlib.pyplot as plt


def main():
    raw = {
        "pydis": collections.defaultdict(list),
        "redis": collections.defaultdict(list),
    }

    for prog in ["pydis", "redis"]:
        for i in range(1, 4):
            with open("csv/%s_%d.csv" % (prog, i), "r") as f:
                for row in csv.reader(f):
                    if not row:
                        break
                    raw[prog][row[0]].append(float(row[1]))

    titles = {
        "SET": "SET",
        "GET": "GET",
        "INCR": "INCR",
        "LPUSH": "LPUSH",
        "RPUSH": "RPUSH",
        "LPOP": "LPOP",
        "RPOP": "RPOP",
        "SADD": "SADD",
        "HSET": "HSET",
        "SPOP": "SPOP",
        "LRANGE_100 (first 100 elements)": "LRANGE (100)",
        "LRANGE_300 (first 300 elements)": "LRANGE (300)",
        "LRANGE_500 (first 450 elements)": "LRANGE (500)",
        "LRANGE_600 (first 600 elements)": "LRANGE (600)",
        "MSET (10 keys)": "MSET",
    }

    avg = {
        "pydis": dict((k, average(raw["pydis"][k])) for k in titles.keys()),
        "redis": dict((k, average(raw["redis"][k])) for k in titles.keys()),
    }

    stddev = {
        "pydis": dict((k, standard_deviation(raw["pydis"][k])) for k in titles.keys()),
        "redis": dict((k, standard_deviation(raw["redis"][k])) for k in titles.keys()),
    }

    print("Benchmark | pydis | redis | Ratio")
    print("--- | ---: | ---: | ---")
    ratios = []
    for bench, title in titles.items():
        p = avg["pydis"][bench]
        r = avg["redis"][bench]
        ratios.append(p / r)
        print("{} | {:,} | {:,} | {:.3f}".format(title, int(p), int(r), p / r))
    print("AVG Ratio: {:3f}".format(average(ratios)))


    width = 0.35  # the width of the bars
    ind = range(len(titles))  # the x locations for the groups

    fig, ax = plt.subplots()
    pydis_rects = ax.bar([xi - width / 2 for xi in ind], avg["pydis"].values(), width, yerr=stddev["pydis"].values(),
                    color="SkyBlue", label="pydis")
    redis_rects = ax.bar([xi + width / 2 for xi in ind], avg["redis"].values(), width, yerr=stddev["redis"].values(),
                    color="IndianRed", label="redis")

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel("Requests per second")
    ax.set_xlabel("Benchmarks")
    ax.set_title("Requests per second by programs and benchmarks")
    ax.set_xticks(ind)
    ax.set_xticklabels(titles.values())
    plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment="right")
    ax.legend()

    autolabel(ax, pydis_rects, "left")
    autolabel(ax, redis_rects, "right")

    plt.show()

    fig.savefig('plot.svg', format='svg', dpi=1200)


def average(xs):
    return sum(xs) / len(xs)


def standard_deviation(xs):
    avg = average(xs)
    return math.sqrt(sum((x - avg)**2 for x in xs) / len(xs))


def autolabel(ax, rects, xpos="center"):
    xpos = xpos.lower()  # normalize the case of the parameter
    ha = {"center": "center", "right": "left", "left": "right"}
    offset = {"center": 0.5, "right": 0.57, "left": 0.43}  # x_txt = x + w*off

    for rect in rects:
        height = rect.get_height()
        if height / 10**6 < 0.05:
            continue
        ax.text(rect.get_x() + rect.get_width() * offset[xpos], 1.01 * height,
                "{:.1f}".format(height / 10**6), ha=ha[xpos], va="bottom")


if __name__ == "__main__":
    main()
