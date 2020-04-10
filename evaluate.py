import csv


def read_timelog(filename, skip_header=True):
    result = []

    with open(filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        if skip_header:
            next(csv_reader)

        for line in csv_reader:
            result.append((line[0], float(line[1]), float(line[2])))

    return result


def read_atop_log(filename):
    sys_mem = []
    free_mem = []
    used_mem = []
    cache_used = []
    dirty_data = []

    f = open(filename)
    lines = f.readlines()
    for i in range(len(lines)):
        line = lines[i]
        if line.startswith("MEM"):
            values = line.split(" ")

            sys_mem_mb = int(values[7]) * 4096 / 1000 ** 2
            sys_mem.append(sys_mem_mb)

            cache_in_mb = int(values[9]) * 4096 / 1000 ** 2
            cache_used.append(cache_in_mb)

            dirty_amt_mb = int(values[12]) * 4096 / 1000 ** 2
            dirty_data.append(dirty_amt_mb)

    return {
        "total": sys_mem,
        "used_mem": used_mem,
        "cache": cache_used,
        "dirty_data": dirty_data
    }


def read_sim_log(filename):
    time = []
    sys_mem = []
    cache_used = []
    dirty_data = []

    with open(filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        next(csv_reader)

        for line in csv_reader:
            time.append(float(line[0]))
            sys_mem.append(float(line[1]))
            dirty_data.append(float(line[2]))
            cache_used.append(float(line[3]))

    return {
        "time": time,
        "total": sys_mem,
        "dirty_data": dirty_data,
        "cache": cache_used
    }


def get_atop_mem_prop(time_log, mem_log, key):
    """
    :param time_log: real time log tuple
    :param mem_log: real mem log dictionary
    :param key: key of the memory property to be returned
    :return: list of values of mem properties corresponding to task end time in log
    """

    task_ends = [task[2] for task in time_log]
    start = time_log[0][1]

    arr = mem_log[key]
    result = []
    for i in range(len(task_ends)):
        time = task_ends[i] - start
        prop = arr[int(time)] + (arr[int(time) + 1] - arr[int(time)]) * (time - int(time))
        result.append(prop)

    return result


def get_sim_mem_prop(time_log, mem_log, key):
    """
    :param time_log: simulation time log tuple
    :param mem_log: simulation mem log dictionary
    :param key: key of the memory property to be returned
    :return: list of values of mem properties corresponding to task end time in log
    """

    task_ends = [task[2] for task in time_log]
    result = []
    for i in range(len(task_ends)):
        idx = mem_log["time"].index(task_ends[i])
        if idx >= 0:
            result.append(mem_log[key][idx])
        else:
            result.append(-1000)

    return result


def time_metrics(realtime_logfile, simtime_logfile):
    sim_time = read_timelog(simtime_logfile)
    real_time = read_timelog(realtime_logfile)

    time_acc = []

    for i in range(len(sim_time)):
        time_acc.append((sim_time[i][2] - sim_time[i][1]) / (real_time[i][2] - real_time[i][1]))

    return time_acc


def mem_metrics(real_time_logfile, sim_time_logfile, real_mem_logfile, sim_mem_logfile):
    real_time_log = read_timelog(real_time_logfile)
    real_mem_log = read_atop_log(real_mem_logfile)
    dirty_origin = real_mem_log["dirty_data"][0]
    cache_origin = real_mem_log["cache"][0]
    real_dirty_amt = [amt - dirty_origin for amt in get_atop_mem_prop(real_time_log, real_mem_log, "dirty_data")]
    real_cache_amt = [amt - cache_origin for amt in get_atop_mem_prop(real_time_log, real_mem_log, "cache")]

    sim_time_log = read_timelog(sim_time_logfile)
    sim_mem_log = read_sim_log(sim_mem_logfile)
    sim_dirty_amt = get_sim_mem_prop(sim_time_log, sim_mem_log, "dirty_data")
    sim_cache_amt = get_sim_mem_prop(sim_time_log, sim_mem_log, "cache")

    dirty_acc = [sim / real for real, sim in zip(real_dirty_amt, sim_dirty_amt)]
    cache_acc = [sim / real for real, sim in zip(real_cache_amt, sim_cache_amt)]

    print(dirty_acc)
    print(cache_acc)


realtime_log = "log/6000_timestamps_pipeline.log"
simtime_log = "log/timestamps.csv"
atop_file = "log/6000_pipeline_mem_c.log"
sim_logfile = "log/simulator.csv"

accuracy = time_metrics(realtime_log, simtime_log)
print(mem_metrics(realtime_log, simtime_log, atop_file, sim_logfile))
