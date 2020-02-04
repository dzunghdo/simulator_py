import matplotlib.pyplot as plt

def plot_mem_log(mem_log, task_time):
    time = mem_log["time"]
    total = mem_log["total"]
    free = mem_log["free"]
    used = mem_log["used"]
    cache = mem_log["cache"]
    dirty = mem_log["dirty"]

    start_time, task1_read_end, task1_write_end, task2_read_end, \
        task2_write_end, task3_read_end, task3_write_end = task_time

    plt.figure()

    plt.axvspan(xmin=start_time, xmax=task1_read_end, color="k", alpha=0.2, label="read")
    plt.axvspan(xmin=task1_read_end, xmax=task1_write_end, color="k", alpha=0.4, label="write")
    plt.axvspan(xmin=task1_write_end, xmax=task2_read_end, color="k", alpha=0.2)
    plt.axvspan(xmin=task2_read_end, xmax=task2_write_end, color="k", alpha=0.4)
    plt.axvspan(xmin=task2_write_end, xmax=task3_read_end, color="k", alpha=0.2)
    plt.axvspan(xmin=task3_read_end, xmax=task3_write_end, color="k", alpha=0.4)

    plt.plot(time, total, color='k', linewidth=1, linestyle="-.", label="total mem")
    plt.plot(time, used, color='g', linewidth=1, label="used mem")
    plt.plot(time, cache, color='m', linewidth=1, label="cache")
    plt.plot(time, dirty, color='r', linewidth=1, label="dirty")

    plt.legend()

    plt.show()
