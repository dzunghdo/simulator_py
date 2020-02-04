class File:
    """
    File class.
    """

    def __init__(self, name, size=0, disk=0, cache=0, dirty=0, active=0, inactive=0):
        """

        :param name: filename
        :param size: size in MB
        :param disk: data on disk in MB
        :param cache: data in cache in MB
        :param dirty: dirty data in MB
        :param active: data in active list in MB
        :param inactive: data in inactive list in MB
        """
        self.name = name
        self.size = size
        self.disk = disk
        self.cache = cache
        self.dirty = dirty
        self.active = active
        self.inactive = inactive


class Memory:
    def __init__(self, size=0, free=0, cache=0, dirty=0, read_bw=0, write_bw=0):
        """
        LRU list: list of tuples, the first value is filename, the 2nd value is timestamp, the 3rd value amount of data

        :param size: total memory in MB
        :param free: free memory in MB
        :param cache: cache used in MB
        :param dirty: dirty data in MB
        :param read_bw: read bandwidth in MBps
        :param write_bw: write bandwidth in MBps
        """
        self.size = size
        self.free = free
        self.cache = cache
        self.dirty = dirty
        self.read_bw = read_bw
        self.write_bw = write_bw
        # self.active_list = []
        # self.inactive_list = []
        self.log = {
            "total": [self.size],
            "free": [self.free],
            "cache": [self.cache],
            "dirty": [self.dirty],
            "used": [self.size - self.free],
            "time": [0]
        }

    def get_available_memory(self):
        return self.free + self.cache - self.dirty

    def get_log(self):
        return self.log

    def evict(self, amount):
        evictable = self.cache - self.dirty
        evicted = min(evictable, amount)
        self.free += evicted
        self.cache -= evicted

    def flush(self, amount):
        self.dirty -= amount

    def read(self, amount):
        self.cache += amount
        self.free -= amount

    def write(self, amount, max_cache, new_dirty):
        if self.cache + amount > max_cache:
            self.cache = max_cache
        else:
            self.cache += amount
        self.dirty += new_dirty
        self.free -= amount

    def balance_lru_lists(self):
        # move old data from active to inactive list
        pass

    # def data_in_cache(self, filename):
    # active = [item[2] for item in self.active_list if item[0] == filename]
    # inactive = [item[2] for item in self.inactive_list if item[0] == filename]
    # return active, inactive

    def print(self):
        print("Memory status:")
        print("\t Total: %.2f" % self.size)
        print("\t Free: %.2f" % self.free)
        print("\t Cache: %.2f" % self.cache)
        print("\t Dirty: %.2f" % self.dirty)

    def add_log(self, time):
        self.log["time"].append(time)
        self.log["total"].append(self.size)
        self.log["free"].append(self.free)
        self.log["used"].append(self.size - self.free)
        self.log["cache"].append(self.cache)
        self.log["dirty"].append(self.dirty)


class Storage:
    def __init__(self, size=0, read_bw=0, write_bw=0):
        """

        :param size: total capacity in MB
        :param read_bw: read bandwidth in MBps
        :param write_bw: write bandwidth in MBps
        """
        self.size = size
        self.read_bw = read_bw
        self.write_bw = write_bw


class Kernel:
    def __init__(self, memory_, storage_):
        self.memory = memory_
        self.storage = storage_
        self.dirty_ratio = 0.2
        self.dirty_bg_ratio = 0.1

    def read(self, file, run_time=0):
        print("%.2f Start reading %s" % (run_time, file.name))
        self.memory.add_log(run_time)
        # periodical flushing
        # periodical eviction

        mem_required = max(0, 2 * file.size - file.cache - self.memory.get_available_memory())
        # Flush if not enough available memory
        if mem_required > 0:
            run_time += self.flush(mem_required)

        # if free memory is not enough after flushing
        mem_required = max(0, 2 * file.size - file.cache - self.memory.free)
        if mem_required > 0:
            run_time += self.evict(mem_required)

        # amount of data read from disk
        from_disk = file.size - file.cache

        # Read from disk
        if from_disk > 0:

            # read to cache
            self.memory.read(from_disk)

            # update file status
            file.disk -= from_disk
            file.cache += from_disk

            # time to read from disk
            run_time += from_disk / self.storage.read_bw
            print("%.2f Read %d MB from disk" % (run_time, from_disk))

        else:
            run_time += file.cache / self.memory.read_bw
            print("%.2f Read %d MB from cache" % (run_time, file.cache))

        # mem used by application
        self.memory.free -= file.size

        self.memory.add_log(run_time)

        return run_time

    def write(self, file, run_time=0):
        print("%.2f Start writing %s " % (run_time, file.name))
        self.memory.add_log(run_time)

        # periodical flushing
        # periodical eviction

        max_cache = self.memory.size - file.size

        cache_required = max(0, file.size - file.cache - self.memory.get_available_memory())
        if cache_required > 0:
            self.memory.evict(cache_required)

        # free write amount
        dirty_thres = self.dirty_ratio * self.memory.get_available_memory()
        free_amt = min(self.memory.free, file.size, dirty_thres - self.memory.dirty)

        if free_amt > 0:
            # write to cache with memory bw
            file.dirty = free_amt
            file.cache += free_amt
            self.memory.write(amount=free_amt, max_cache=max_cache, new_dirty=free_amt)
            run_time += free_amt / self.memory.write_bw
            self.memory.add_log(run_time)
            print("%.2f Freely write %d MB " % (run_time, free_amt))

        if file.cache == file.size:
            return run_time

        # throttled write before memory is used up
        throttled_amt = min(file.size - free_amt, self.memory.get_available_memory())

        # evict data if there is not enough free mem
        mem_required = max(throttled_amt - self.memory.free, 0)
        if mem_required > 0:
            self.memory.evict(mem_required)

        # write data with enough free memory and throttled bw
        written_amt = min(self.memory.free, throttled_amt)
        self.memory.write(amount=written_amt, max_cache=max_cache, new_dirty=0)
        file.cache += written_amt
        run_time += written_amt / self.storage.write_bw
        self.memory.add_log(run_time)
        print("%.2f Throttled write %d MB " % (run_time, written_amt))

        return run_time

    def flush(self, amount):
        self.memory.flush(amount)
        return amount / self.storage.write_bw

    def evict(self, amount):
        self.memory.evict(amount)
        return 0

    def release(self, file):
        self.memory.free += file.cache

    def compute(self):
        return 0
