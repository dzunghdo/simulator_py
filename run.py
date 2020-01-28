# i/o simulator in python


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

    def get_available_memory(self):
        return self.free + self.cache - self.dirty

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

    def write(self, amount, dirty):
        self.cache += amount
        self.dirty += dirty
        self.free -= amount

    def balance_lru_lists(self):
        # move old data from active to inactive list
        pass

    # def data_in_cache(self, filename):
    # active = [item[2] for item in self.active_list if item[0] == filename]
    # inactive = [item[2] for item in self.inactive_list if item[0] == filename]
    # return active, inactive

    def log(self):
        print("Memory status:")
        print("\t Total: %.2f" % self.size)
        print("\t Free: %.2f" % self.free)
        print("\t Cache: %.2f" % self.cache)
        print("\t Dirty: %.2f" % self.dirty)


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

    def read(self, file):
        run_time = 0

        # periodical flushing
        # periodical eviction

        mem_required = max(0, 2 * file.size - file.cache - memory.get_available_memory())
        # Flush if not enough memory
        if mem_required > 0:
            run_time += self.flush(mem_required)

        # if memory is not enough after flushing
        mem_required = max(0, 2 * file.size - file.cache - memory.free)
        if mem_required > 0:
            run_time += self.evict(mem_required)

        # amount of data read from disk
        from_disk = file.size - file.cache
        # Read from disk
        if from_disk > 0:
            # read to cache
            memory.read(from_disk)
            # update file status
            file.disk -= from_disk
            file.cache += from_disk
            # time to read from disk
            run_time += from_disk / storage.read_bw
        else:
            run_time += file.cache / memory.read_bw

        # memory used by app
        memory.free -= file.size

        return run_time

    def write(self, file):
        run_time = 0

        # periodical flushing
        # periodical eviction

        cache_required = max(0, file.size - file.cache - memory.get_available_memory())
        if cache_required > 0:
            memory.evict(cache_required)

        # free write amount
        dirty_thres = self.dirty_ratio * memory.get_available_memory()
        free_amt = min(memory.free, file.size, dirty_thres - memory.dirty)

        if free_amt > 0:
            file.dirty = free_amt
            file.cache += free_amt
            memory.write(amount=free_amt, dirty=free_amt)
            run_time += free_amt / memory.write_bw

        if file.cache == file.size:
            return run_time

        # throttled write before memory is used up
        throttled_amt = min(file.size - free_amt, memory.get_available_memory())

        # evict data if there is not enough free mem
        mem_required = max(throttled_amt - memory.free, 0)
        if mem_required > 0:
            memory.evict(mem_required)

        # write data with enough free memory and throttled bw
        written_amt = min(memory.free, throttled_amt)
        memory.write(amount=written_amt, dirty=0)
        file.cache += written_amt
        run_time += written_amt / storage.write_bw

        return run_time

    def flush(self, amount):
        self.memory.flush(amount)
        return amount / self.storage.write_bw

    def evict(self, amount):
        self.memory.evict(amount)
        return 0

    def compute(self):
        return 0


memory = Memory(14.5 * pow(2, 10), 14.5 * pow(2, 10), read_bw=6000, write_bw=3200)
storage = Storage(200 * pow(2, 10), read_bw=250, write_bw=160)
kernel = Kernel(memory, storage)

file1 = File("file1", 6010, 6010)
file2 = File("file2", 6010, 6010)
file3 = File("file3", 6010, 6010)
file4 = File("file4", 6010, 6010)

task1_read = kernel.read(file1)
print("Task1 read: %.2f" % task1_read)
memory.log()
task1_write = kernel.write(file2)
print("Task2 write: %.2f" % task1_write)
memory.log()
task2_read = kernel.read(file2)
print("Task2 read: %.2f" % task2_read)
memory.log()
# task2_write = kernel.write(file3)
# print("Task2 write: %.2f" % task2_write)
# memory.log()
# task3_read = kernel.read(file3)
# task3_write = kernel.write(file4)

# print("Task2 read: %.2f" % task2_read)
# memory.log()
