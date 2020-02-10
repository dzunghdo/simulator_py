import time


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
        # self.disk = disk
        # self.dirty = dirty
        # self.active = active
        # self.inactive = inactive


class Block:
    """Data block of a File"""

    def __init__(self, filename, size=0, dirty=False, accessed_time=0.0):
        self.filename = filename
        self.size = size
        self.dirty = dirty
        self.accessed_time = accessed_time


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
        self.active = []
        self.inactive = []
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

    def get_data_in_cache(self, filename):
        """
        Return the amount of data cached
        :param filename:
        :return:
        """
        amount = 0
        for block in self.inactive:
            if block.filename == filename:
                amount += block.size

        for block in self.active:
            if block.filename == filename:
                amount += block.size

        return amount

    def get_available_memory(self):
        return self.free + self.cache - self.dirty

    def get_log(self):
        return self.log

    def evict(self, amount):
        # evictable = self.cache - self.dirty
        # evicted = min(evictable, amount)

        evicted = 0
        for block in self.inactive:

            if evicted >= amount:
                break
            elif evicted < amount < evicted + block.size:
                block_evicted = amount - evicted
                block.size -= block_evicted
                evicted += block_evicted
                break
            else:
                evicted += block.size
                self.inactive.remove(block)

        self.free += evicted
        self.cache -= evicted

    def flush(self, amount):
        self.dirty -= amount

    def read_from_disk(self, amount, filename=None):
        """
        Read data not cached from disk. Add new read block to inactive list.
        :param amount:
        :param filename:
        :return:
        """

        self.cache += amount
        self.free -= amount

        block = Block(filename=filename, size=amount, dirty=False, accessed_time=time.time())
        self.inactive.append(block)
        self.sort_lru_list()

    def cache_read(self, filename):
        """
        Read data from cache. All data in cache is active
        :param filename:
        :return:
        """

        read_size = 0
        for block in self.inactive[:]:
            if block.filename == filename:
                read_size += block.size
                self.inactive.remove(block)

        for block in self.active[:]:
            if block.filename == filename:
                read_size += block.size
                self.active.remove(block)

        # Update all accessed data as active
        active_block = Block(filename, read_size, dirty=False, accessed_time=time.time())
        self.active.append(active_block)

        self.sort_lru_list()

    def write(self, filename, amount, max_cache, new_dirty):
        """
        Write a file to disk through cache. The written file is not in cache. Thus, all data is in inactive list
        :param filename: filename
        :param amount: total amount of data to write
        :param max_cache: maximum cache
        :param new_dirty: amount of dirty data
        :return:
        """

        if self.cache + amount > max_cache:
            self.cache = max_cache
            self.free = 0
        else:
            self.cache += amount
            self.free -= amount

        self.dirty += new_dirty

        block = Block(filename, amount, dirty=False, accessed_time=time.time())
        self.inactive.append(block)
        self.sort_lru_list()

    def sort_lru_list(self):
        self.inactive = sorted(self.inactive, key=lambda block: block.accessed_time)
        self.active = sorted(self.active, key=lambda block: block.accessed_time)

    def balance_lru_lists(self):
        # move old data from active to inactive list
        pass

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

    def print_cached_files(self):
        print("\nInactive:")
        total_inactive = 0
        for block in self.inactive:
            total_inactive += block.size
            print("%s, %d MB, %f" % (block.filename, block.size, block.accessed_time))
        print("Total: %d MB" % total_inactive)

        total_active = 0
        print("\nActive:")
        for block in self.active:
            total_inactive += block.size
            print("%s, %d MB, %f" % (block.filename, block.size, block.accessed_time))
        print("Total: %d MB\n" % total_active)


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

        cached_amt = self.memory.get_data_in_cache(file.name)
        from_disk = file.size - cached_amt

        # ============= FLUSHING - EVICTION ==========
        # Memory required to accommodate file in cache and application used memory
        mem_required = max(0, 2 * file.size - cached_amt - self.memory.get_available_memory())
        # Flush if not enough available memory
        # This takes time to write dirty data to memory
        if mem_required > 0:
            run_time += self.flush(mem_required)

        # if free memory is not enough after flushing, then evict old pages
        # This doesn't take time
        mem_required = max(0, 2 * file.size - cached_amt - self.memory.free)
        if mem_required > 0:
            # run_time += self.evict(mem_required)
            self.evict(mem_required)

        # =========== START READING ===========
        # if part of file is cached, access pages in cache again to update LRU lists
        if cached_amt > 0:
            self.memory.cache_read(file.name)

        # Read from disk: if a part of file is read from disk, memory read is neglected.
        if from_disk > 0:

            # read to cache
            self.memory.read_from_disk(from_disk, file.name)

            # time to read from disk
            disk_read_time = from_disk / self.storage.read_bw
            run_time += disk_read_time

            ## time for periodical flushing is taken into account
            # flushing_time = self.period_flush(disk_read_time)
            # run_time += flushing_time

            print("\tRead %d MB from disk in %.2f sec" % (from_disk, disk_read_time))

        else:
            mem_read_time = cached_amt / self.memory.read_bw
            run_time += mem_read_time

            # Periodical flushing doesn't take time during reading from cache
            self.period_flush(mem_read_time)

            print("\tRead %d MB from cache in %.2f sec" % (cached_amt, mem_read_time))

        # mem used by application
        self.memory.free -= file.size

        print("%.2f File %s is read" % (run_time, file.name))
        self.memory.add_log(run_time)

        return run_time

    def write(self, file, run_time=0):
        print("%.2f Start writing %s " % (run_time, file.name))
        self.memory.add_log(run_time)

        max_cache = self.memory.size - file.size
        cached_amt = self.memory.get_data_in_cache(file.name)

        # check if more cache is required for writing file
        cache_required = max(0, file.size - cached_amt - self.memory.get_available_memory())
        if cache_required > 0:
            self.memory.evict(cache_required)

        # ============= FREE WRITE ===============

        dirty_thres = self.dirty_ratio * self.memory.get_available_memory()
        free_amt = min(self.memory.free, file.size, dirty_thres - self.memory.dirty)

        if free_amt > 0:
            # write to cache with memory bw
            cached_amt += free_amt
            self.memory.write(file.name, amount=free_amt, max_cache=max_cache, new_dirty=free_amt)
            free_write_time = free_amt / self.memory.write_bw
            run_time += free_write_time

            self.memory.add_log(run_time)
            print("\tFreely write %d MB in %.2f sec" % (free_amt, free_write_time))

        if cached_amt == file.size:
            return run_time

        # ============= WRITE WITH DISK BW =============

        # Write dirty data before threshold is reached
        dirty_amt = max(0, self.get_dirty_threshold() - self.memory.dirty)
        throttled_amt = file.size - free_amt

        self.memory.write(file.name, amount=throttled_amt, max_cache=max_cache, new_dirty=dirty_amt)

        throttled_write_time = throttled_amt / self.storage.write_bw
        run_time += throttled_write_time

        self.memory.add_log(run_time)

        print("\tThrottled write %d MB(%d MB dirty) in %.2f sec" % (throttled_amt, dirty_amt, throttled_write_time))
        print("%.2f File %s is written " % (run_time, file.name))

        return run_time

    def flush(self, amount):
        self.memory.flush(amount)
        return amount / self.storage.write_bw

    def evict(self, amount):
        self.memory.evict(amount)
        return 0

    def period_flush(self, duration):
        amount_flushed = min(self.memory.dirty, self.storage.write_bw * duration)
        flushing_time = amount_flushed / self.storage.write_bw
        self.memory.flush(amount_flushed)
        return flushing_time

    def period_evict(self):
        return 0

    def release(self, file):
        self.memory.free += file.size

    def compute(self, start_time, cpu_time=0):
        self.period_flush(cpu_time)
        return start_time + cpu_time

    def get_dirty_threshold(self):
        return self.memory.get_available_memory() * self.dirty_ratio
