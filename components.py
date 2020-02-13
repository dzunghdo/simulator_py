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

    def get_evictable_memory(self):
        return sum([block.size for block in self.inactive if not block.dirty])

    def get_log(self):
        return self.log

    def evict(self, amount):

        evicted = 0
        for block in self.inactive:

            if block.dirty:
                continue

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
        self.update_lru_lists()

    def cache_read(self, filename):
        """
        Read data from cache. All data in cache is active
        :param filename:
        :return:
        """

        dirty = 0
        not_dirty = 0
        for block in self.inactive[:]:
            if block.filename == filename:
                if block.dirty:
                    dirty += block.size
                else:
                    not_dirty += block.size
                self.inactive.remove(block)

        for block in self.active[:]:
            if block.filename == filename:
                if block.dirty:
                    dirty += block.size
                else:
                    not_dirty += block.size
                self.active.remove(block)

        # Update all accessed data as active
        dirty_block = Block(filename, dirty, dirty=True, accessed_time=time.time())
        not_dirty_block = Block(filename, not_dirty, dirty=False, accessed_time=time.time())
        self.active.append(dirty_block)
        self.active.append(not_dirty_block)

        self.update_lru_lists()

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

        if new_dirty > 0:
            self.dirty += new_dirty
            self.inactive.append(Block(filename, new_dirty, dirty=True, accessed_time=time.time()))

        if amount - new_dirty > 0:
            self.inactive.append(Block(filename, amount - new_dirty, dirty=False, accessed_time=time.time()))

        self.update_lru_lists()

    def update_lru_lists(self):
        self.inactive = sorted(self.inactive, key=lambda block: block.accessed_time)
        self.active = sorted(self.active, key=lambda block: block.accessed_time)

        inactive_size = sum([block.size for block in self.inactive])
        active_size = sum([block.size for block in self.active])

        # move old data from active to inactive list
        if active_size >= 2 * inactive_size:
            avg = (active_size + inactive_size) / 2
            for block in self.active[:]:
                if active_size - block.size < avg:
                    block.size -= active_size - avg
                    new_block = Block(block.filename, active_size - avg, dirty=block.dirty,
                                      accessed_time=block.accessed_time)
                    self.inactive.append(new_block)
                    break
                else:
                    self.inactive.append(block)
                    self.active.remove(block)

        self.inactive = sorted(self.inactive, key=lambda block: block.accessed_time)

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

    def print_cached_dirty(self):
        print("\nInactive:")
        total_inactive = 0
        for block in self.inactive:
            total_inactive += block.size
            print("%s, %d MB, dirty=%r, %f" % (block.filename, block.size, block.dirty, block.accessed_time))
        print("Total: %d MB" % total_inactive)

        total_active = 0
        print("\nActive:")
        for block in self.active:
            total_active += block.size
            print("%s, %d MB, dirty=%r, %f" % (block.filename, block.size, block.dirty, block.accessed_time))
        print("Total: %d MB\n" % total_active)

    def print_file_total_cached(self):
        inactive = {}
        active = {}

        for block in self.inactive:
            if block.filename in inactive.keys():
                inactive[block.filename] += block.size
            else:
                inactive[block.filename] = block.size

        for block in self.active:
            if block.filename in active.keys():
                active[block.filename] += block.size
            else:
                active[block.filename] = block.size

        print("\nInactive:")
        print(inactive)
        print("Active:")
        print(active)
        print("\n")


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

        # # check if more cache is required for writing file
        # cache_required = max(0, file.size - cached_amt - self.memory.get_available_memory())
        # if cache_required > 0:
        #     self.memory.evict(cache_required)

        # ============= FREE WRITE ===============
        # Write data before dirty_data is reached
        dirty_thres = self.dirty_ratio * self.memory.get_available_memory()
        dirty_amt = min(file.size, dirty_thres - self.memory.dirty)

        if dirty_amt > 0:
            # write to cache with memory bw
            required = max((dirty_amt - self.memory.free, 0))
            self.evict(required)
            self.memory.write(file.name, amount=dirty_amt, max_cache=max_cache, new_dirty=dirty_amt)
            free_write_time = dirty_amt / self.memory.write_bw
            run_time += free_write_time

            self.memory.add_log(run_time)
            print("\tFreely write %d MB in %.2f sec" % (dirty_amt, free_write_time))

        if dirty_amt == file.size:
            return run_time

        # ============= WRITE WITH DISK BW =============
        # Write dirty data after dirty_ratio is reached
        throttled_amt = file.size - dirty_amt

        cache_required = max(0, throttled_amt - self.memory.free)
        evict_amt = min(cache_required, self.memory.get_evictable_memory())

        to_cache_amt = min(evict_amt + self.memory.free, throttled_amt)
        to_disk_amt = throttled_amt - to_cache_amt

        if cache_required > 0:
            self.memory.evict(evict_amt)

        cache_write_time = to_cache_amt / self.storage.write_bw
        disk_write_time = to_disk_amt / self.storage.write_bw
        run_time += cache_write_time + disk_write_time
        self.memory.write(file.name, amount=to_cache_amt, max_cache=max_cache, new_dirty=0)

        self.memory.add_log(run_time)

        print("\tThrottled write %d to cache MB(%d MB dirty) in %.2f sec" % (to_cache_amt, dirty_amt, cache_write_time))
        print("\tThrottled write %d to disk in %.2f sec" % (to_disk_amt, disk_write_time))
        print("%.2f File %s is written " % (run_time, file.name))

        return run_time

    def flush(self, amount):
        self.memory.flush(amount)
        return amount / self.storage.write_bw

    def evict(self, amount):
        self.memory.evict(amount)
        return 0

    def period_flush(self, duration):

        self.memory.inactive.reverse()

        max_flushed = min(self.memory.dirty, self.storage.write_bw * duration)

        flushed = 0

        for block in self.memory.inactive:
            if block.dirty:
                if flushed + block.size <= max_flushed:
                    block.dirty = False
                    self.memory.flush(block.size)
                elif flushed < max_flushed < flushed + block.size:
                    blk_flushed = max_flushed - flushed
                    block.size -= blk_flushed
                    self.memory.flush(blk_flushed)
                    new_block = Block(block.filename, blk_flushed, dirty=False, accessed_time=block.accessed_time)
                    self.memory.inactive.append(new_block)
                else:
                    break

        self.memory.inactive = sorted(self.memory.inactive, key=lambda item: item.accessed_time)

        return duration

    def period_evict(self):
        return 0

    def release(self, file):
        self.memory.free += file.size

    def compute(self, start_time, cpu_time=0):
        self.period_flush(cpu_time)
        return start_time + cpu_time

    def get_dirty_threshold(self):
        return self.memory.get_available_memory() * self.dirty_ratio
