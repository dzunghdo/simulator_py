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
    def __init__(self, size=0, free=0, read_bw=0, write_bw=0):
        """
        LRU list: list of tuples, the first value is filename, the 2nd value is timestamp, the 3rd value amount of data

        :param size: total memory in MB
        :param free: free memory in MB
        :param read_bw: read bandwidth in MBps
        :param write_bw: write bandwidth in MBps
        """
        self.size = size
        self.free = free
        self.read_bw = read_bw
        self.write_bw = write_bw
        self.dirty_ratio = 20
        self.dirty_bg_ratio = 10
        self.active_list = []
        self.inactive_list = []

    def writeback(self, disk):
        # Write file to storage with disk bandwidth
        # reduce dirty data

        pass

    def evict(self, amount):
        # remove oldest data from inactive list
        pass

    def balance_lru_lists(self):
        # move old data from active to inactive list
        pass

    def data_in_cache(self, filename):
        active = [item[2] for item in self.active_list if item[0] == filename]
        inactive = [item[2] for item in self.inactive_list if item[0] == filename]
        return active, inactive


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


memory = Memory(14.5 * pow(2, 10), 14.5 * pow(2, 10), 6000, 3200)
storage = Storage(200 * pow(2, 10))


def read(file):
    run_time = 0

    # Read until free memory is up
    while memory.free > 0:
        # if data is in cache:
        if file.cache == file.size:
            # Move data to active list
            # Balance active and inactive list
            return file.size / memory.read_bw

        if file.cache < file.size:
            run_time += (file.size - file.cache) / storage.read_bw

    # check if data is in inactive list
    # update active/inactive list at the same time
    # Then read and evict inactive list at the same time
    # balance active and inactive list

    # if file.size == file.cache:
    #     file.cache = file.size
    #     return file.size / memory.read_bw
    #
    # if 0 < file.cache < file.size:
    #     file.cache = file.size
    #     return file.cache / memory.read_bw + (file.size - file.cache) / storage.read_bw
    #
    # if file.cache == 0:
    #     file.cache = file.size
    #     return file.size / storage.read_bw
    pass


def write(file):
    # write with memory bandwidth until dirty_ratio is reached, update active/inactive list
    # write with disk bandwidth, evict inactive data, add data to inactive list

    # file.cache = file.size
    # return file.size / storage.write_bw
    pass


file1 = File("file1", 6010, 6010)
file2 = File("file2", 6010, 6010)
file3 = File("file3", 6010, 6010)
file4 = File("file4", 6010, 6010)

task1_read = read(file1)
task1_write = write(file2)
task2_read = read(file2)
task2_write = write(file3)
task3_read = read(file3)
task3_write = write(file4)
