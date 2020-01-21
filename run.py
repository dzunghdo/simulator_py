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
        # TODO: Write file to storage with disk bandwidth
        # TODO: reduce dirty data

        pass

    def evict(self, amount):
        # TODO: remove oldest data in inactive list
        pass

    def balance_lru_lists(self):
        # TODO: move older data from active to inactive list
        pass


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
    # TODO: get system info
        # TODO: amount of data is in cache (active and inactive)
        # TODO: amount of cache used
        # TODO: amount of free memory

    # TODO: Read until cache is full and update active/inactive list at the same time
    # TODO: Then read and evict inactive list at the same time
    # TODO: balance active and inactive list

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
    # TODO: get system info
        # TODO: amount of data is in cache (active and inactive)
        # TODO: amount of cache used
        # TODO: amount of free memory

    # TODO: write with memory bandwidth until dirty_ratio is reached, update active/inactive list
    # TODO: write with disk bandwidth, evict inactive data, add data to inactive list

    # file.cache = file.size
    # return file.size / storage.write_bw
    pass


file1 = File("file1", 6010, 6010)
