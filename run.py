# i/o simulator in python


class File:
    """
    File class.
    """
    def __init__(self, size=0, disk=0, cache=0, dirty=0):
        """

        :param size: size in MB
        :param disk: data on disk in MB
        :param cache: data in cache in MB
        :param dirty: dirty data in MB
        """
        self.size = size
        self.disk = disk
        self.cache = cache
        self.dirty = dirty


class Memory:
    def __init__(self, size=0, free=0, rb=0, wb=0):
        """

        :param size: total memory in MB
        :param free: free memory in MB
        :param rb: read bandwidth in MBps
        :param wb: write bandwidth in MBps
        """
        self.size = size
        self.free = free
        self.rb = rb
        self.wb = wb


class Disk:
    def __init__(self, size=0, rb=0, wb=0):
        """

        :param size: total capacity in MB
        :param rb: read bandwidth in MBps
        :param wb: write bandwidth in MBps
        """
        self.size = size
        self.rb = rb
        self.wb = wb


disk = Disk(200 * pow(2, 10))

# def write(file)