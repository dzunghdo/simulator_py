# i/o simulator in python
import plot
from components import Kernel
from components import File
from components import Storage
from components import Memory

memory = Memory(14.5 * pow(2, 10), 14800, read_bw=2680, write_bw=1100)
storage = Storage(200 * pow(2, 10), read_bw=110, write_bw=110)
kernel = Kernel(memory, storage)

input_size = 6000
compute_time = 13

file1 = File("file1", input_size, input_size)
file2 = File("file2", input_size, input_size)
file3 = File("file3", input_size, input_size)
file4 = File("file4", input_size, input_size)

start_time = 0

task1_read_end = kernel.read(file1, start_time)
task1_compute_end = kernel.compute(task1_read_end, compute_time)
task1_write_end = kernel.write(file2, task1_compute_end)
kernel.release(file2)

task2_read_end = kernel.read(file2, task1_write_end)
task2_compute_end = kernel.compute(task2_read_end, compute_time)
task2_write_end = kernel.write(file3, task2_compute_end)
kernel.release(file3)
# kernel.memory.print_cached_dirty()
kernel.memory.print_file_total_cached()

task3_read_end = kernel.read(file3, task2_write_end)
task3_compute_end = kernel.compute(task3_read_end, compute_time)
task3_write_end = kernel.write(file4, task3_compute_end)
kernel.release(file4)

task_time = {
    "read_start": [start_time, task1_write_end, task2_write_end],
    "read_end": [task1_read_end, task2_read_end, task3_read_end],
    "write_start": [task1_compute_end, task2_compute_end, task3_compute_end],
    "write_end": [task1_write_end, task2_write_end, task3_write_end],

}

plot.plot_mem_log(memory.get_log(), task_time, "input = %d MB \nmem_rb = %d MBps\nmem_wb = %d MBps \n"
                                               "disk_rb = %d MBps\ndisk_wb = %d MBps"
                  % (input_size, memory.read_bw, memory.write_bw,
                     storage.read_bw, storage.write_bw),
                  xmin=0, xmax=300, ymin=-1000, ymax=16000)
