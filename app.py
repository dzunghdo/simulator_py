# i/o simulator in python
import plot
from components import Kernel
from components import File
from components import Storage
from components import Memory

memory = Memory(14.5 * pow(2, 10), 14.5 * pow(2, 10), read_bw=6000, write_bw=3200)
storage = Storage(200 * pow(2, 10), read_bw=250, write_bw=160)
kernel = Kernel(memory, storage)

file1 = File("file1", 6010, 6010)
file2 = File("file2", 6010, 6010)
file3 = File("file3", 6010, 6010)
file4 = File("file4", 6010, 6010)

start_time = 0

task1_read_end = kernel.read(file1, start_time)
task1_compute_end = kernel.compute(task1_read_end, 10)
task1_write_end = kernel.write(file2, task1_compute_end)
kernel.release(file2)

task2_read_end = kernel.read(file2, task1_write_end)
task2_compute_end = kernel.compute(task2_read_end, 10)
task2_write_end = kernel.write(file3, task2_compute_end)
kernel.release(file3)

task3_read_end = kernel.read(file3, task2_write_end)
task3_compute_end = kernel.compute(task3_read_end, 10)
task3_write_end = kernel.write(file4, task3_compute_end)
# kernel.memory.print_cached_dirty()
# kernel.memory.print_file_total_cached()
kernel.release(file4)

task_time = [start_time, task1_read_end, task1_compute_end, task1_write_end,
             task2_read_end, task2_compute_end, task2_write_end,
             task3_read_end, task3_compute_end, task3_write_end]

plot.plot_mem_log(memory.get_log(), task_time)
