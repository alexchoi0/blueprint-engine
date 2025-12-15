"""Parallel I/O example - demonstrates concurrent file operations.

The interpreter automatically runs independent operations in parallel.
No explicit parallel primitives needed!
"""

load("@bp/io", "read_file", "write_file")

write_file("/tmp/bp_test1.txt", "Hello from file 1")
write_file("/tmp/bp_test2.txt", "Hello from file 2")

content1 = read_file("/tmp/bp_test1.txt")
content2 = read_file("/tmp/bp_test2.txt")

print("File 1:", content1)
print("File 2:", content2)
print("All done!")
