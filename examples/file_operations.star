load("@bp/io", "mkdir", "write_file")

mkdir("/tmp/blueprint_test", recursive=True)

write_file("/tmp/blueprint_test/one.txt", "File 1: one.txt")
write_file("/tmp/blueprint_test/two.txt", "File 2: two.txt")
write_file("/tmp/blueprint_test/three.txt", "File 3: three.txt")

print("Created 3 files in /tmp/blueprint_test")
