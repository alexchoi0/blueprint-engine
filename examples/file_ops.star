print("Testing file operations...")

content = read_file("examples/hello.star")
print("Read file length:", len(content))

write_file("/tmp/blueprint_test.txt", "Hello from Blueprint3!")
print("Wrote test file")

content = read_file("/tmp/blueprint_test.txt")
print("Read back:", content)

if exists("/tmp/blueprint_test.txt"):
    print("File exists!")
else:
    print("File does not exist!")

rm("/tmp/blueprint_test.txt")
print("Cleaned up test file")
