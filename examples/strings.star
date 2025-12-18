print("=== Strings Example ===")

s = "Hello, World!"
print("original:", s)
print("upper:", s.upper())
print("lower:", s.lower())
print("len:", len(s))

print("starts with 'Hello':", s.startswith("Hello"))
print("ends with '!':", s.endswith("!"))
print("contains 'World':", "World" in s)

words = "apple,banana,cherry,date"
parts = words.split(",")
print("split:", parts)

joined = " | ".join(parts)
print("joined:", joined)

text = "  trim me  "
print("stripped:", text.strip())

name = "Blueprint"
version = 3
msg = f"Welcome to {name} version {version}!"
print(msg)

template = "Name: {}, Age: {}"
print(template.format("Bob", 25))

print("repeat:", "ab" * 5)
print("slice:", s[0:5])
