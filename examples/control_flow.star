print("=== Control Flow Example ===")

x = 15
if x > 10:
    print("x is greater than 10")
elif x > 5:
    print("x is greater than 5")
else:
    print("x is 5 or less")

print("counting to 5:")
for i in range(1, 6):
    print(" ", i)

print("enumerate example:")
fruits = ["apple", "banana", "cherry"]
for i, fruit in enumerate(fruits):
    print(f"  {i}: {fruit}")

print("zip example:")
names = ["Alice", "Bob", "Charlie"]
ages = [30, 25, 35]
for name, age in zip(names, ages):
    print(f"  {name} is {age}")

print("break example:")
for i in range(10):
    if i == 5:
        print("  breaking at", i)
        break
    print(" ", i)

print("continue example:")
for i in range(5):
    if i == 2:
        continue
    print(" ", i)

result = "even" if x % 2 == 0 else "odd"
print(f"{x} is {result}")

values = [1, 2, 3, 4, 5]
has_three = 3 in values
print("has 3:", has_three)
