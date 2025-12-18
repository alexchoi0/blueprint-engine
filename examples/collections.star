print("=== Collections Example ===")

numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
print("numbers:", numbers)
print("sum:", sum(numbers))
print("min:", min(numbers))
print("max:", max(numbers))
print("len:", len(numbers))

evens = [x for x in numbers if x % 2 == 0]
print("evens:", evens)

squares = [x * x for x in range(1, 6)]
print("squares:", squares)

doubled = [x * 2 for x in numbers]
print("doubled:", doubled)

person = {
    "name": "Alice",
    "age": 30,
    "city": "New York"
}
print("person:", person)
print("name:", person["name"])

person["email"] = "alice@example.com"
print("updated:", person)

coords = (10, 20, 30)
x, y, z = coords
print("coords:", coords)
print("x =", x, "y =", y, "z =", z)

matrix = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
print("matrix:", matrix)
print("matrix[1][1] =", matrix[1][1])

flat = [cell for row in matrix for cell in row]
print("flattened:", flat)
