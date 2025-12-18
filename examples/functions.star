print("=== Functions Example ===")

def factorial(n):
    if n <= 1:
        return 1
    return n * factorial(n - 1)

def fibonacci(n):
    if n <= 1:
        return n
    a, b = 0, 1
    for _ in range(n - 1):
        a, b = b, a + b
    return b

print("factorial(5) =", factorial(5))
print("factorial(10) =", factorial(10))

print("fibonacci(10) =", fibonacci(10))
print("fibonacci(20) =", fibonacci(20))

double = lambda x: x * 2
square = lambda x: x * x

print("double(7) =", double(7))
print("square(8) =", square(8))

def make_adder(n):
    return lambda x: x + n

add_10 = make_adder(10)
print("add_10(5) =", add_10(5))
