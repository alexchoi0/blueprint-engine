print("=== Time Example ===")

start = now()
print("start time:", start)

sleep(0.1)

end = now()
print("end time:", end)
print("elapsed:", end - start, "seconds")

def measure(fn):
    t1 = now()
    result = fn()
    t2 = now()
    return (result, t2 - t1)

def slow_sum():
    total = 0
    for i in range(10000):
        total = total + i
    return total

result, elapsed = measure(slow_sum)
print("sum of 0..9999 =", result)
print("took", elapsed, "seconds")
