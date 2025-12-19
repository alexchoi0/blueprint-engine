result1 = task(lambda: 1 + 1, max_wait=5.0)
print("Result 1 (fast task):", result1)

result2 = task(lambda: sleep(0.5), max_wait=2.0)
print("Result 2 (sleep within timeout):", result2)

result3 = task(lambda: sleep(3.0), max_wait=1.0)
print("Result 3 (sleep exceeds timeout):", result3)

deadline = now() + 2.0
result4 = task(lambda: "completed", wait_until=deadline)
print("Result 4 (wait_until future deadline):", result4)

past_deadline = now() - 1.0
result5 = task(lambda: "completed", wait_until=past_deadline)
print("Result 5 (wait_until past deadline):", result5)
