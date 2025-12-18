print("Testing process execution...")

result = run(["echo", "Hello from subprocess"])
print("stdout:", result.stdout)
print("code:", result.code)

result = shell("echo 'Shell command works!' && pwd")
print("Shell output:", result.stdout)
