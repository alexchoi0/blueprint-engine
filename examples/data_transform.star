load("@bp/io", "write_file")
load("@bp/json", "json_encode")

users = [
    {"name": "Alice", "age": 30, "role": "admin"},
    {"name": "Bob", "age": 25, "role": "user"},
    {"name": "Charlie", "age": 35, "role": "user"},
]

admins = [u for u in users if u["role"] == "admin"]
names = [u["name"] for u in users]
ages = {u["name"]: u["age"] for u in users}

def make_greeting(user):
    return "Hello, %s! You are %d years old." % (user["name"], user["age"])

greetings = [make_greeting(u) for u in users]

output = {
    "total_users": len(users),
    "admin_count": len(admins),
    "names": names,
    "ages": ages,
    "greetings": greetings,
}

write_file("/tmp/transform_output.json", json_encode(output))
