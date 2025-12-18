print("=== JSON Example ===")

data = {
    "users": [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": "Charlie", "age": 35}
    ],
    "count": 3,
    "active": True
}

json_str = json.encode(data)
print("encoded:", json_str)

pretty = json.encode(data, indent=2)
print("pretty printed:")
print(pretty)

parsed = json.decode(json_str)
print("decoded:", parsed)
print("first user:", parsed["users"][0]["name"])

write_file("/tmp/data.json", pretty)
print("wrote to /tmp/data.json")

content = read_file("/tmp/data.json")
loaded = json.decode(content)
print("loaded back:", loaded["count"], "users")

rm("/tmp/data.json")
