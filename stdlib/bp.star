"""Blueprint Standard Library.

This is the main entry point for the Blueprint standard library.
Import this module to access all Blueprint functionality.

Example:
    load("@bp", "read_file", "write_file", "http")

    config = read_file("config.json")
    response = http.get("https://api.example.com/data")
    write_file("output.json", response["body"])
"""

load("@bp/io",
    "read_file",
    "write_file",
    "append_file",
    "delete_file",
    "file_exists",
    "is_dir",
    "is_file",
    "mkdir",
    "rmdir",
    "list_dir",
    "copy_file",
    "move_file",
    "file_size",
)

load("@bp/http",
    http_get = "get",
    http_post = "post",
    http_put = "put",
    http_delete = "delete",
    http_patch = "patch",
    http_head = "head",
    http_request = "request",
)

load("@bp/json",
    json_encode = "encode",
    json_decode = "decode",
    json_load = "load_file",
    json_save = "save_file",
)

load("@bp/exec",
    "run",
    "shell",
    "env",
)

load("@bp/sync",
    "race",
    "take",
    "after",
    "pipeline",
)

load("@bp/time",
    "sleep",
    "now",
)

load("@bp/util",
    "reduce",
    "log",
)

load("@bp/net",
    "tcp",
    "udp",
    "socket",
)

io = struct(
    read_file = read_file,
    write_file = write_file,
    append_file = append_file,
    delete_file = delete_file,
    file_exists = file_exists,
    is_dir = is_dir,
    is_file = is_file,
    mkdir = mkdir,
    rmdir = rmdir,
    list_dir = list_dir,
    copy_file = copy_file,
    move_file = move_file,
    file_size = file_size,
)

http = struct(
    get = http_get,
    post = http_post,
    put = http_put,
    delete = http_delete,
    patch = http_patch,
    head = http_head,
    request = http_request,
)

json = struct(
    encode = json_encode,
    decode = json_decode,
    load_file = json_load,
    save_file = json_save,
)

exec = struct(
    run = run,
    shell = shell,
    env = env,
)

sync = struct(
    race = race,
    take = take,
    after = after,
    pipeline = pipeline,
)

time = struct(
    sleep = sleep,
    now = now,
)

util = struct(
    reduce = reduce,
    log = log,
)

net = struct(
    tcp = tcp,
    udp = udp,
    socket = socket,
)
