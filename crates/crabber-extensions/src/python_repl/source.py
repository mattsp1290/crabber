
import ast, builtins, io, json, os, struct, sys, traceback

PROTOCOL = sys.argv[1]
REQUEST_MAX = int(sys.argv[2])
RESPONSE_MAX = int(sys.argv[3])
STDOUT_MAX = int(sys.argv[4])
STDERR_MAX = int(sys.argv[5])
RESULT_MAX = int(sys.argv[6])
EXCEPTION_MAX = int(sys.argv[7])
REQUEST_FD = os.dup(0)
RESPONSE_FD = os.dup(1)
devnull = os.open(os.devnull, os.O_RDWR)
for fd in (0, 1, 2):
    os.dup2(devnull, fd)
if devnull > 2:
    os.close(devnull)
sys.stdin = open(0, "r")

class BoundedWriter(io.TextIOBase):
    def __init__(self, maximum):
        self.maximum = maximum
        self.parts = []
        self.used = 0
        self.truncated = False
        self.active = True
    def writable(self):
        return True
    def write(self, value):
        if not isinstance(value, str):
            value = str(value)
        accepted = len(value)
        if not self.active or self.truncated:
            return accepted
        raw = value.encode("utf-8", "replace")
        room = self.maximum - self.used
        if len(raw) > room:
            raw = raw[:max(room, 0)]
            while raw:
                try:
                    value = raw.decode("utf-8")
                    break
                except UnicodeDecodeError:
                    raw = raw[:-1]
            else:
                value = ""
            self.truncated = True
        else:
            value = raw.decode("utf-8")
        if raw:
            self.parts.append(value)
            self.used += len(raw)
        return accepted
    def flush(self):
        return None
    def seal(self):
        self.active = False
    def result(self):
        return {"text": "".join(self.parts), "truncated": self.truncated}

class DiscardWriter(io.TextIOBase):
    def writable(self):
        return True
    def write(self, value):
        return len(value) if isinstance(value, str) else 0
    def flush(self):
        return None

def read_exact(fd, size):
    chunks = []
    remaining = size
    while remaining:
        chunk = os.read(fd, remaining)
        if not chunk:
            raise EOFError()
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)

def read_frame():
    header = read_exact(REQUEST_FD, 4)
    size = struct.unpack(">I", header)[0]
    if size == 0 or size > REQUEST_MAX:
        raise RuntimeError("invalid request frame")
    raw = read_exact(REQUEST_FD, size)
    return json.loads(raw.decode("utf-8"))

def write_all(fd, raw):
    offset = 0
    while offset < len(raw):
        offset += os.write(fd, raw[offset:])

def write_frame(value):
    raw = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if not raw or len(raw) > RESPONSE_MAX:
        raise RuntimeError("invalid response frame")
    write_all(RESPONSE_FD, struct.pack(">I", len(raw)) + raw)

def bounded(value, maximum):
    raw = value.encode("utf-8", "replace")
    if len(raw) <= maximum:
        return {"text": raw.decode("utf-8"), "truncated": False}
    raw = raw[:maximum]
    while raw:
        try:
            return {"text": raw.decode("utf-8"), "truncated": True}
        except UnicodeDecodeError:
            raw = raw[:-1]
    return {"text": "", "truncated": True}

user_globals = {"__builtins__": builtins, "__name__": "__main__"}
discard = DiscardWriter()
sys.stdout = discard
sys.stderr = discard

write_frame({"version": PROTOCOL, "phase": "ready", "python": [sys.version_info.major, sys.version_info.minor]})

while True:
    try:
        request = read_frame()
    except EOFError:
        break
    if not isinstance(request, dict) or set(request) != {"version", "id", "code"}:
        raise RuntimeError("invalid request")
    if request["version"] != PROTOCOL or not isinstance(request["id"], int) or request["id"] <= 0 or not isinstance(request["code"], str):
        raise RuntimeError("invalid request")
    stdout = BoundedWriter(STDOUT_MAX)
    stderr = BoundedWriter(STDERR_MAX)
    sys.stdout = stdout
    sys.stderr = stderr
    status = "completed"
    result = {"text": "", "truncated": False}
    exception = {"text": "", "truncated": False}
    try:
        tree = ast.parse(request["code"], filename="<python_repl>", mode="exec")
        if tree.body and isinstance(tree.body[-1], ast.Expr):
            prefix = ast.Module(body=tree.body[:-1], type_ignores=[])
            if prefix.body:
                exec(compile(prefix, "<python_repl>", "exec"), user_globals, user_globals)
            value = eval(compile(ast.Expression(tree.body[-1].value), "<python_repl>", "eval"), user_globals, user_globals)
            result = bounded(repr(value), RESULT_MAX)
        else:
            exec(compile(tree, "<python_repl>", "exec"), user_globals, user_globals)
    except BaseException as exc:
        status = "python_error"
        user_tb = exc.__traceback__
        while user_tb is not None and user_tb.tb_frame.f_code.co_filename != "<python_repl>":
            user_tb = user_tb.tb_next
        rendered = []
        if user_tb is not None:
            rendered.append("Traceback (most recent call last):\n")
            rendered.extend(traceback.format_list(traceback.extract_tb(user_tb)))
        rendered.extend(traceback.format_exception_only(type(exc), exc))
        exception = bounded("".join(rendered), EXCEPTION_MAX)
    finally:
        stdout.seal()
        stderr.seal()
        sys.stdout = discard
        sys.stderr = discard
    write_frame({
        "version": PROTOCOL, "id": request["id"], "status": status,
        "stdout": stdout.result(), "stderr": stderr.result(),
        "result": result, "exception": exception,
    })
