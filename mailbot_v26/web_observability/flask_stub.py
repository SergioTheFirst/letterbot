from __future__ import annotations

import base64
import re
import contextvars
import hmac
import json
import os
from dataclasses import dataclass
from hashlib import sha256
from http import HTTPStatus
from http.cookies import SimpleCookie
from typing import Any, Callable, Dict, Iterable
from urllib.parse import parse_qs, urlencode, urlparse
from wsgiref.simple_server import make_server

_request_ctx = contextvars.ContextVar("request_ctx")
_session_ctx = contextvars.ContextVar("session_ctx")
_app_ctx = contextvars.ContextVar("app_ctx")


class Request:
    def __init__(self, method: str, path: str, args: dict[str, str], form: dict[str, str]):
        self.method = method
        self.path = path
        self.args = args
        self.form = form
        self.endpoint: str | None = None


class _RequestProxy:
    def __getattr__(self, name: str) -> Any:
        ctx = _request_ctx.get(None)
        if ctx is None:
            raise RuntimeError("Request context not available")
        return getattr(ctx, name)


class Session(dict):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.permanent: bool = False


class _SessionProxy:
    def __getattr__(self, name: str) -> Any:
        ctx = _session_ctx.get(None)
        if ctx is None:
            raise RuntimeError("Session context not available")
        return getattr(ctx, name)

    def __setattr__(self, name: str, value: Any) -> None:
        ctx = _session_ctx.get(None)
        if ctx is None:
            raise RuntimeError("Session context not available")
        setattr(ctx, name, value)

    def __getitem__(self, key: str) -> Any:
        ctx = _session_ctx.get(None)
        if ctx is None:
            raise RuntimeError("Session context not available")
        return ctx[key]

    def __setitem__(self, key: str, value: Any) -> None:
        ctx = _session_ctx.get(None)
        if ctx is None:
            raise RuntimeError("Session context not available")
        ctx[key] = value

    def get(self, key: str, default: Any | None = None) -> Any:
        ctx = _session_ctx.get(None)
        if ctx is None:
            return default
        return ctx.get(key, default)


request = _RequestProxy()
session = _SessionProxy()


@dataclass
class Response:
    data: bytes
    status_code: int = 200
    headers: Dict[str, str] | None = None
    mimetype: str | None = None

    def get_data(self, as_text: bool = False) -> str | bytes:
        return self.data.decode("utf-8") if as_text else self.data

    def get_json(self) -> Any:
        try:
            return json.loads(self.data.decode("utf-8"))
        except Exception:
            return None

    @property
    def content_length(self) -> int:
        return len(self.data)


class TestClient:
    def __init__(self, app: "Flask") -> None:
        self.app = app
        self.cookies: dict[str, str] = {}

    def __enter__(self) -> "TestClient":
        return self

    def __exit__(self, exc_type, exc: object, tb: object) -> None:
        return None

    def _prepare_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self.cookies:
            cookie_header = "; ".join(f"{k}={v}" for k, v in self.cookies.items())
            headers["Cookie"] = cookie_header
        return headers

    def _handle_response(self, response: Response) -> Response:
        cookie_header = None
        if response.headers:
            cookie_header = response.headers.get("Set-Cookie")
        if cookie_header:
            simple_cookie = SimpleCookie()
            simple_cookie.load(cookie_header)
            for key, morsel in simple_cookie.items():
                self.cookies[key] = morsel.value
        return response

    def get(self, path: str, query_string: dict[str, str] | None = None, headers: dict[str, str] | None = None) -> Response:
        query = f"?{urlencode(query_string)}" if query_string else ""
        combined_headers = self._prepare_headers()
        if headers:
            combined_headers.update(headers)
        response = self.app._handle_request("GET", f"{path}{query}", data=None, headers=combined_headers)
        return self._handle_response(response)

    def post(self, path: str, data: dict[str, str] | None = None, headers: dict[str, str] | None = None) -> Response:
        combined_headers = self._prepare_headers()
        if headers:
            combined_headers.update(headers)
        response = self.app._handle_request("POST", path, data=data, headers=combined_headers)
        return self._handle_response(response)


class Flask:
    def __init__(self, name: str, template_folder: str | None = None, static_folder: str | None = None) -> None:
        self.name = name
        self.template_folder = template_folder
        self.static_folder = static_folder
        self.routes: list[tuple[str, set[str], Callable[[Any], Any], str | None]] = []
        self.before_request_funcs: list[Callable[[], Response | None]] = []
        self.secret_key: str | None = None
        self._endpoint_map: dict[str, str] = {}
        self.config: dict[str, object] = {}

    def route(self, path: str, methods: Iterable[str] | None = None) -> Callable[[Callable[[Any], Any]], Callable[[Any], Any]]:
        allowed_methods = set(methods or ["GET"])

        def decorator(func: Callable[[Any], Any]) -> Callable[[Any], Any]:
            endpoint = func.__name__
            self.routes.append((path, allowed_methods, func, endpoint))
            self._endpoint_map[endpoint] = path
            return func

        return decorator

    def before_request(self, func: Callable[[], Response | None]) -> Callable[[], Response | None]:
        self.before_request_funcs.append(func)
        return func

    def test_client(self) -> TestClient:
        return TestClient(self)

    def _load_session(self, headers: dict[str, str]) -> Session:
        cookie_header = headers.get("Cookie", "")
        cookie = SimpleCookie()
        cookie.load(cookie_header)
        raw = cookie.get("session")
        if not raw:
            return Session()
        value = raw.value
        try:
            data_part, sig = value.rsplit(".", 1)
        except ValueError:
            return Session()
        expected = hmac.new((self.secret_key or "").encode(), data_part.encode(), sha256).hexdigest()
        if not hmac.compare_digest(expected, sig):
            return Session()
        try:
            decoded = base64.urlsafe_b64decode(data_part.encode()).decode("utf-8")
            payload = json.loads(decoded)
            if isinstance(payload, dict):
                return Session(payload)
        except Exception:
            return Session()
        return Session()

    def _dump_session(self, session_obj: Session) -> str:
        payload = json.dumps(dict(session_obj), separators=(",", ":"))
        data_part = base64.urlsafe_b64encode(payload.encode()).decode()
        signature = hmac.new((self.secret_key or "").encode(), data_part.encode(), sha256).hexdigest()
        return f"{data_part}.{signature}"

    @staticmethod
    def _compile_route(path: str) -> tuple[str, list[str], list[str]]:
        parts = path.strip("/").split("/")
        names: list[str] = []
        converters: list[str] = []
        pattern_parts: list[str] = []
        for part in parts:
            if part.startswith("<") and part.endswith(">"):
                inner = part[1:-1]
                if ":" in inner:
                    converter, name = inner.split(":", 1)
                else:
                    converter, name = "string", inner
                converters.append(converter)
                names.append(name)
                if converter == "int":
                    pattern_parts.append(r"(?P<%s>\d+)" % name)
                else:
                    pattern_parts.append(r"(?P<%s>[^/]+)" % name)
            else:
                pattern_parts.append(re.escape(part))
        pattern = "^/" + "/".join(pattern_parts) + "$"
        return pattern, names, converters

    def _match_route(
        self, method: str, path: str
    ) -> tuple[Callable[[Any], Any] | None, str | None, dict[str, Any]]:
        path_only = path.split("?")[0]
        for route_path, methods, func, endpoint in self.routes:
            if method not in methods:
                continue
            if "<" not in route_path:
                if path_only == route_path:
                    return func, endpoint, {}
                continue
            pattern, names, converters = self._compile_route(route_path)
            match = re.match(pattern, path_only)
            if not match:
                continue
            kwargs: dict[str, Any] = {}
            for name, converter in zip(names, converters):
                value = match.group(name)
                if converter == "int":
                    try:
                        value = int(value)
                    except (TypeError, ValueError):
                        kwargs = {}
                        break
                kwargs[name] = value
            if kwargs:
                return func, endpoint, kwargs
        if path.startswith("/static/") and self.static_folder:
            return self._static_handler, "static", {}
        return None, None, {}

    def _parse_request(self, method: str, path: str, data: dict[str, str] | None, headers: dict[str, str]) -> Request:
        parsed = urlparse(path)
        args = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        form = {}
        if method == "POST" and data:
            form = {k: str(v) for k, v in data.items()}
        req = Request(method, parsed.path or path, args, form)
        return req

    def _handle_request(self, method: str, path: str, data: dict[str, str] | None, headers: dict[str, str] | None = None) -> Response:
        headers = headers or {}
        func, endpoint, kwargs = self._match_route(method, path)
        req = self._parse_request(method, path, data, headers)
        req.endpoint = endpoint
        token_req = _request_ctx.set(req)
        sess_obj = self._load_session(headers)
        token_sess = _session_ctx.set(sess_obj)
        token_app = _app_ctx.set(self)
        try:
            if func is None:
                return Response(b"Not Found", status_code=404, headers={})
            for hook in self.before_request_funcs:
                result = hook()
                if result is not None:
                    resp = result
                    break
            else:
                resp = func(**kwargs) if kwargs else func()
            if not isinstance(resp, Response):
                resp = Response(str(resp).encode("utf-8"))
            cookie_value = self._dump_session(sess_obj)
            resp.headers = resp.headers or {}
            resp.headers.setdefault("Set-Cookie", f"session={cookie_value}; Path=/; HttpOnly")
            return resp
        finally:
            _request_ctx.reset(token_req)
            _session_ctx.reset(token_sess)
            _app_ctx.reset(token_app)

    def _static_handler(self) -> Response:
        path = request.path[len("/static/") :]
        if not self.static_folder:
            return Response(b"Not Found", status_code=404, headers={})
        file_path = os.path.join(self.static_folder, path)
        if not os.path.isfile(file_path):
            return Response(b"Not Found", status_code=404, headers={})
        with open(file_path, "rb") as fp:
            data = fp.read()
        return Response(data, status_code=200, headers={"Content-Type": _guess_mime(file_path)})

    def run(self, host: str = "127.0.0.1", port: int = 5000) -> None:
        def app(environ, start_response):  # type: ignore[override]
            method = environ.get("REQUEST_METHOD", "GET")
            path = environ.get("PATH_INFO", "/")
            query = environ.get("QUERY_STRING")
            if query:
                path = f"{path}?{query}"
            headers = {"Cookie": environ.get("HTTP_COOKIE", "")}
            length = int(environ.get("CONTENT_LENGTH") or 0)
            data = None
            if method == "POST" and length:
                raw_body = environ["wsgi.input"].read(length).decode("utf-8")
                data = {k: v[0] for k, v in parse_qs(raw_body).items()}
            response = self._handle_request(method, path, data=data, headers=headers)
            start_response(
                f"{response.status_code} {HTTPStatus(response.status_code).phrase}",
                [(k, v) for k, v in (response.headers or {}).items()],
            )
            return [response.data]

        with make_server(host, port, app) as httpd:
            httpd.serve_forever()

    def url_map(self) -> dict[str, str]:
        return dict(self._endpoint_map)


def jsonify(payload: Any) -> Response:
    data = json.dumps(payload, separators=(",", ":"))
    return Response(data.encode("utf-8"), status_code=200, headers={"Content-Type": "application/json"})


def redirect(location: str, code: int = 302) -> Response:
    return Response(b"", status_code=code, headers={"Location": location})


def render_template(template_path: str, **context: Any) -> str:
    if not os.path.isabs(template_path):
        template_path = os.path.join(os.getcwd(), template_path)
    with open(template_path, "r", encoding="utf-8") as fp:
        content = fp.read()
    for key, value in context.items():
        placeholder = f"{{{{ {key} }}}}"
        placeholder_alt = f"{{{{{key}}}}}"
        content = content.replace(placeholder, str(value))
        content = content.replace(placeholder_alt, str(value))
    return content


def url_for(endpoint: str, **values: Any) -> str:
    app = _app_ctx.get(None)
    if app is None:
        return f"/{endpoint if endpoint != 'index' else ''}" if endpoint else "/"
    path = app._endpoint_map.get(endpoint)
    if path:
        return path
    return f"/{endpoint if endpoint else ''}"


def _guess_mime(file_path: str) -> str:
    if file_path.endswith(".css"):
        return "text/css"
    return "text/plain"
