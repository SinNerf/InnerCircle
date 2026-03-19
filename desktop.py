import argparse
import sys
import threading
import time

from PyQt6.QtCore import QUrl
from PyQt6.QtWebEngineWidgets import QWebEngineView
from PyQt6.QtWidgets import QApplication


def _start_local_server(host: str, port: int) -> None:
    import uvicorn
    uvicorn.run("app.main:app", host=host, port=port, log_level="warning")


def main() -> None:
    parser = argparse.ArgumentParser(description="InnerCircle Desktop")
    parser.add_argument("--url", default=None)
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    if args.url:
        target = args.url.rstrip("/")
    else:
        host = "127.0.0.1"
        target = f"http://{host}:{args.port}"
        server = threading.Thread(target=_start_local_server, args=(host, args.port), daemon=True)
        server.start()
        time.sleep(2)

    qt = QApplication(sys.argv)
    qt.setApplicationName("InnerCircle")

    view = QWebEngineView()
    view.setWindowTitle("InnerCircle")
    view.resize(1024, 720)
    view.setMinimumSize(480, 400)
    view.load(QUrl(target))
    view.show()

    sys.exit(qt.exec())


if __name__ == "__main__":
    main()
