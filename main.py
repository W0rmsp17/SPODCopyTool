from ui.controller import Controller
from ui.app import App

def main():
    controller = Controller(
        timeout=(10, 300),
        chunk=8 * 1024 * 1024,
        min_chunk=1 * 1024 * 1024,
        max_single=4 * 1024 * 1024,
        delete_extras=False,
    )
    app = App(controller)
    app.run()

if __name__ == "__main__":
    main()
