import threading
import time
from itertools import batched


class BackgroundTask:
    def __init__(self, background_task):
        self.is_running = False
        self.thread = None
        self.background_task = background_task

    def background_function(self):
        print("Background task started")
        while self.is_running:
            self.background_task()
            # print("Working...")
            time.sleep(1)  # Simulate some work
        print("Background task stopped")

    def start(self):
        if not self.is_running:
            self.is_running = True
            self.thread = threading.Thread(target=self.background_function)
            self.thread.start()
            print("Thread started")

    def stop(self):
        if self.is_running:
            self.is_running = False
            self.thread.join()  # Wait for the thread to finish
            print("Thread stopped")