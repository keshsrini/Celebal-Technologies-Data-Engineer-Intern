import schedule
import time
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime

class ScheduleTrigger:
    def __init__(self, pipeline_func):
        self.pipeline_func = pipeline_func
        self.running = False
    
    def daily_at(self, time_str):
        schedule.every().day.at(time_str).do(self.pipeline_func)
    
    def hourly(self):
        schedule.every().hour.do(self.pipeline_func)
    
    def every_minutes(self, minutes):
        schedule.every(minutes).minutes.do(self.pipeline_func)
    
    def start(self):
        self.running = True
        while self.running:
            schedule.run_pending()
            time.sleep(1)
    
    def stop(self):
        self.running = False

class EventTrigger(FileSystemEventHandler):
    def __init__(self, pipeline_func, watch_directory="triggers"):
        self.pipeline_func = pipeline_func
        self.watch_directory = watch_directory
        self.observer = Observer()
    
    def on_created(self, event):
        if not event.is_directory:
            print(f"File created: {event.src_path}")
            self.pipeline_func()
    
    def on_modified(self, event):
        if not event.is_directory:
            print(f"File modified: {event.src_path}")
            self.pipeline_func()
    
    def start_watching(self):
        self.observer.schedule(self, self.watch_directory, recursive=True)
        self.observer.start()
        print(f"Watching directory: {self.watch_directory}")
    
    def stop_watching(self):
        self.observer.stop()
        self.observer.join()

class TriggerManager:
    def __init__(self, pipeline_func):
        self.pipeline_func = pipeline_func
        self.schedule_trigger = ScheduleTrigger(pipeline_func)
        self.event_trigger = EventTrigger(pipeline_func)
    
    def setup_schedule(self, schedule_type, **kwargs):
        if schedule_type == "daily":
            self.schedule_trigger.daily_at(kwargs.get("time", "09:00"))
        elif schedule_type == "hourly":
            self.schedule_trigger.hourly()
        elif schedule_type == "minutes":
            self.schedule_trigger.every_minutes(kwargs.get("minutes", 30))
    
    def start_all(self):
        # Start event trigger
        self.event_trigger.start_watching()
        
        # Start schedule trigger in separate thread
        schedule_thread = threading.Thread(target=self.schedule_trigger.start)
        schedule_thread.daemon = True
        schedule_thread.start()
    
    def stop_all(self):
        self.schedule_trigger.stop()
        self.event_trigger.stop_watching()