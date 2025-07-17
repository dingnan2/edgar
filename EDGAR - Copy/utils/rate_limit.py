import threading
import time

class SafeRateLimiter:
    def __init__(self, capacity=1, refill_rate=9):
        self.capacity = capacity
        self.tokens = capacity
        self.refill_rate = refill_rate
        self.last_refill = time.time()
        self.lock = threading.Lock()
    
    def acquire(self):
        with self.lock:
            now = time.time()
            tokens_to_add = (now - self.last_refill) * self.refill_rate
            self.tokens = min(self.capacity, self.tokens + tokens_to_add)
            self.last_refill = now
            
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            
            wait_time = (1 - self.tokens) / self.refill_rate
            time.sleep(wait_time)
            self.tokens = 0
            return True
