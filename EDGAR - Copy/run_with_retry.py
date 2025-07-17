import subprocess
import time

def run_with_retry(script_name, max_retries=100, wait_time=10):
    attempt = 0
    while attempt < max_retries:
        try:
            print(f"\n[INFO] Attempt {attempt + 1}: Running {script_name}...\n")
            result = subprocess.run(["python", script_name], check=True)
            print(f"\n[SUCCESS] Script finished successfully.\n")
            break  # Exit loop if run is successful
        except subprocess.CalledProcessError as e:
            err_output = str(e)
            print(f"\n[ERROR] Script exited with error: {e}")

            # Only break if the error is due to rate limiting
            if "429 ERROR" in err_output:
                print("\n[STOP] Exiting due to rate limit.")
                break
            else:
                attempt += 1
                print(f"\n[RETRY] Waiting {wait_time} seconds before retrying...\n")
                time.sleep(wait_time)

if __name__ == "__main__":
    run_with_retry("daily_downloader_inspect.py")