from datetime import datetime
import os
from dotenv import load_dotenv
from src.worker import Worker

load_dotenv()

worker = Worker(os.getenv("INPUT_FILE_PATH"), os.getenv("OUT_DIR"), os.getenv("LANGUAGE"))

print(datetime.now(), "[process]", "Started.")
worker.process()
print(datetime.now(), "[process]", "Finished.")
