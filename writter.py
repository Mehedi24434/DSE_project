
import process_data as p
import time
status = p.Time_checker(Market_Status=True)

while status == "Closed":
    p.writter()
    time.sleep(60)
    





