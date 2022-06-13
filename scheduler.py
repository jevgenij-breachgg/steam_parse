import schedule
import time
import steam_parse


def job():
    steam_parse.main()


schedule.every(20).seconds.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
