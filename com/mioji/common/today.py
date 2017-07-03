
import datetime


class Today(object):

    FT = '%Y%m%d'

    def __init__(self):
        self.today = datetime.date.today()
        self.day_strcache = {}
        self.today_tag = self.today.strftime(Today.FT)

    def day_offset_date_str(self, day_offset):
        s = self.day_strcache.get(day_offset, None)
        if s is None:
            day_offset = int(day_offset)
            date = self.today + datetime.timedelta(days=day_offset)
            s = date.strftime(Today.FT)
            self.day_strcache[day_offset] = s
        return s

today = Today()