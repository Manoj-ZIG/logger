from datetime import datetime
from dateutil.parser import parse

import re
import pint
import random
import pandas as pd

try:
    from date_time_module.datetime_extractor import DatetimeExtractor
except ModuleNotFoundError as e:
    from ..date_time_module.datetime_extractor import DatetimeExtractor

class Postprocess:
    def __init__(self, adm_tag_list):
        self.adm_tag_list = adm_tag_list
    
    ###############  Date Preprocess  #########################
    def date_parser(self, date_str, default_date=(2020, 1, 1)):
        """
        date_parser function used to structure the date format (mm-dd-yyyy hh:mm:ss)
        args:
            str: date_string
            tuple: default_date
        return:
            str: date_str (formatted)
        """
        yy, mm, dd = default_date
        default_date_ = datetime(yy, mm, dd)
        try:
            date_str = str(parse(date_str))
        except:
            print("Failed : ", date_str)
            pass
        date_str_obj = DatetimeExtractor.get_date_time_from_corpus_v2(date_str,['None'])
        if date_str_obj and date_str_obj[0][1] and len(date_str_obj[0][1])<=4 and ':' not in date_str_obj[0][1]:
            # add extra 00 at the end of time
            date_str = f"{date_str_obj[0][0]} {date_str_obj[0][1]+'00'}"
        elif date_str_obj and not date_str_obj[0][1]:
            date_str = f"{date_str_obj[0][0]} 00:00:00"
        elif date_str_obj and  len(date_str_obj[0][1])>=5:
            date_str = f"{date_str_obj[0][0]} {date_str_obj[0][1]}"
        else:
            date_str = None
        # parse the date
        if date_str:
            try:
                parsed_date = parse(date_str, default=default_date_)
                day = parsed_date.day
                month = parsed_date.month
                year = parsed_date.year

                hour = parsed_date.hour
                minute = parsed_date.minute
                seconds = parsed_date.second
                if day<10 :
                    day_ = f'0{day}'
                else:
                    day_ = day
                return f"{month}-{day_}-{year} {hour}:{minute}:{seconds}"
            except Exception as e:
                pass
        else:
            return None

    def get_adm_discharge_date(self, corpus, threshold = 150000):
        """" 
        if corpus is highly dense, in order to reduce the time to bring year of adm/dsch date
        use the 25 % of corpus
        """
        if len(corpus) >= threshold:
            trimmed_corpus = corpus[:threshold]
        else:
            trimmed_corpus = corpus

        detected_date_tag = DatetimeExtractor.get_date_time_from_corpus_v2(
            trimmed_corpus, self.adm_tag_list)
        
        filter_date_list_ = [i for i in detected_date_tag if i[-1] and len(i[0]) > 4]
        filter_date_list_without_tag_ = [i for i in detected_date_tag if len(i[0]) > 4]
        filter_date_list = []
        filter_date_list_without_tag = []
        for i in filter_date_list_:
            try:
                if parse(i[0]).year:
                    filter_date_list.append(i)
            except:
                pass

        for i in filter_date_list_without_tag_:
            try:
                if parse(i[0]).year:
                    filter_date_list_without_tag.append(i)
            except:
                pass

        if filter_date_list:
            random_date = [parse(i[0]).year for i in random.choices(
                filter_date_list, k=5)]
            adm_year = max(set(random_date), key=random_date.count)
            return adm_year
        elif filter_date_list_without_tag:
            random_date = [parse(i[0]).year for i in random.choices(
                filter_date_list_without_tag, k=5)]
            adm_year = max(set(random_date), key=random_date.count)
            return adm_year
        else:
            return None
            
        # for dt in detected_date_tag:
        #     if dt[2] and len(dt[0]) > 4:
        #         adm_year = parse(dt[0]).year
        #         break
