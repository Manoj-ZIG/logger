from datetime import datetime, timedelta
from dateutil.parser import parse
try:
    from datetime_module.datetime_extractore import DatetimeExtractor
    from constant.date_suppression_constant import  suppress_date_tag, suppress_date_without_adm_disch, precedence_dict
except ModuleNotFoundError as e:
    from ..datetime_module.datetime_extractore import DatetimeExtractor
    from ..constant.date_suppression_constant import  suppress_date_tag, suppress_date_without_adm_disch, precedence_dict

class DateSuppression:
    def __init__(self) -> None:
        pass

    @staticmethod
    def date_parser(date_str, default_date=(2020, 1, 1)):
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

        # date_str = date_str.replace('-', '/')

        date_str_obj = DatetimeExtractor.get_date_time_from_corpus_v2(date_str, [
            'None'])
        if date_str_obj and date_str_obj[0][1] and len(date_str_obj[0][1]) <= 4 and ':' not in date_str_obj[0][1]:
            # add extra 00 at the end of time
            date_str = f"{date_str_obj[0][0]} {date_str_obj[0][1]+'00'}"
        elif date_str_obj and not date_str_obj[0][1]:
            date_str = f"{date_str_obj[0][0]} 00:00:00"
        elif date_str_obj and len(date_str_obj[0][1]) >= 5:
            date_str = f"{date_str_obj[0][0]} {date_str_obj[0][1][:8]}"
        elif date_str_obj and len(date_str_obj[0][1]) <= 5:
            date_str = f"{date_str_obj[0][0]} {date_str_obj[0][1]+':00'}"
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
                if day < 10:
                    day_ = f'0{day}'
                else:
                    day_ = day
                return f"{month}-{day_}-{year} {hour}:{minute}:{seconds}"
            except Exception as e:
                pass
        else:
            return None

    @staticmethod
    def guard_rail_date(inp_date, min_max_date, yr):
        inp_date = DateSuppression.date_parser(inp_date, (yr, 1, 1))
        min_date, max_date = min_max_date
        flag = False
        try:
            flag = parse(
                min_date)-timedelta(3) <= parse(inp_date) <= parse(max_date)+timedelta(3)

        except TypeError as e:
            pass
        if flag == True:
            return True
        else:
            return False

    @staticmethod
    def date_suppression(date_ls=[], min_max_date=(), is_section=False, crp=None,):
        # suppress_date_tag_ = [i.lower() for i in suppress_date_tag]
        min_date, max_date = min_max_date
        try:
            min_year, max_year = parse(min_date).year, parse(max_date).year
        except ValueError as e:
            min_year = 2000
            max_year = 2030

        if date_ls:
            # stage1: remove out-side date-range
            suppress_stage1 = []
            for dt in date_ls:
                date_obj = dt[0]
                time_obj = '' if not dt[1] else dt[1]
                tag_obj = dt[2]
                date_time_obj_str = f'{date_obj} {time_obj}'
                if DateSuppression.guard_rail_date(date_time_obj_str, min_max_date, min_year):
                    suppress_stage1.append(dt)

            # stage2: based on date-tags
            suppress_stage2 = []
            if suppress_stage1:
                suppress_stage2 = [i for i in suppress_stage1 if str(
                    i[-1]).lower() not in suppress_date_tag]
            else:
                # print('all dts outside of range')
                pass

            # stage 3 get precedence date's if multiple tags are present
            if suppress_stage2:
                suppress_stage3 = DateSuppression.get_precedence_date(
                    suppress_stage2)
                return suppress_stage3
            else:
                suppress_stage1a = [i for i in suppress_stage1 if str(
                    i[-1]).lower() not in suppress_date_without_adm_disch]
                return suppress_stage1a if is_section else suppress_stage2

        return date_ls

    @staticmethod
    # here check the precd_dict
    def get_precedence_date(date_ls):
        # precedence_dict = {'recorded': 1, 'result': 2,  'recorded1': 1}
        filter_date_ls = [i for i in date_ls if i[-1]
                          in precedence_dict.keys()]
        sorted_date_tag_ls = sorted(
            filter_date_ls, key=lambda k: precedence_dict[k[-1]])
        if sorted_date_tag_ls:
            return sorted_date_tag_ls[0:1]
        else:
            return date_ls
