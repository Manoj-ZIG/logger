from datetime import datetime, timedelta
from dateutil.parser import parse
try:
    from datetime_module.datetime_extractore import DatetimeExtractor
    from constant.date_suppression_constant import section_list, suppress_date_tag, suppress_date_without_adm_disch, precedence_dict
except ModuleNotFoundError as e:
    from ..datetime_module.datetime_extractore import DatetimeExtractor
    from ..constant.date_suppression_constant import section_list, suppress_date_tag, suppress_date_without_adm_disch, precedence_dict

class DateSuppressionLabs:
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
        inp_date = DateSuppressionLabs.date_parser(inp_date, (yr, 1, 1))
        min_date, max_date = min_max_date
        flag = False
        try:
            flag = parse(
                min_date)-timedelta(3) <= parse(inp_date) <= parse(max_date)

        except TypeError as e:
            pass
        if flag == True:
            return True
        else:
            return False

    @staticmethod
    def get_precedence_date(date_ls):
        # precedence_dict = {'recorded': 1, 'result': 2,  'recorded1': 1}
        filter_date_ls = [i for i in date_ls if i.get('date')[-1]
                          in precedence_dict.keys()]
        sorted_date_tag_ls = sorted(
            filter_date_ls, key=lambda k: precedence_dict[k.get('date')[-1]])
        if sorted_date_tag_ls:
            return sorted_date_tag_ls[0:1]
        else:
            # return date_ls
            return list()

    @staticmethod
    def date_suppression_lab(lab_date, curr_page, table_bb):
        if lab_date:
            detected_date_ls = None

            # stage 1 (suppress based on recent page)

            sorted_lab_date_recent_page = {
                k: v for k, v in lab_date.items() if k <= curr_page}
            sorted_lab_date = dict(
                sorted(sorted_lab_date_recent_page.items(), reverse=True))

            for page, date_ls in sorted_lab_date.items():
                if not detected_date_ls:
                    # stage 2: sort based on nearest bb
                    sorted_lab_date_stage2 = sorted(date_ls, key=lambda x: abs(table_bb -
                                                                                x.get('Geometry.BoundingBox.Top')))

                    # stag 3: filter based on date tags which has highest precedence
                    detected_date_ls = DateSuppressionLabs.get_precedence_date(
                        sorted_lab_date_stage2)
            return detected_date_ls
        else:
            return list()
