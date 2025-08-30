from datetime import datetime
from dateutil.parser import parse
import pandas as pd
import random
import pint
import re
import calendar
from rapidfuzz import process, fuzz
try:
    from datetime_module.datetime_extractore import DatetimeExtractor
except ModuleNotFoundError as e:
    from ..datetime_module.datetime_extractore import DatetimeExtractor


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
        
        # date_str = date_str.replace('-', '/')
        # get all month & abbr from calender
        all_mnths_ = list(calendar.month_name)[1:] + list(calendar.month_abbr)[1:]
        all_mnths = list(map(lambda x: x.lower(), all_mnths_))

        if '-' in date_str and not any(ele in date_str.lower() for ele in all_mnths):
            date_str = date_str.replace('-', '/')
     
        date_str_obj = DatetimeExtractor.get_date_time_from_corpus_v2(date_str,['None'])
        if date_str_obj and date_str_obj[0][1] and len(date_str_obj[0][1])<=4 and ':' not in date_str_obj[0][1]:
            # add extra 00 at the end of time
            date_str = f"{date_str_obj[0][0]} {date_str_obj[0][1]+'00'}"
        elif date_str_obj and not date_str_obj[0][1]:
            date_str = f"{date_str_obj[0][0]} 00:00:00"
        elif date_str_obj and  len(date_str_obj[0][1])>=5:
            date_str = f"{date_str_obj[0][0]} {date_str_obj[0][1][:8]}"
        elif date_str_obj and  len(date_str_obj[0][1])<=5:
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
        
        filter_date_list_ = [
            i for i in detected_date_tag if i[-1] and len(i[0]) > 4]
        filter_date_list_without_tag_ = [
            i for i in detected_date_tag if len(i[0]) > 4]
        
        filter_date_list, filter_date_list_without_tag = [], []

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
      
    
    def get_process_value_str(raw_value_str):
        """" 
        function use to clean the raw string, like if its contain any date & time it will detect them and
        remove it from given raw string.
        """
        # raw_value_str = str(raw_value_str).lower()
        raw_value_str = str(raw_value_str.replace(',','.')).lower()
        detected_datetime_str = DatetimeExtractor.get_date_time_from_corpus_v2(str(raw_value_str), [
            'None'])
        detected_time_str = DatetimeExtractor.suppress_datetime(
            DatetimeExtractor.validate_time(raw_value_str))
        
        processed_str = raw_value_str
        detected_ranges = re.findall(r"(?:[\(\[\{].*?[\)\]\}])", raw_value_str)
        # clean date/time
        if detected_datetime_str or detected_time_str:
            date_, time_, _ = detected_datetime_str[0] if detected_datetime_str else (
                None, None, None)
            time_ = detected_time_str[0][0] if detected_time_str and not time_ else time_
            processed_str = processed_str.replace(
                str(date_), '').replace(str(time_), '')
        # clean the range brackets
        if detected_ranges:
            processed_str = processed_str.replace(detected_ranges[0], '')
        # detecting  ranges with no brackets
        detected_ranges_without_brackets=re.findall(r"(?:\d{1,2}(?:\.\d{1,3})?\s?\-\s?\d{1,2}(?:\.\d{1,3})?\s?)",processed_str, re.I)
        if detected_ranges_without_brackets:
              processed_str=processed_str.replace(detected_ranges_without_brackets[0], '')
        return processed_str.strip()
    ###############  Vital Preprocess  #########################
    def suppress_floating_values(val_str):
        val, val_ = None,None
        try:
            val = re.sub("[^0-9.:]+", " ", str(val_str).strip()).strip().split(" ")[0]
        except IndexError as e:
            pass
        if val:
            try:
                val_ = int(val)
            except ValueError as e:
                pass
            return val_
        else:
            return None
        

    def temperature_postprocess(temp_str, default='degF'):
        temp_str = Postprocess.get_process_value_str(temp_str)
        temp_str = str(temp_str).strip()
        ureg = pint.UnitRegistry()
        Q_ = ureg.Quantity
        temp_val = None
        try:
            temp_val = float(
                re.sub(r"[^0-9.]", " ", temp_str).split(" ")[0])
        except ValueError as e:
            pass
        if temp_val:
            if 30 <= temp_val <= 40:
                temp_degC = Q_(temp_val, ureg.degC)
                temp_degF = temp_degC.to(ureg.degF)
                return round(temp_degF.magnitude, 2) if default == 'degF' else round(temp_degC.magnitude, 2)
            elif 88 <= temp_val <= 108:
                temp_degF = Q_(temp_val, ureg.degF)
                temp_degC = temp_degF.to(ureg.degC)
                return round(temp_degF.magnitude, 2) if default == 'degF' else round(temp_degC.magnitude, 2)
            else:
                pass

    def blood_pressure_postprocess(bp_str, default='mm Hg'):
        bp_str = Postprocess.get_process_value_str(str(bp_str).strip())
        systolic = None
        diastolic = None
        matcher = re.finditer(
            r"(?P<systolic>\d{2,3})\s*(?:\/)\s*(?P<diastolic>\d{2,3})", bp_str, re.I)
        for i in matcher:
            systolic = i.group('systolic')
            diastolic = i.group('diastolic')
            break
        if not (systolic or diastolic):
            bp_val = re.sub("[^0-9]+", " ", bp_str).split(" ")[0]
            if bp_val:
                return int(bp_val),None
            else:
                return None, None
        else:
            return int(systolic), int(diastolic)
        
    def respiratory_rate_postprocess(resp_str, default='br/min'):
        resp_str = str(resp_str).strip()
        resp_str = Postprocess.get_process_value_str(resp_str)
        resp_val = Postprocess.suppress_floating_values(resp_str)
        
        if resp_val and 10 <= int(resp_val) <= 60:
            return int(resp_val)
        
    def spo2_postprocess(spo2_str, default='%'):
        spo2_str = str(spo2_str).strip()
        spo2_str = Postprocess.get_process_value_str(spo2_str)
        spo2_val = Postprocess.suppress_floating_values(spo2_str)
       
        if spo2_val and 80 <= int(spo2_val) <= 100:
            return int(spo2_val)
    
    def pulse_rate_postprocess(pulse_str, r_min=50,r_max=135, default='%'):
        pulse_str = str(pulse_str).strip()
        pulse_str = Postprocess.get_process_value_str(pulse_str)
        pulse_val = Postprocess.suppress_floating_values(pulse_str)
        if pulse_val and r_min <= int(pulse_val) <= r_max:
            return int(pulse_val)
    def o2_flow_rate_postprocess(flow_rate_str, r_min=0,r_max=20, default='lpm'):
        flow_rate_str = str(flow_rate_str).strip()
        flow_rate_str = Postprocess.get_process_value_str(flow_rate_str)
        flow_rate_val = re.sub("[^0-9]+", " ", flow_rate_str).split(" ")[0]
        if flow_rate_val and r_min<= int(flow_rate_val) <= r_max:
            return int(flow_rate_val)
    def map_postprocess(map_str, default = 'mmHg'):
        map_str = Postprocess.get_process_value_str(map_str)
        map_str = str(map_str).strip()
        # ureg = pint.UnitRegistry()
        # Q_ = ureg.Quantity
        map_val = None
        try:
            if map_str:
                map_val = int(
                    re.sub(r"[^0-9.]", " ", map_str).split(" ")[0])
                return map_val
            else:
                return None
        except ValueError as e:
            pass
    def fio2_postprocess(fio2_str, default = '%'):
        try:
            fio2_str = str(fio2_str).strip()
            fio2_str = Postprocess.get_process_value_str(fio2_str)
            fio2_str = float(
                        re.sub(r"[^0-9]", " ", fio2_str).split(" ")[0]
                        )
            return fio2_str
        except Exception as e:
            return None

    ###############  WBC Preprocess  #########################
    def wbc_postprocess(wbc_str):
        ureg = pint.UnitRegistry()
        wbc_str = Postprocess.get_process_value_str(wbc_str)
        wbc_str = str(wbc_str).replace(',', '.')
        wbc_unit = ['10^3/uL', 'k/cumm', 'k/uL', '1000/mm3',
                    'thou/mcL', '10^3/mm^3', '10^3/cumm', 'x10^3/mm^3',]
        # wbc_value_match = re.findall(r"^(?:\d{1,}\.?\d*)", wbc_str.strip(), re.I)
        wbc_value_match = re.findall(r"(?:\d{1,}\.?\d*)", wbc_str.strip(), re.I)

        wbc_value = None
        if wbc_value_match:
            wbc_value = wbc_value_match[0]

            if any(item in wbc_str for item in wbc_unit) and wbc_value:
                postprocess_wbc_value = float(wbc_value) * ureg.thou / ureg.µL
            elif wbc_value and 1 <= float(wbc_value) <= 100:
                postprocess_wbc_value = float(wbc_value) * ureg.thou / ureg.µL
            else:
                postprocess_wbc_value = (
                    float(wbc_value) / 1000) * ureg.thou / ureg.µL
            return (postprocess_wbc_value.magnitude, 'k/uL')
        else:
            return None, None
        
    
    def imm_gran_postprocess(imm_gran_str, default='%'):
        ureg = pint.UnitRegistry()
        Q_ = ureg.Quantity
        imm_gran_val = None
        imm_gran_str = Postprocess.get_process_value_str(imm_gran_str)
        try:
            imm_gran_val = float(
                re.sub(r"[^0-9.]", " ", str(imm_gran_str)).split(" ")[0])
        except ValueError as e:
            return None, None
        if imm_gran_val or imm_gran_val == 0.0:
            imm_gran_pct = Q_(imm_gran_val, '%')
            imm_gran_base = imm_gran_pct.to_base_units()
            return (imm_gran_pct.magnitude, default ) if default == '%' else (imm_gran_base.magnitude, default)
        else:
            return None, None
    
    def procalcitonin_postprocess(pct_str, default='ng/ml'):
        ureg = pint.UnitRegistry()
        Q = ureg.Quantity
        pct_str = str(pct_str).lower()
        pct_str = Postprocess.get_process_value_str(pct_str)
        other_pct_unit = ['ug/l', 'ng/ml', 'ng/l', 'ng/dl']
        pct_str_match = re.findall(
            r"(?:\d{1,}\.?\d*)", pct_str.strip(), re.I)
        pct_unit_ = re.sub(r"[0-9.@]", '', pct_str.strip().lower())

        pct_value = None
        if pct_str_match:
            pct_value = pct_str_match[0]
            matcher = process.extractOne(
                pct_unit_, other_pct_unit, scorer=fuzz.token_sort_ratio)
            if matcher[1] >= 78 and pct_value:
                postprocess_pct_value = Q(float(pct_value), matcher[0])
                postprocess_pct_value_conv = postprocess_pct_value.to(default)
                return (round(postprocess_pct_value.magnitude, 2), matcher[0]) if default == matcher[0] else (round(postprocess_pct_value_conv.magnitude, 2), default)
            else:
                postprocess_pct_value = Q(float(pct_value), default)
                return (round(postprocess_pct_value.magnitude, 2), default)
        else:
            return None, None
    
    def crp_postprocess(crp_str, default='mg/l'):
        ureg = pint.UnitRegistry()
        Q = ureg.Quantity
        crp_str = str(crp_str).lower()
        crp_str = Postprocess.get_process_value_str(crp_str)
        other_crp_unit = ['ug/ml', 'mg/l', 'mg/dl', 'g/l',]
        crp_str_match = re.findall(
            r"(?:\d{1,}\.?\d*)", crp_str.strip(), re.I)
        crp_unit_ = re.sub(r"[0-9.@]", '', crp_str.strip().lower())

        crp_value = None
        if crp_str_match:
            crp_value = crp_str_match[0]
            matcher = process.extractOne(
                crp_unit_, other_crp_unit, scorer=fuzz.token_sort_ratio)
            if matcher[1] >= 78 and crp_value:
                postprocess_crp_value = Q(float(crp_value), matcher[0])
                postprocess_crp_value_conv = postprocess_crp_value.to(default)
                return (round(postprocess_crp_value.magnitude, 2), matcher[0]) if default == matcher[0] else (round(postprocess_crp_value_conv.magnitude, 2), default)
            else:
                postprocess_crp_value = Q(float(crp_value), default)
                return (round(postprocess_crp_value.magnitude, 2), default)
        else:
            return None, None
    ###############  Chemistry Preprocess  #########################
    def bun_postprocess(bun_str, default='mg/dl'):
        ureg = pint.UnitRegistry()
        Q = ureg.Quantity

        bun_str = str(bun_str).lower()
        bun_str = Postprocess.get_process_value_str(bun_str)
        bun_unit = ['mg/dl']
        bun_other_unit = ['mg/l', 'µg/ml']
        bun_str_match = re.findall(r"(?:\d{1,}\.?\d*)", bun_str.strip(), re.I)

        bun_value = None
        if bun_str_match:
            bun_value = bun_str_match[0]

            if any(item in bun_str for item in bun_unit) and bun_value:
                postprocess_bun_value = Q(float(bun_value), 'mg/dL')
                postprocess_bun_value_conv = postprocess_bun_value.to('µg/mL')
                return (postprocess_bun_value.magnitude, default) if default == 'mg/dl' else (postprocess_bun_value_conv.magnitude, 'µg/mL')
            elif any(item in bun_str for item in bun_other_unit) and bun_value:
                postprocess_bun_value = Q(float(bun_value), 'µg/mL')
                postprocess_bun_value_conv = postprocess_bun_value.to('mg/dL')
                return (postprocess_bun_value.magnitude, 'µg/mL') if default != 'mg/dl' else (postprocess_bun_value_conv.magnitude, default)

            else:
                postprocess_bun_value = Q(float(bun_value), 'mg/dL')
                return (postprocess_bun_value.magnitude, default)

        else:
            return None, None
        
    def sodium_postprocess(na_str, default='mmol/l'):
        ureg = pint.UnitRegistry()
        Q = ureg.Quantity
        na_str = str(na_str).lower()
        na_unit = ['mmol/l']
        other_na_unit = ['mol/l', 'mmol/dl', 'mol/dl']
        na_str_match = Postprocess.get_process_value_str(na_str)
        na_str_match = re.findall(
            r"(?:\d{1,}\.?\d*)", na_str_match.strip(), re.I)

        na_value = None
        if na_str_match:
            na_value = na_str_match[0]
            if any(item in na_str for item in na_unit) and na_value:
                postprocess_na_value = Q(float(na_value), 'mmol/L')
                postprocess_na_value_conv = postprocess_na_value.to('mol/l')
                return (postprocess_na_value.magnitude,default) if default == 'mmol/l' else (postprocess_na_value_conv.magnitude, 'mol/l')

            elif any(item in na_str for item in other_na_unit) and na_value:
                unit_index = [item in na_str for item in other_na_unit].index(True)
                postprocess_na_value = Q(
                    float(na_value), other_na_unit[unit_index])
                postprocess_na_value_conv = postprocess_na_value.to('mmol/l')
                return (postprocess_na_value.magnitude, other_na_unit[unit_index]) if default in other_na_unit else (postprocess_na_value_conv.magnitude, 'mmol/l')
            # here we need to add range if available
            else:
                postprocess_na_value = Q(float(na_value), 'mmol/L')
                return (postprocess_na_value.magnitude, default)
        else:
            return None, None
        
    def creatinine_postprocess(creat_str, default='mg/dl'):
        ureg = pint.UnitRegistry()
        Q = ureg.Quantity
        creat_range = (0, 10)
        creatinine_str = Postprocess.get_process_value_str(creat_str)
        creatinine_unit = ['mg/dl']
        creatinine_other_unit = ['mg/l', 'µg/ml']
        creatinine_str_match = re.findall(
            r"(?:\d{1,}\.?\d*)", creatinine_str.strip(), re.I)

        creatinine_value = None
        if creatinine_str_match:
            creatinine_value = creatinine_str_match[0]

            if any(item in str(creatinine_str) for item in creatinine_unit) and creatinine_value:
                postprocess_creatinine_value = Q(float(creatinine_value), 'mg/dL')
                postprocess_creatinine_value_conv = postprocess_creatinine_value.to(
                    'µg/mL')
                range_flag = creat_range[0] <= postprocess_creatinine_value.magnitude <= creat_range[1]
                range_flag_ = creat_range[0] <= postprocess_creatinine_value_conv.magnitude <= creat_range[1]
                
                creat_val, creat_unit = (postprocess_creatinine_value.magnitude, default) if default == 'mg/dl' and range_flag else (postprocess_creatinine_value_conv.magnitude, 'µg/mL') 
                return (creat_val, creat_unit) if creat_val else (None,None)
            
            elif any(item in str(creatinine_str) for item in creatinine_other_unit) and creatinine_value:
                postprocess_creatinine_value = Q(float(creatinine_value), 'µg/mL')
                postprocess_creatinine_value_conv = postprocess_creatinine_value.to(
                    'mg/dL')
                range_flag = creat_range[0] <= postprocess_creatinine_value.magnitude <= creat_range[1]
                range_flag_ = creat_range[0] <= postprocess_creatinine_value_conv.magnitude <= creat_range[1]
                # return postprocess_creatinine_value.magnitude if default != 'mg/dl' else postprocess_creatinine_value_conv.magnitude
                creat_val, creat_unit = (postprocess_creatinine_value.magnitude, 'µg/mL') if default != 'mg/dl' and range_flag else (postprocess_creatinine_value_conv.magnitude, default) 
                return creat_val, creat_unit if creat_val else (None, None)

            else:
                postprocess_creatinine_value = Q(float(creatinine_value), 'mg/dL')
                postprocess_creatinine_value_conv = postprocess_creatinine_value.to(
                    'µg/mL')
                range_flag = creat_range[0] <= postprocess_creatinine_value.magnitude <= creat_range[1]
                range_flag_ = creat_range[0] <= postprocess_creatinine_value_conv.magnitude <= creat_range[1]
                # return postprocess_creatinine_value.magnitude if default == 'mg/dl' else postprocess_creatinine_value_conv.magnitude
                creat_val, creat_unit = (postprocess_creatinine_value.magnitude, default) if default == 'mg/dl' and range_flag else (postprocess_creatinine_value_conv.magnitude, 'µg/mL')
                return creat_val, creat_unit if creat_val else (None,None)
        else:
            return None, None
        
    def baseline_creatinine_postprocess(baseline_creat_str, default='mg/dl'):
        
        baseline_creat_str = Postprocess.get_process_value_str(baseline_creat_str)
        baseline_creat_str = str(baseline_creat_str).strip()
        try:
            if baseline_creat_str:
                baseline_creat_str = float(
                    re.sub(r"[^0-9.]", " ", baseline_creat_str).split(" ")[0]
                    )
                return baseline_creat_str, default
            else:
                return None, None
            
        except ValueError as e:
            print(e)
            return None, None 

    def bilirubin_postprocess(bilirubin_str, default='mg/dl'):
        ureg = pint.UnitRegistry()
        Q = ureg.Quantity

        bilirubin_str = Postprocess.get_process_value_str(bilirubin_str)
        bilirubin_unit = ['mg/dl']
        bilirubin_other_unit = ['mg/l', 'µg/ml']
        bilirubin_str_match = re.findall(
            r"(?:\d{1,}[,.]\d*)", bilirubin_str.strip(), re.I)

        bilirubin_value = None
        if bilirubin_str_match:
            bilirubin_value = bilirubin_str_match[0]
            bilirubin_value = bilirubin_value.replace(',','.')

            if any(item in bilirubin_str for item in bilirubin_unit) and bilirubin_value:
                postprocess_bilirubin_value = Q(
                    float(bilirubin_value), 'mg/dL')
                postprocess_bilirubin_value_conv = postprocess_bilirubin_value.to(
                    'µg/mL')
                return (postprocess_bilirubin_value.magnitude, default) if default == 'mg/dl' else (postprocess_bilirubin_value_conv.magnitude, 'µg/mL')
            elif any(item in bilirubin_str for item in bilirubin_other_unit) and bilirubin_value:
                postprocess_bilirubin_value = Q(
                    float(bilirubin_value), 'µg/mL')
                postprocess_bilirubin_value_conv = postprocess_bilirubin_value.to(
                    'mg/dL')
                return (postprocess_bilirubin_value.magnitude, 'µg/mL') if default != 'mg/dl' else (postprocess_bilirubin_value_conv.magnitude, default)

            else:
                postprocess_bilirubin_value = Q(
                    float(bilirubin_value), 'mg/dL')
                postprocess_bilirubin_value_conv = postprocess_bilirubin_value.to(
                    'µg/mL')
                return (postprocess_bilirubin_value.magnitude, default) if default == 'mg/dl' else (postprocess_bilirubin_value_conv.magnitude, 'µg/mL')
        else:
            return None, None
    
    def potassium_postprocess(pot_str, default='mmol/l'):
        ureg = pint.UnitRegistry()
        Q = ureg.Quantity
        pot_str = Postprocess.get_process_value_str(pot_str)
        pot_unit = ['mmol/l']
        other_pot_unit = ['mol/l', 'mmol/dl', 'mol/dl']
        pot_str_match = re.findall(
            r"(?:\d{1,}[,.]?\d*)", pot_str.strip(), re.I)

        pot_value = None
        if pot_str_match:
            pot_value = pot_str_match[0]
            pot_value = pot_value.replace(',', '.')
            if any(item in pot_str for item in pot_unit) and pot_value:
                postprocess_pot_value = Q(float(pot_value), 'mmol/L')
                postprocess_pot_value_conv = postprocess_pot_value.to('mol/l')
                return (postprocess_pot_value.magnitude, default) if default == 'mmol/l' else (postprocess_pot_value_conv.magnitude, 'mol/l')

            elif any(item in pot_str for item in other_pot_unit) and pot_value:
                unit_index = [
                    item in pot_str for item in other_pot_unit].index(True)
                postprocess_pot_value = Q(
                    float(pot_value), other_pot_unit[unit_index])
                postprocess_pot_value_conv = postprocess_pot_value.to('mmol/l')
                return (postprocess_pot_value.magnitude, other_pot_unit[unit_index]) if default in other_pot_unit else (postprocess_pot_value_conv.magnitude, default)
            # here we need to add range if available
            else:
                postprocess_pot_value = Q(float(pot_value), 'mmol/L')
                return (postprocess_pot_value.magnitude, default)
        else:
            return None, None
 
    def total_protein_postprocess(total_protein_str, default='g/dl'):
        ureg = pint.UnitRegistry()
        Q = ureg.Quantity
        total_protein_str = Postprocess.get_process_value_str(
            total_protein_str)
        total_protein_unit = ['g/dl']
        other_total_protein_unit = ['g/l']
        total_protein_str_match = re.findall(
            r"(?:\d{1,}\.?\d*)", total_protein_str.strip(), re.I)

        total_protein_value = None
        if total_protein_str_match:
            total_protein_value = total_protein_str_match[0]
            if any(item in total_protein_str for item in total_protein_unit) and total_protein_value:
                postprocess_total_protein_value = Q(
                    float(total_protein_value), 'g/dl')
                postprocess_total_protein_value_conv = postprocess_total_protein_value.to(
                    'g/l')
                return (postprocess_total_protein_value.magnitude, default) if default == 'g/dl' else (postprocess_total_protein_value_conv.magnitude, 'g/l')

            elif any(item in total_protein_str for item in other_total_protein_unit) and total_protein_value:
                unit_index = [
                    item in total_protein_str for item in other_total_protein_unit].index(True)
                postprocess_total_protein_value = Q(
                    float(total_protein_value), other_total_protein_unit[unit_index])
                postprocess_total_protein_value_conv = postprocess_total_protein_value.to(
                    'g/dl')
                return (postprocess_total_protein_value.magnitude, 'g/l') if default in other_total_protein_unit else (postprocess_total_protein_value_conv.magnitude, default)
            # here we need to add range if available
            else:
                postprocess_total_protein_value = Q(
                    float(total_protein_value), 'g/dl')
                return (postprocess_total_protein_value.magnitude, default)
        else:
            return None, None
    
    ####### Urinalysis #############
    # urinalysis
    def urine_specific_gravity_postprocess(ua_spgr_str):
        ua_spgr_str = Postprocess.get_process_value_str(ua_spgr_str)
        ua_spgr_str_match = re.findall(
            r"(?:\d{1,}\.?\d*)", ua_spgr_str, re.I)
        ua_spgr_value = None
        if ua_spgr_str_match:
            ua_spgr_value = ua_spgr_str_match[0]
            return float(ua_spgr_value),None
        else:
            return None, None

    def urine_osmolality_postprocess(osmolality_str):
        osmolality_str = Postprocess.get_process_value_str(osmolality_str)
        osmolality_str_match = re.findall(
            r"(?:\d{1,}\.?\d*)", osmolality_str, re.I)
        osmolality_value = None
        if osmolality_str_match:
            osmolality_value = osmolality_str_match[0]
            return float(osmolality_value), 'mOsm/kg'
        else:
            return None, None
    
    def urine_creatinine_postprocess(urine_creatinine_str, default='mg/dl'):
        ureg = pint.UnitRegistry()
        Q = ureg.Quantity
        urine_creatinine_str = Postprocess.get_process_value_str(
            urine_creatinine_str)
        urine_creatinine_unit = ['mg/dl']
        other_urine_creatinine_unit = ['g/l', 'g/dl']
        urine_creatinine_str_match = re.findall(
            r"(?:\d{1,}\.?\d*)", urine_creatinine_str.strip(), re.I)

        urine_creatinine_value = None
        if urine_creatinine_str_match:
            urine_creatinine_value = urine_creatinine_str_match[0]
            if any(item in urine_creatinine_str for item in urine_creatinine_unit) and urine_creatinine_value:
                postprocess_urine_creatinine_value = Q(
                    float(urine_creatinine_value), 'mg/dl')
                postprocess_urine_creatinine_value_conv = postprocess_urine_creatinine_value.to(
                    'g/l')
                return (postprocess_urine_creatinine_value.magnitude, default) if default == 'mg/dl' else (postprocess_urine_creatinine_value_conv.magnitude, 'g/l')

            elif any(item in urine_creatinine_str for item in other_urine_creatinine_unit) and urine_creatinine_value:
                unit_index = [
                    item in urine_creatinine_str for item in other_urine_creatinine_unit].index(True)
                postprocess_urine_creatinine_value = Q(
                    float(urine_creatinine_value), other_urine_creatinine_unit[unit_index])
                postprocess_urine_creatinine_value_conv = postprocess_urine_creatinine_value.to(
                    'mg/dl')
                return (postprocess_urine_creatinine_value.magnitude, other_urine_creatinine_unit[unit_index]) if default in other_urine_creatinine_unit else (postprocess_urine_creatinine_value_conv.magnitude, default)
            # here we need to add range if available
            else:
                postprocess_urine_creatinine_value = Q(
                    float(urine_creatinine_value), 'mg/dl')
                return (postprocess_urine_creatinine_value.magnitude, default)
        else:
            return None,None

    def urine_protein_postprocess(urine_protein_str, default='mg/dl'):
        reference_dict = {'negative': 10, 'trace': 15, '1+': 30, '2+': 100,
                        '3+': 300, '4+': 1000}
        urine_protein_str=  Postprocess.get_process_value_str(
            urine_protein_str)
        prot_value = None
        for k, prot_value_ in reference_dict.items():
            if k in urine_protein_str.strip().lower():
                prot_value = prot_value_
                break
        if prot_value:
            return (prot_value, default)
        else:
            matcher = re.findall(
                r"(?:\d{1,}\.?\d*)", urine_protein_str.strip(), re.I)
            if matcher:
                prot_value = matcher[0]
                return (prot_value, default)
            else:
                return None, None
    
    def urine_volume_postprocess(urine_volume_str, default='ml/kg/hr'):
        ureg = pint.UnitRegistry()
        Q = ureg.Quantity
        urine_volume_str = Postprocess.get_process_value_str(
            urine_volume_str)
        other_urine_volume_unit = ['ml', 'ml/kg', 'ml/kg/hr']
        urine_volume_str_match = re.findall(
            r"(?:\d{1,}\.?\d*)", urine_volume_str.strip(), re.I)
        urine_volume_unit_ = re.sub(
            r"[0-9.@]", '', urine_volume_str.strip().lower())

        urine_volume_value = None
        if urine_volume_str_match:
            urine_volume_value = urine_volume_str_match[0]
            if urine_volume_value:
                postprocess_urine_volume_value = Q(
                    float(urine_volume_value), default)
                return (round(postprocess_urine_volume_value.magnitude, 2), default)
        else:
            return None, None
    
    # trop-ckmb
    def trop_postprocess(trop_str, default='ng/ml'):
        ureg = pint.UnitRegistry()
        Q = ureg.Quantity
        trop_str = str(trop_str).lower()
        trop_unit = ['ng/ml']
        other_trop_unit = ['ng/dl', 'ng/l', 'µg/l']
        trop_str_match = Postprocess.get_process_value_str(trop_str)
        trop_str_match = re.findall(
            r"(?:\d{1,}\.?\d*)", trop_str_match.strip(), re.I)

        trop_value = None
        if trop_str_match:
            trop_value = trop_str_match[0]
            if any(item in trop_str for item in trop_unit) and trop_value:
                postprocess_trop_value = Q(float(trop_value), default)
                postprocess_trop_value_conv = postprocess_trop_value.to(
                    'ng/dL')
                return (postprocess_trop_value.magnitude, default) if default == 'ng/ml' else (postprocess_trop_value_conv.magnitude, 'ng/dL')

            elif any(item in trop_str for item in other_trop_unit) and trop_value:
                unit_index = [
                    item in trop_str for item in other_trop_unit].index(True)
                postprocess_trop_value = Q(
                    float(trop_value), other_trop_unit[unit_index])
                postprocess_trop_value_conv = postprocess_trop_value.to(
                    default)
                return (postprocess_trop_value.magnitude, other_trop_unit[unit_index]) if default in other_trop_unit else (postprocess_trop_value_conv.magnitude, default)
            # here we need to add range if available
            else:
                postprocess_trop_value = Q(float(trop_value), default)
                return (postprocess_trop_value.magnitude, default)
        else:
            return None, None
    def pt_postprocess(pt_str, default = 'sec'):
        pt_str = Postprocess.get_process_value_str(pt_str)
        pt_str = str(pt_str.replace(",",".")).strip()
        # ureg = pint.UnitRegistry()
        # Q_ = ureg.Quantity
        pt_val = None
        try:
            if pt_str:
                pt_val = float(
                    re.sub(r"[^0-9.]", " ", pt_str).split(" ")[0]
                    )
                return (round(pt_val, 2), default)
            else:
                return None, None
        except ValueError as e:
            return None, None
            

    def aptt_postprocess(aptt_str, default = 'sec'):
        aptt_str = Postprocess.get_process_value_str(aptt_str)
        aptt_str = str(aptt_str.replace(",",".")).strip()
        # ureg = pint.UnitRegistry()
        # Q_ = ureg.Quantity
        aptt_val = None
        try:
            if aptt_str:
                aptt_val = float(
                    re.sub(r"[^0-9.]", " ", aptt_str).split(" ")[0]
                    )
                return (round(aptt_val, 2), default)
            else:
                return None, None
        except ValueError as e:
            return None, None

    def inr_postprocess(inr_str, default = ''):
        inr_str = Postprocess.get_process_value_str(inr_str)
        inr_str = str(inr_str.replace(",",".")).strip()
        # ureg = pint.UnitRegistry()
        # Q_ = ureg.Quantity
        inr_val = None
        try:
            if inr_str:
                inr_val = float(
                    re.sub(r"[^0-9.]", " ", inr_str).split(" ")[0]
                    )
                inr_val
                return (round(inr_val, 2), default)
            else:
                return None, None
        except ValueError as e:
            return None, None
        
    def platelets_postprocess(platelets_str, default = '/mL'):
        platelets_str = Postprocess.get_process_value_str(platelets_str)
        platelets_str = str(platelets_str).strip()
        # ureg = pint.UnitRegistry()
        # Q_ = ureg.Quantity
        platelets_val = None
        try:
            if platelets_str:
                platelets_val = int(
                    re.sub(r"[^0-9.]", " ", platelets_str).split(" ")[0]
                    )
                if 10 <= platelets_val <= 1000:
                    platelets_val *= 1000
                return (platelets_val, default)
            else:
                return None, None
        except ValueError as e:
            return None, None
        
    def gfr_postprocess(gfr_str, default='mil/min/1.73sqm'):
        try :
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            gfr_str = str(gfr_str).lower()
            gfr_unit = ['mil/min/1.73sqm']
            other_gfr_unit = []
            gfr_str_match = Postprocess.get_process_value_str(gfr_str)
            gfr_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", gfr_str_match.strip(), re.I)

            gfr_value = None
            if gfr_str_match:
                gfr_value = gfr_str_match[0]
                if any(item in gfr_str for item in gfr_unit) and gfr_value:
                    postprocess_gfr_value = Q(float(gfr_value), 'mil/min/1.73sqm')
                    return (postprocess_gfr_value.magnitude,default)

                elif any(item in gfr_str for item in other_gfr_unit) and gfr_value:
                    unit_index = [item in gfr_str for item in other_gfr_unit].index(True)
                    postprocess_gfr_value = Q(
                        float(gfr_value), other_gfr_unit[unit_index])
                    postprocess_gfr_value_conv = postprocess_gfr_value.to('mil/min/1.73sqm')
                    return (postprocess_gfr_value.magnitude, other_gfr_unit[unit_index]) if default in other_gfr_unit else (postprocess_gfr_value_conv.magnitude, 'mil/min/1.73sqm')
                # here we need to add range if available
                else:
                    postprocess_gfr_value = float(gfr_value), 'mil/min/1.73sqm'
                    return postprocess_gfr_value
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
        
    def rbc_postprocess(rbc_str, default='M/CU MM'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            rbc_str = str(rbc_str).lower()
            rbc_unit = ['M/CU MM']
            other_rbc_unit = []
            rbc_str_match = Postprocess.get_process_value_str(rbc_str)
            rbc_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", rbc_str_match.strip(), re.I)

            rbc_value = None
            if rbc_str_match:
                rbc_value = rbc_str_match[0]
                if any(item in rbc_str for item in rbc_unit) and rbc_value:
                    postprocess_rbc_value = Q(float(rbc_value), 'M/CU MM')
                    return (postprocess_rbc_value.magnitude,default) if default == 'M/CU MM' else (postprocess_rbc_value_conv.magnitude, 'mol/l')

                elif any(item in rbc_str for item in other_rbc_unit) and rbc_value:
                    unit_index = [item in rbc_str for item in other_rbc_unit].index(True)
                    postprocess_rbc_value = Q(
                        float(rbc_value), other_rbc_unit[unit_index])
                    postprocess_rbc_value_conv = postprocess_rbc_value.to('M/CU MM')
                    return (postprocess_rbc_value.magnitude, other_rbc_unit[unit_index]) if default in other_rbc_unit else (postprocess_rbc_value_conv.magnitude, 'M/CU MM')
                # here we need to add range if available
                else:
                    postprocess_rbc_value = float(rbc_value), default
                    return postprocess_rbc_value
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
        
    def direct_billirubin_postprocess(direct_billirubin_str, default='mg/dl'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            direct_billirubin_str = str(direct_billirubin_str).lower()
            direct_billirubin_unit = ['mg/dl']
            other_direct_billirubin_unit = []
            direct_billirubin_str_match = Postprocess.get_process_value_str(direct_billirubin_str)
            direct_billirubin_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", direct_billirubin_str_match.strip(), re.I)

            direct_billirubin_value = None
            if direct_billirubin_str_match:
                direct_billirubin_value = direct_billirubin_str_match[0]
                if any(item in direct_billirubin_str for item in direct_billirubin_unit) and direct_billirubin_value:
                    postprocess_direct_billirubin_value = Q(float(direct_billirubin_value), 'mg/dl')
                    return (postprocess_direct_billirubin_value.magnitude,default)

                elif any(item in direct_billirubin_str for item in other_direct_billirubin_unit) and direct_billirubin_value:
                    unit_index = [item in direct_billirubin_str for item in other_direct_billirubin_unit].index(True)
                    postprocess_direct_billirubin_value = Q(
                        float(direct_billirubin_value), other_direct_billirubin_unit[unit_index])
                    postprocess_direct_billirubin_value_conv = postprocess_direct_billirubin_value.to('mg/dl')
                    return (postprocess_direct_billirubin_value.magnitude, other_direct_billirubin_unit[unit_index]) if default in other_direct_billirubin_unit else (postprocess_direct_billirubin_value_conv.magnitude, 'mg/dl')
                # here we need to add range if available
                else:
                    postprocess_direct_billirubin_value = Q(float(direct_billirubin_value), 'mg/dl')
                    return (postprocess_direct_billirubin_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
    def hgb_postprocess(hgb_str, default='g/dl'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            hgb_str = str(hgb_str).lower()
            hgb_unit = ['g/dl']
            other_hgb_unit = []
            hgb_str_match = Postprocess.get_process_value_str(hgb_str)
            hgb_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", hgb_str_match.strip(), re.I)

            hgb_value = None
            if hgb_str_match:
                hgb_value = hgb_str_match[0]
                if any(item in hgb_str for item in hgb_unit) and hgb_value:
                    postprocess_hgb_value = Q(float(hgb_value), 'g/dl')
                    return (postprocess_hgb_value.magnitude,default)

                elif any(item in hgb_str for item in other_hgb_unit) and hgb_value:
                    unit_index = [item in hgb_str for item in other_hgb_unit].index(True)
                    postprocess_hgb_value = Q(
                        float(hgb_value), other_hgb_unit[unit_index])
                    postprocess_hgb_value_conv = postprocess_hgb_value.to('g/dl')
                    return (postprocess_hgb_value.magnitude, other_hgb_unit[unit_index]) if default in other_hgb_unit else (postprocess_hgb_value_conv.magnitude, 'g/dl')
                # here we need to add range if available
                else:
                    postprocess_hgb_value = Q(float(hgb_value), 'g/dl')
                    return (postprocess_hgb_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
        
    def alt_postprocess(alt_str, default='U/L'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            alt_str = str(alt_str).lower()
            alt_unit = ['U/L']
            other_alt_unit = []
            alt_str_match = Postprocess.get_process_value_str(alt_str)
            alt_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", alt_str_match.strip(), re.I)

            alt_value = None
            if alt_str_match:
                alt_value = alt_str_match[0]
                if any(item in alt_str for item in alt_unit) and alt_value:
                    postprocess_alt_value = Q(float(alt_value), 'U/L')
                    return (postprocess_alt_value.magnitude,default)

                elif any(item in alt_str for item in other_alt_unit) and alt_value:
                    unit_index = [item in alt_str for item in other_alt_unit].index(True)
                    postprocess_alt_value = Q(
                        float(alt_value), other_alt_unit[unit_index])
                    postprocess_alt_value_conv = postprocess_alt_value.to('U/L')
                    return (postprocess_alt_value.magnitude, other_alt_unit[unit_index]) if default in other_alt_unit else (postprocess_alt_value_conv.magnitude, 'U/L')
                # here we need to add range if available
                else:
                    postprocess_alt_value = Q(float(alt_value), 'U/L')
                    return (postprocess_alt_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
    def ast_postprocess(ast_str, default='U/L'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            ast_str = str(ast_str).lower()
            ast_unit = ['U/L']
            other_ast_unit = []
            ast_str_match = Postprocess.get_process_value_str(ast_str)
            ast_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", ast_str_match.strip(), re.I)

            ast_value = None
            if ast_str_match:
                ast_value = ast_str_match[0]
                if any(item in ast_str for item in ast_unit) and ast_value:
                    postprocess_ast_value = Q(float(ast_value), 'U/L')
                    return (postprocess_ast_value.magnitude,default)

                elif any(item in ast_str for item in other_ast_unit) and ast_value:
                    unit_index = [item in ast_str for item in other_ast_unit].index(True)
                    postprocess_ast_value = Q(
                        float(ast_value), other_ast_unit[unit_index])
                    postprocess_ast_value_conv = postprocess_ast_value.to('U/L')
                    return (postprocess_ast_value.magnitude, other_ast_unit[unit_index]) if default in other_ast_unit else (postprocess_ast_value_conv.magnitude, 'U/L')
                # here we need to add range if available
                else:
                    postprocess_ast_value = Q(float(ast_value), 'U/L')
                    return (postprocess_ast_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
        
    def alkaline_phosphate_postprocess(alkaline_phosphate_str, default='U/L'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            alkaline_phosphate_str = str(alkaline_phosphate_str).lower()
            alkaline_phosphate_unit = ['U/L']
            other_alkaline_phosphate_unit = []
            alkaline_phosphate_str_match = Postprocess.get_process_value_str(alkaline_phosphate_str)
            alkaline_phosphate_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", alkaline_phosphate_str_match.strip(), re.I)

            alkaline_phosphate_value = None
            if alkaline_phosphate_str_match:
                alkaline_phosphate_value = alkaline_phosphate_str_match[0]
                if any(item in alkaline_phosphate_str for item in alkaline_phosphate_unit) and alkaline_phosphate_value:
                    postprocess_alkaline_phosphate_value = Q(float(alkaline_phosphate_value), 'U/L')
                    return (postprocess_alkaline_phosphate_value.magnitude,default)

                elif any(item in alkaline_phosphate_str for item in other_alkaline_phosphate_unit) and alkaline_phosphate_value:
                    unit_index = [item in alkaline_phosphate_str for item in other_alkaline_phosphate_unit].index(True)
                    postprocess_alkaline_phosphate_value = Q(
                        float(alkaline_phosphate_value), other_alkaline_phosphate_unit[unit_index])
                    postprocess_alkaline_phosphate_value_conv = postprocess_alkaline_phosphate_value.to('U/L')
                    return (postprocess_alkaline_phosphate_value.magnitude, other_alkaline_phosphate_unit[unit_index]) if default in other_alkaline_phosphate_unit else (postprocess_alkaline_phosphate_value_conv.magnitude, 'U/L')
                # here we need to add range if available
                else:
                    postprocess_alkaline_phosphate_value = Q(float(alkaline_phosphate_value), 'U/L')
                    return (postprocess_alkaline_phosphate_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
        
    def ggt_postprocess(ggt_str, default='U/L'):
        try :
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            ggt_str = str(ggt_str).lower()
            ggt_unit = ['U/L']
            other_ggt_unit = []
            ggt_str_match = Postprocess.get_process_value_str(ggt_str)
            ggt_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", ggt_str_match.strip(), re.I)

            ggt_value = None
            if ggt_str_match:
                ggt_value = ggt_str_match[0]
                if any(item in ggt_str for item in ggt_unit) and ggt_value:
                    postprocess_ggt_value = Q(float(ggt_value), 'U/L')
                    return (postprocess_ggt_value.magnitude,default) 

                elif any(item in ggt_str for item in other_ggt_unit) and ggt_value:
                    unit_index = [item in ggt_str for item in other_ggt_unit].index(True)
                    postprocess_ggt_value = Q(
                        float(ggt_value), other_ggt_unit[unit_index])
                    postprocess_ggt_value_conv = postprocess_ggt_value.to('U/L')
                    return (postprocess_ggt_value.magnitude, other_ggt_unit[unit_index]) if default in other_ggt_unit else (postprocess_ggt_value_conv.magnitude, 'U/L')
                # here we need to add range if available
                else:
                    postprocess_ggt_value = Q(float(ggt_value), 'U/L')
                    return (postprocess_ggt_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
            
    def chloride_postprocess(chloride_str, default='mmo/L'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            chloride_str = str(chloride_str).lower()
            chloride_unit = ['U/L']
            other_chloride_unit = []
            chloride_str_match = Postprocess.get_process_value_str(chloride_str)
            chloride_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", chloride_str_match.strip(), re.I)

            chloride_value = None
            if chloride_str_match:
                chloride_value = chloride_str_match[0]
                if any(item in chloride_str for item in chloride_unit) and chloride_value:
                    postprocess_chloride_value = Q(float(chloride_value), 'U/L')
                    return (postprocess_chloride_value.magnitude,default)

                elif any(item in chloride_str for item in other_chloride_unit) and chloride_value:
                    unit_index = [item in chloride_str for item in other_chloride_unit].index(True)
                    postprocess_chloride_value = Q(
                        float(chloride_value), other_chloride_unit[unit_index])
                    postprocess_chloride_value_conv = postprocess_chloride_value.to('U/L')
                    return (postprocess_chloride_value.magnitude, other_chloride_unit[unit_index]) if default in other_chloride_unit else (postprocess_chloride_value_conv.magnitude, 'U/L')
                # here we need to add range if available
                else:
                    postprocess_chloride_value = float(chloride_value), default
                    return postprocess_chloride_value
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None

    def blood_alcohol_level_postprocess(blood_alcohol_level_str, default='%'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            blood_alcohol_level_str = str(blood_alcohol_level_str).lower()
            blood_alcohol_level_unit = ['%']
            other_blood_alcohol_level_unit = []
            blood_alcohol_level_str_match = Postprocess.get_process_value_str(blood_alcohol_level_str)
            blood_alcohol_level_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", blood_alcohol_level_str_match.strip(), re.I)

            blood_alcohol_level_value = None
            if blood_alcohol_level_str_match:
                blood_alcohol_level_value = blood_alcohol_level_str_match[0]
                if any(item in blood_alcohol_level_str for item in blood_alcohol_level_unit) and blood_alcohol_level_value:
                    postprocess_blood_alcohol_level_value = Q(float(blood_alcohol_level_value), '%')
                    return (postprocess_blood_alcohol_level_value.magnitude,default) 

                elif any(item in blood_alcohol_level_str for item in other_blood_alcohol_level_unit) and blood_alcohol_level_value:
                    unit_index = [item in blood_alcohol_level_str for item in other_blood_alcohol_level_unit].index(True)
                    postprocess_blood_alcohol_level_value = Q(
                        float(blood_alcohol_level_value), other_blood_alcohol_level_unit[unit_index])
                    postprocess_blood_alcohol_level_value_conv = postprocess_blood_alcohol_level_value.to('%')
                    return (postprocess_blood_alcohol_level_value.magnitude, other_blood_alcohol_level_unit[unit_index]) if default in other_blood_alcohol_level_unit else (postprocess_blood_alcohol_level_value_conv.magnitude, '%')
                # here we need to add range if available
                else:
                    postprocess_blood_alcohol_level_value = Q(float(blood_alcohol_level_value), '%')
                    return (postprocess_blood_alcohol_level_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
    def acetaminophen_level_postprocess(acetaminophen_level_str, default='mcg/ml'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            acetaminophen_level_str = str(acetaminophen_level_str).lower()
            acetaminophen_level_unit = ['mcg/ml']
            other_acetaminophen_level_unit = []
            acetaminophen_level_str_match = Postprocess.get_process_value_str(acetaminophen_level_str)
            acetaminophen_level_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", acetaminophen_level_str_match.strip(), re.I)

            acetaminophen_level_value = None
            if acetaminophen_level_str_match:
                acetaminophen_level_value = acetaminophen_level_str_match[0]
                if any(item in acetaminophen_level_str for item in acetaminophen_level_unit) and acetaminophen_level_value:
                    postprocess_acetaminophen_level_value = Q(float(acetaminophen_level_value), 'mcg/ml')
                    return (postprocess_acetaminophen_level_value.magnitude,default) 

                elif any(item in acetaminophen_level_str for item in other_acetaminophen_level_unit) and acetaminophen_level_value:
                    unit_index = [item in acetaminophen_level_str for item in other_acetaminophen_level_unit].index(True)
                    postprocess_acetaminophen_level_value = Q(
                        float(acetaminophen_level_value), other_acetaminophen_level_unit[unit_index])
                    postprocess_acetaminophen_level_value_conv = postprocess_acetaminophen_level_value.to('mcg/ml')
                    return (postprocess_acetaminophen_level_value.magnitude, other_acetaminophen_level_unit[unit_index]) if default in other_acetaminophen_level_unit else (postprocess_acetaminophen_level_value_conv.magnitude, 'mcg/ml')
                # here we need to add range if available
                else:
                    postprocess_acetaminophen_level_value = float(acetaminophen_level_value), default
                    return postprocess_acetaminophen_level_value
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
    
    def vitamin_B1_postprocess(vitamin_B1_str, default='nmol/L'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            vitamin_B1_str = str(vitamin_B1_str).lower()
            vitamin_B1_unit = ['nmol/L']
            other_vitamin_B1_unit = []
            vitamin_B1_str_match = Postprocess.get_process_value_str(vitamin_B1_str)
            vitamin_B1_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", vitamin_B1_str_match.strip(), re.I)

            vitamin_B1_value = None
            if vitamin_B1_str_match:
                vitamin_B1_value = vitamin_B1_str_match[0]
                if any(item in vitamin_B1_str for item in vitamin_B1_unit) and vitamin_B1_value:
                    postprocess_vitamin_B1_value = Q(float(vitamin_B1_value), 'nmol/L')
                    return (postprocess_vitamin_B1_value.magnitude,default)

                elif any(item in vitamin_B1_str for item in other_vitamin_B1_unit) and vitamin_B1_value:
                    unit_index = [item in vitamin_B1_str for item in other_vitamin_B1_unit].index(True)
                    postprocess_vitamin_B1_value = Q(
                        float(vitamin_B1_value), other_vitamin_B1_unit[unit_index])
                    postprocess_vitamin_B1_value_conv = postprocess_vitamin_B1_value.to('nmol/L')
                    return (postprocess_vitamin_B1_value.magnitude, other_vitamin_B1_unit[unit_index]) if default in other_vitamin_B1_unit else (postprocess_vitamin_B1_value_conv.magnitude, 'nmol/L')
                # here we need to add range if available
                else:
                    postprocess_vitamin_B1_value = Q(float(vitamin_B1_value), 'nmol/L')
                    return (postprocess_vitamin_B1_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
    def vitamin_B12_postprocess(vitamin_B12_str, default='pg/ml'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            vitamin_B12_str = str(vitamin_B12_str).lower()
            vitamin_B12_unit = ['pg/ml']
            other_vitamin_B12_unit = ['pmol/L']
            vitamin_B12_str_match = Postprocess.get_process_value_str(vitamin_B12_str)
            vitamin_B12_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", vitamin_B12_str_match.strip(), re.I)

            vitamin_B12_value = None
            if vitamin_B12_str_match:
                vitamin_B12_value = vitamin_B12_str_match[0]
                if any(item in vitamin_B12_str for item in vitamin_B12_unit) and vitamin_B12_value:
                    postprocess_vitamin_B12_value = Q(float(vitamin_B12_value), 'pg/ml')
                    postprocess_vitamin_B12_value_conv = postprocess_vitamin_B12_value.to('pmol/L')
                    return (postprocess_vitamin_B12_value.magnitude,default) if default == 'pg/ml' else (postprocess_vitamin_B12_value_conv.magnitude, 'pmol/L')

                elif any(item in vitamin_B12_str for item in other_vitamin_B12_unit) and vitamin_B12_value:
                    unit_index = [item in vitamin_B12_str for item in other_vitamin_B12_unit].index(True)
                    postprocess_vitamin_B12_value = Q(
                        float(vitamin_B12_value), other_vitamin_B12_unit[unit_index])
                    postprocess_vitamin_B12_value_conv = postprocess_vitamin_B12_value.to('pg/ml')
                    return (postprocess_vitamin_B12_value.magnitude, other_vitamin_B12_unit[unit_index]) if default in other_vitamin_B12_unit else (postprocess_vitamin_B12_value_conv.magnitude, 'pg/ml')
                # here we need to add range if available
                else:
                    postprocess_vitamin_B12_value = Q(float(vitamin_B12_value), 'pg/ml')
                    return (postprocess_vitamin_B12_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
        
    def ammonia_level_postprocess(ammonia_level_str, default='umol/L'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            ammonia_level_str = str(ammonia_level_str).lower()
            ammonia_level_unit = ['umol/L']
            other_ammonia_level_unit = []
            ammonia_level_str_match = Postprocess.get_process_value_str(ammonia_level_str)
            ammonia_level_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", ammonia_level_str_match.strip(), re.I)

            ammonia_level_value = None
            if ammonia_level_str_match:
                ammonia_level_value = ammonia_level_str_match[0]
                if any(item in ammonia_level_str for item in ammonia_level_unit) and ammonia_level_value:
                    postprocess_ammonia_level_value = Q(float(ammonia_level_value), 'umol/L')
                    return (postprocess_ammonia_level_value.magnitude,default) 
                elif any(item in ammonia_level_str for item in other_ammonia_level_unit) and ammonia_level_value:
                    unit_index = [item in ammonia_level_str for item in other_ammonia_level_unit].index(True)
                    postprocess_ammonia_level_value = Q(
                        float(ammonia_level_value), other_ammonia_level_unit[unit_index])
                    postprocess_ammonia_level_value_conv = postprocess_ammonia_level_value.to('umol/L')
                    return (postprocess_ammonia_level_value.magnitude, other_ammonia_level_unit[unit_index]) if default in other_ammonia_level_unit else (postprocess_ammonia_level_value_conv.magnitude, 'umol/L')
                # here we need to add range if available
                else:
                    postprocess_ammonia_level_value = Q(float(ammonia_level_value), 'umol/L')
                    return (postprocess_ammonia_level_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
        
    def albumin_postprocess(albumin_str, default='g/dl'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            albumin_str = str(albumin_str).lower()
            albumin_unit = ['g/dl']
            other_albumin_unit = []
            albumin_str_match = Postprocess.get_process_value_str(albumin_str)
            albumin_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", albumin_str_match.strip(), re.I)

            albumin_value = None
            if albumin_str_match:
                albumin_value = albumin_str_match[0]
                if any(item in albumin_str for item in albumin_unit) and albumin_value:
                    postprocess_albumin_value = Q(float(albumin_value), 'g/dl')
          
                    return (postprocess_albumin_value.magnitude,default) 

                elif any(item in albumin_str for item in other_albumin_unit) and albumin_value:
                    unit_index = [item in albumin_str for item in other_albumin_unit].index(True)
                    postprocess_albumin_value = Q(
                        float(albumin_value), other_albumin_unit[unit_index])
                    postprocess_albumin_value_conv = postprocess_albumin_value.to('g/dl')
                    return (postprocess_albumin_value.magnitude, other_albumin_unit[unit_index]) if default in other_albumin_unit else (postprocess_albumin_value_conv.magnitude, 'g/dl')
                # here we need to add range if available
                else:
                    postprocess_albumin_value = Q(float(albumin_value), 'g/dl')
                    return (postprocess_albumin_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
        
    def serum_cortisol_postprocess(serum_cortisol_str, default='mcg/dl'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            serum_cortisol_str = str(serum_cortisol_str).lower()
            serum_cortisol_unit = ['mcg/dl']
            other_serum_cortisol_unit = []
            serum_cortisol_str_match = Postprocess.get_process_value_str(serum_cortisol_str)
            serum_cortisol_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", serum_cortisol_str_match.strip(), re.I)

            serum_cortisol_value = None
            if serum_cortisol_str_match:
                serum_cortisol_value = serum_cortisol_str_match[0]
                if any(item in serum_cortisol_str for item in serum_cortisol_unit) and serum_cortisol_value:
                    postprocess_serum_cortisol_value = Q(float(serum_cortisol_value), 'mcg/dl')
                    return (postprocess_serum_cortisol_value.magnitude,default)

                elif any(item in serum_cortisol_str for item in other_serum_cortisol_unit) and serum_cortisol_value:
                    unit_index = [item in serum_cortisol_str for item in other_serum_cortisol_unit].index(True)
                    postprocess_serum_cortisol_value = Q(
                        float(serum_cortisol_value), other_serum_cortisol_unit[unit_index])
                    postprocess_serum_cortisol_value_conv = postprocess_serum_cortisol_value.to('mcg/dl')
                    return (postprocess_serum_cortisol_value.magnitude, other_serum_cortisol_unit[unit_index]) if default in other_serum_cortisol_unit else (postprocess_serum_cortisol_value_conv.magnitude, 'mcg/dl')
                # here we need to add range if available
                else:
                    postprocess_serum_cortisol_value = float(serum_cortisol_value)
                    return (postprocess_serum_cortisol_value, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
        
    def hematocrit_test_postprocess(hematocrit_test_str, default='%'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            hematocrit_test_str = str(hematocrit_test_str).lower()
            hematocrit_test_unit = ['%']
            other_hematocrit_test_unit = []
            hematocrit_test_str_match = Postprocess.get_process_value_str(hematocrit_test_str)
            hematocrit_test_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", hematocrit_test_str_match.strip(), re.I)

            hematocrit_test_value = None
            if hematocrit_test_str_match:
                hematocrit_test_value = hematocrit_test_str_match[0]
                if any(item in hematocrit_test_str for item in hematocrit_test_unit) and hematocrit_test_value:
                    postprocess_hematocrit_test_value = Q(float(hematocrit_test_value), '%')
            
                    return (postprocess_hematocrit_test_value.magnitude,default)

                elif any(item in hematocrit_test_str for item in other_hematocrit_test_unit) and hematocrit_test_value:
                    unit_index = [item in hematocrit_test_str for item in other_hematocrit_test_unit].index(True)
                    postprocess_hematocrit_test_value = Q(
                        float(hematocrit_test_value), other_hematocrit_test_unit[unit_index])
                    postprocess_hematocrit_test_value_conv = postprocess_hematocrit_test_value.to('%')
                    return (postprocess_hematocrit_test_value.magnitude, other_hematocrit_test_unit[unit_index]) if default in other_hematocrit_test_unit else (postprocess_hematocrit_test_value_conv.magnitude, '%')
                # here we need to add range if available
                else:
                    postprocess_hematocrit_test_value = Q(float(hematocrit_test_value), '%')
                    return (postprocess_hematocrit_test_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None 
    def tsh_postprocess(tsh_str, default='mclU/ml'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            tsh_str = str(tsh_str).lower()
            tsh_unit = ['mclU/ml']
            other_tsh_unit = []
            tsh_str_match = Postprocess.get_process_value_str(tsh_str)
            tsh_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", tsh_str_match.strip(), re.I)

            tsh_value = None
            if tsh_str_match:
                tsh_value = tsh_str_match[0]
                if any(item in tsh_str for item in tsh_unit) and tsh_value:
                    postprocess_tsh_value = Q(float(tsh_value), 'mclU/ml')
                    return (postprocess_tsh_value.magnitude,default) 

                elif any(item in tsh_str for item in other_tsh_unit) and tsh_value:
                    unit_index = [item in tsh_str for item in other_tsh_unit].index(True)
                    postprocess_tsh_value = Q(
                        float(tsh_value), other_tsh_unit[unit_index])
                    postprocess_tsh_value_conv = postprocess_tsh_value.to('mclU/ml')
                    return (postprocess_tsh_value.magnitude, other_tsh_unit[unit_index]) if default in other_tsh_unit else (postprocess_tsh_value_conv.magnitude, 'mclU/ml')
                # here we need to add range if available
                else:
                    postprocess_tsh_value = float(tsh_value)
                    return (postprocess_tsh_value, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
        
    def ft4_postprocess(ft4_str, default='ng/dl'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            ft4_str = str(ft4_str).lower()
            ft4_unit = ['ng/dl']
            other_ft4_unit = []
            ft4_str_match = Postprocess.get_process_value_str(ft4_str)
            ft4_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", ft4_str_match.strip(), re.I)

            ft4_value = None
            if ft4_str_match:
                ft4_value = ft4_str_match[0]
                if any(item in ft4_str for item in ft4_unit) and ft4_value:
                    postprocess_ft4_value = Q(float(ft4_value), 'ng/dl')
                    return (postprocess_ft4_value.magnitude,default) 

                elif any(item in ft4_str for item in other_ft4_unit) and ft4_value:
                    unit_index = [item in ft4_str for item in other_ft4_unit].index(True)
                    postprocess_ft4_value = Q(
                        float(ft4_value), other_ft4_unit[unit_index])
                    postprocess_ft4_value_conv = postprocess_ft4_value.to('ng/dl')
                    return (postprocess_ft4_value.magnitude, other_ft4_unit[unit_index]) if default in other_ft4_unit else (postprocess_ft4_value_conv.magnitude, 'ng/dl')
                # here we need to add range if available
                else:
                    postprocess_ft4_value = Q(float(ft4_value), 'ng/dl')
                    return (postprocess_ft4_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
        
    def t4_postprocess(t4_str, default='mcg/dl'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            t4_str = str(t4_str).lower()
            t4_unit = ['mcg/dl']
            other_t4_unit = []
            t4_str_match = Postprocess.get_process_value_str(t4_str)
            t4_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", t4_str_match.strip(), re.I)

            t4_value = None
            if t4_str_match:
                t4_value = t4_str_match[0]
                if any(item in t4_str for item in t4_unit) and t4_value:
                    postprocess_t4_value = Q(float(t4_value), 'mcg/dl')
                    return (postprocess_t4_value.magnitude,default) 

                elif any(item in t4_str for item in other_t4_unit) and t4_value:
                    unit_index = [item in t4_str for item in other_t4_unit].index(True)
                    postprocess_t4_value = Q(
                        float(t4_value), other_t4_unit[unit_index])
                    postprocess_t4_value_conv = postprocess_t4_value.to('mcg/dl')
                    return (postprocess_t4_value.magnitude, other_t4_unit[unit_index]) if default in other_t4_unit else (postprocess_t4_value_conv.magnitude, 'mcg/dl')
                # here we need to add range if available
                else:
                    postprocess_t4_value = float(t4_value)
                    return (postprocess_t4_value, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
    def ft3_postprocess(ft3_str, default='pg/ml'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            ft3_str = str(ft3_str).lower()
            ft3_unit = ['pg/ml']
            other_ft3_unit = []
            ft3_str_match = Postprocess.get_process_value_str(ft3_str)
            ft3_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", ft3_str_match.strip(), re.I)

            ft3_value = None
            if ft3_str_match:
                ft3_value = ft3_str_match[0]
                if any(item in ft3_str for item in ft3_unit) and ft3_value:
                    postprocess_ft3_value = Q(float(ft3_value), 'pg/ml')
                    return (postprocess_ft3_value.magnitude,default) 

                elif any(item in ft3_str for item in other_ft3_unit) and ft3_value:
                    unit_index = [item in ft3_str for item in other_ft3_unit].index(True)
                    postprocess_ft3_value = Q(
                        float(ft3_value), other_ft3_unit[unit_index])
                    postprocess_ft3_value_conv = postprocess_ft3_value.to('pg/ml')
                    return (postprocess_ft3_value.magnitude, other_ft3_unit[unit_index]) if default in other_ft3_unit else (postprocess_ft3_value_conv.magnitude, 'pg/ml')
                # here we need to add range if available
                else:
                    postprocess_ft3_value = Q(float(ft3_value), 'pg/ml')
                    return (postprocess_ft3_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
    def rel_lymph_postprocess(lymph_str, default='%'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            lymph_str = str(lymph_str).lower()
            lymph_unit = ['%']
            other_lymph_unit = []
            lymph_str_match = Postprocess.get_process_value_str(lymph_str)
            lymph_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", lymph_str_match.strip(), re.I)

            lymph_value = None
            if lymph_str_match:
                lymph_value = lymph_str_match[0]
                if any(item in lymph_str for item in lymph_unit) and lymph_value:
                    postprocess_lymph_value = Q(float(lymph_value), '%')
                    return (postprocess_lymph_value.magnitude,default) 

                elif any(item in lymph_str for item in other_lymph_unit) and lymph_value:
                    unit_index = [item in lymph_str for item in other_lymph_unit].index(True)
                    postprocess_lymph_value = Q(
                        float(lymph_value), other_lymph_unit[unit_index])
                    postprocess_lymph_value_conv = postprocess_lymph_value.to('%')
                    return (postprocess_lymph_value.magnitude, other_lymph_unit[unit_index]) if default in other_lymph_unit else (postprocess_lymph_value_conv.magnitude, '%')
                # here we need to add range if available
                else:
                    postprocess_lymph_value = Q(float(lymph_value), '%')
                    return (postprocess_lymph_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
    def abs_lymph_postprocess(lymph_str, default='k/mcL'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            lymph_str = str(lymph_str).lower()
            lymph_unit = ['K/u mm']
            other_lymph_unit = []
            lymph_str_match = Postprocess.get_process_value_str(lymph_str)
            lymph_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", lymph_str_match.strip(), re.I)

            lymph_value = None
            if lymph_str_match:
                lymph_value = lymph_str_match[0]
                if any(item in lymph_str for item in lymph_unit) and lymph_value:
                    postprocess_lymph_value = Q(float(lymph_value), 'K/u mm')
                    return (postprocess_lymph_value.magnitude,default) 

                elif any(item in lymph_str for item in other_lymph_unit) and lymph_value:
                    unit_index = [item in lymph_str for item in other_lymph_unit].index(True)
                    postprocess_lymph_value = Q(
                        float(lymph_value), other_lymph_unit[unit_index])
                    postprocess_lymph_value_conv = postprocess_lymph_value.to('K/u mm')
                    return (postprocess_lymph_value.magnitude, other_lymph_unit[unit_index]) if default in other_lymph_unit else (postprocess_lymph_value_conv.magnitude, 'K/u mm')
                # here we need to add range if available
                else:
                    postprocess_lymph_value = Q(float(lymph_value), 'K/u mm')
                    return (postprocess_lymph_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
        
    def lld_postprocess(lld_str, default='U/L'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            lld_str = str(lld_str).lower()
            lld_unit = ['U/L']
            other_lld_unit = []
            lld_str_match = Postprocess.get_process_value_str(lld_str)
            lld_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", lld_str_match.strip(), re.I)

            lld_value = None
            if lld_str_match:
                lld_value = lld_str_match[0]
                if any(item in lld_str for item in lld_unit) and lld_value:
                    postprocess_lld_value = Q(float(lld_value), 'U/L')
                    return (postprocess_lld_value.magnitude,default) 

                elif any(item in lld_str for item in other_lld_unit) and lld_value:
                    unit_index = [item in lld_str for item in other_lld_unit].index(True)
                    postprocess_lld_value = Q(
                        float(lld_value), other_lld_unit[unit_index])
                    postprocess_lld_value_conv = postprocess_lld_value.to('U/L')
                    return (postprocess_lld_value.magnitude, other_lld_unit[unit_index]) if default in other_lld_unit else (postprocess_lld_value_conv.magnitude, 'U/L')
                # here we need to add range if available
                else:
                    postprocess_lld_value = Q(float(lld_value), 'U/L')
                    return (postprocess_lld_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
    def uric_acid_postprocess(uric_acid_str, default='mg/dl'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            uric_acid_str = str(uric_acid_str).lower()
            uric_acid_unit = ['mg/dl']
            other_uric_acid_unit = []
            uric_acid_str_match = Postprocess.get_process_value_str(uric_acid_str)
            uric_acid_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", uric_acid_str_match.strip(), re.I)

            uric_acid_value = None
            if uric_acid_str_match:
                uric_acid_value = uric_acid_str_match[0]
                if any(item in uric_acid_str for item in uric_acid_unit) and uric_acid_value:
                    postprocess_uric_acid_value = Q(float(uric_acid_value), 'mg/dl')
                    return (postprocess_uric_acid_value.magnitude,default) 

                elif any(item in uric_acid_str for item in other_uric_acid_unit) and uric_acid_value:
                    unit_index = [item in uric_acid_str for item in other_uric_acid_unit].index(True)
                    postprocess_uric_acid_value = Q(
                        float(uric_acid_value), other_uric_acid_unit[unit_index])
                    postprocess_uric_acid_value_conv = postprocess_uric_acid_value.to('mg/dl')
                    return (postprocess_uric_acid_value.magnitude, other_uric_acid_unit[unit_index]) if default in other_uric_acid_unit else (postprocess_uric_acid_value_conv.magnitude, 'mg/dl')
                # here we need to add range if available
                else:
                    postprocess_uric_acid_value = Q(float(uric_acid_value), 'mg/dl')
                    return (postprocess_uric_acid_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
    def paO2_postprocess(paO2_str, default='mmHg'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            paO2_str = str(paO2_str).lower()
            paO2_unit = ['mmHg']
            other_paO2_unit = []
            paO2_str_match = Postprocess.get_process_value_str(paO2_str)
            paO2_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", paO2_str_match.strip(), re.I)

            paO2_value = None
            if paO2_str_match:
                paO2_value = paO2_str_match[0]
                if any(item in paO2_str for item in paO2_unit) and paO2_value:
                    postprocess_paO2_value = Q(float(paO2_value), 'mmHg')
                    return (postprocess_paO2_value.magnitude,default) 

                elif any(item in paO2_str for item in other_paO2_unit) and paO2_value:
                    unit_index = [item in paO2_str for item in other_paO2_unit].index(True)
                    postprocess_paO2_value = Q(
                        float(paO2_value), other_paO2_unit[unit_index])
                    postprocess_paO2_value_conv = postprocess_paO2_value.to('mmHg')
                    return (postprocess_paO2_value.magnitude, other_paO2_unit[unit_index]) if default in other_paO2_unit else (postprocess_paO2_value_conv.magnitude, 'mmHg')
                # here we need to add range if available
                else:
                    postprocess_paO2_value = Q(float(paO2_value), 'mmHg')
                    return (postprocess_paO2_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
    def paCO2_postprocess(paCO2_str, default='mmHg'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            paCO2_str = str(paCO2_str).lower()
            paCO2_unit = ['mmHg']
            other_paCO2_unit = []
            paCO2_str_match = Postprocess.get_process_value_str(paCO2_str)
            paCO2_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", paCO2_str_match.strip(), re.I)

            paCO2_value = None
            if paCO2_str_match:
                paCO2_value = paCO2_str_match[0]
                if any(item in paCO2_str for item in paCO2_unit) and paCO2_value:
                    postprocess_paCO2_value = Q(float(paCO2_value), 'mmHg')
                    return (postprocess_paCO2_value.magnitude,default) 

                elif any(item in paCO2_str for item in other_paCO2_unit) and paCO2_value:
                    unit_index = [item in paCO2_str for item in other_paCO2_unit].index(True)
                    postprocess_paCO2_value = Q(
                        float(paCO2_value), other_paCO2_unit[unit_index])
                    postprocess_paCO2_value_conv = postprocess_paCO2_value.to('mmHg')
                    return (postprocess_paCO2_value.magnitude, other_paCO2_unit[unit_index]) if default in other_paCO2_unit else (postprocess_paCO2_value_conv.magnitude, 'mmHg')
                # here we need to add range if available
                else:
                    postprocess_paCO2_value = Q(float(paCO2_value), 'mmHg')
                    return (postprocess_paCO2_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
    def HCO3_postprocess(HCO3_str, default='mEq/L'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            HCO3_str = str(HCO3_str).lower()
            HCO3_unit = ['mEq/L']
            other_HCO3_unit = []
            HCO3_str_match = Postprocess.get_process_value_str(HCO3_str)
            HCO3_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", HCO3_str_match.strip(), re.I)

            HCO3_value = None
            if HCO3_str_match:
                HCO3_value = HCO3_str_match[0]
                if any(item in HCO3_str for item in HCO3_unit) and HCO3_value:
                    postprocess_HCO3_value = Q(float(HCO3_value), 'mEq/L')
                    return (postprocess_HCO3_value.magnitude,default) 

                elif any(item in HCO3_str for item in other_HCO3_unit) and HCO3_value:
                    unit_index = [item in HCO3_str for item in other_HCO3_unit].index(True)
                    postprocess_HCO3_value = Q(
                        float(HCO3_value), other_HCO3_unit[unit_index])
                    postprocess_HCO3_value_conv = postprocess_HCO3_value.to('mEq/L')
                    return (postprocess_HCO3_value.magnitude, other_HCO3_unit[unit_index]) if default in other_HCO3_unit else (postprocess_HCO3_value_conv.magnitude, 'mEq/L')
                # here we need to add range if available
                else:
                    postprocess_HCO3_value = float(HCO3_value)
                    return (postprocess_HCO3_value, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
    def glucose_postprocess(glucose_str, default='mg/dl'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            glucose_str = str(glucose_str).lower()
            glucose_unit = ['mg/dl']
            other_glucose_unit = []
            glucose_str_match = Postprocess.get_process_value_str(glucose_str)
            glucose_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", glucose_str_match.strip(), re.I)

            glucose_value = None
            if glucose_str_match:
                glucose_value = glucose_str_match[0]
                if any(item in glucose_str for item in glucose_unit) and glucose_value:
                    postprocess_glucose_value = Q(float(glucose_value), 'mg/dl')
                    return (postprocess_glucose_value.magnitude,default) 

                elif any(item in glucose_str for item in other_glucose_unit) and glucose_value:
                    unit_index = [item in glucose_str for item in other_glucose_unit].index(True)
                    postprocess_glucose_value = Q(
                        float(glucose_value), other_glucose_unit[unit_index])
                    postprocess_glucose_value_conv = postprocess_glucose_value.to('mg/dl')
                    return (postprocess_glucose_value.magnitude, other_glucose_unit[unit_index]) if default in other_glucose_unit else (postprocess_glucose_value_conv.magnitude, 'mg/dl')
                # here we need to add range if available
                else:
                    postprocess_glucose_value = Q(float(glucose_value), 'mg/dl')
                    return (postprocess_glucose_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
    def calcium_postprocess(calcium_str, default='mg/dl'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            calcium_str = str(calcium_str).lower()
            calcium_unit = ['mg/dl']
            other_calcium_unit = []
            calcium_str_match = Postprocess.get_process_value_str(calcium_str)
            calcium_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", calcium_str_match.strip(), re.I)

            calcium_value = None
            if calcium_str_match:
                calcium_value = calcium_str_match[0]
                if any(item in calcium_str for item in calcium_unit) and calcium_value:
                    postprocess_calcium_value = Q(float(calcium_value), 'mg/dl')
                    return (postprocess_calcium_value.magnitude,default) 

                elif any(item in calcium_str for item in other_calcium_unit) and calcium_value:
                    unit_index = [item in calcium_str for item in other_calcium_unit].index(True)
                    postprocess_calcium_value = Q(
                        float(calcium_value), other_calcium_unit[unit_index])
                    postprocess_calcium_value_conv = postprocess_calcium_value.to('mg/dl')
                    return (postprocess_calcium_value.magnitude, other_calcium_unit[unit_index]) if default in other_calcium_unit else (postprocess_calcium_value_conv.magnitude, 'mg/dl')
                # here we need to add range if available
                else:
                    postprocess_calcium_value = Q(float(calcium_value), 'mg/dl')
                    return (postprocess_calcium_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
    def magnesium_postprocess(magnesium_str, default='mg/dl'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            magnesium_str = str(magnesium_str).lower()
            magnesium_unit = ['mg/dl']
            other_magnesium_unit = []
            magnesium_str_match = Postprocess.get_process_value_str(magnesium_str)
            magnesium_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", magnesium_str_match.strip(), re.I)

            magnesium_value = None
            if magnesium_str_match:
                magnesium_value = magnesium_str_match[0]
                if any(item in magnesium_str for item in magnesium_unit) and magnesium_value:
                    postprocess_magnesium_value = Q(float(magnesium_value), 'mg/dl')
                    return (postprocess_magnesium_value.magnitude,default) 

                elif any(item in magnesium_str for item in other_magnesium_unit) and magnesium_value:
                    unit_index = [item in magnesium_str for item in other_magnesium_unit].index(True)
                    postprocess_magnesium_value = Q(
                        float(magnesium_value), other_magnesium_unit[unit_index])
                    postprocess_magnesium_value_conv = postprocess_magnesium_value.to('mg/dl')
                    return (postprocess_magnesium_value.magnitude, other_magnesium_unit[unit_index]) if default in other_magnesium_unit else (postprocess_magnesium_value_conv.magnitude, 'mg/dl')
                # here we need to add range if available
                else:
                    postprocess_magnesium_value = Q(float(magnesium_value), 'mg/dl')
                    return (postprocess_magnesium_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
    def phosphate_postprocess(phosphate_str, default='U/l'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            phosphate_str = str(phosphate_str).lower()
            phosphate_unit = ['U/l']
            other_phosphate_unit = []
            phosphate_str_match = Postprocess.get_process_value_str(phosphate_str)
            phosphate_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", phosphate_str_match.strip(), re.I)

            phosphate_value = None
            if phosphate_str_match:
                phosphate_value = phosphate_str_match[0]
                if any(item in phosphate_str for item in phosphate_unit) and phosphate_value:
                    postprocess_phosphate_value = Q(float(phosphate_value), 'U/l')
                    return (postprocess_phosphate_value.magnitude,default) 

                elif any(item in phosphate_str for item in other_phosphate_unit) and phosphate_value:
                    unit_index = [item in phosphate_str for item in other_phosphate_unit].index(True)
                    postprocess_phosphate_value = Q(
                        float(phosphate_value), other_phosphate_unit[unit_index])
                    postprocess_phosphate_value_conv = postprocess_phosphate_value.to('U/l')
                    return (postprocess_phosphate_value.magnitude, other_phosphate_unit[unit_index]) if default in other_phosphate_unit else (postprocess_phosphate_value_conv.magnitude, 'U/l')
                # here we need to add range if available
                else:
                    postprocess_phosphate_value = Q(float(phosphate_value), 'U/l')
                    return (postprocess_phosphate_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
    def CO2_postprocess(CO2_str, default='mmol/L'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            CO2_str = str(CO2_str).lower()
            CO2_unit = ['mmol/L']
            other_CO2_unit = []
            CO2_str_match = Postprocess.get_process_value_str(CO2_str)
            CO2_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", CO2_str_match.strip(), re.I)

            CO2_value = None
            if CO2_str_match:
                CO2_value = CO2_str_match[0]
                if any(item in CO2_str for item in CO2_unit) and CO2_value:
                    postprocess_CO2_value = Q(float(CO2_value), 'mmol/L')
                    return (postprocess_CO2_value.magnitude,default) 

                elif any(item in CO2_str for item in other_CO2_unit) and CO2_value:
                    unit_index = [item in CO2_str for item in other_CO2_unit].index(True)
                    postprocess_CO2_value = Q(
                        float(CO2_value), other_CO2_unit[unit_index])
                    postprocess_CO2_value_conv = postprocess_CO2_value.to('mmol/L')
                    return (postprocess_CO2_value.magnitude, other_CO2_unit[unit_index]) if default in other_CO2_unit else (postprocess_CO2_value_conv.magnitude, 'mmol/L')
                # here we need to add range if available
                else:
                    postprocess_CO2_value = Q(float(CO2_value), 'mmol/L')
                    return (postprocess_CO2_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None
        
        
        
    def gcs_postprocess(gcs_str, default = ''):
        gcs_str = Postprocess.get_process_value_str(gcs_str)
        gcs_str = str(gcs_str).strip()
        try:
            if gcs_str:
                gcs_str = int(
                    re.sub(r"[^0-9]", " ", gcs_str).split(" ")[0]
                    )
                return gcs_str, None
            else:
                return None, None
        except ValueError as e:
            print(e)
            return None, None  
    def ptt_postprocess(ptt_str, default='s'):
        try:
            ureg = pint.UnitRegistry()
            Q = ureg.Quantity
            ptt_str = str(ptt_str).lower()
            ptt_unit = ['s']
            other_ptt_unit = []
            ptt_str_match = Postprocess.get_process_value_str(ptt_str)
            ptt_str_match = re.findall(
                r"(?:\d{1,}\.?\d*)", ptt_str_match.strip(), re.I)

            ptt_value = None
            if ptt_str_match:
                ptt_value = ptt_str_match[0]
                if any(item in ptt_str for item in ptt_unit) and ptt_value:
                    postprocess_ptt_value = Q(float(ptt_value), 's')
                    return (postprocess_ptt_value.magnitude,default) 

                elif any(item in ptt_str for item in other_ptt_unit) and ptt_value:
                    unit_index = [item in ptt_str for item in other_ptt_unit].index(True)
                    postprocess_ptt_value = Q(
                        float(ptt_value), other_ptt_unit[unit_index])
                    postprocess_ptt_value_conv = postprocess_ptt_value.to('s')
                    return (postprocess_ptt_value.magnitude, other_ptt_unit[unit_index]) if default in other_ptt_unit else (postprocess_ptt_value_conv.magnitude, 's')
                # here we need to add range if available
                else:
                    postprocess_ptt_value = Q(float(ptt_value), 's')
                    return (postprocess_ptt_value.magnitude, default)
            else:
                return None, None
        except Exception as e:
            print(e)
            return None, None                

        