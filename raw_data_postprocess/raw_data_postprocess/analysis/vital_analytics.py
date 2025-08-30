class VitalAnalytics:
    def __init__(self):
        pass

    @staticmethod
    def get_ref_range_and_boolean_flag(test_name, test_result, test_name_range_dict):
        if test_result:
            if test_result > test_name_range_dict.get(test_name)[1] or test_result < test_name_range_dict.get(test_name)[0]:
                return test_name_range_dict.get(test_name), 1
            else:
                return test_name_range_dict.get(test_name), 0
        else:
            return None,None
