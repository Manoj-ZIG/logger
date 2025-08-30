import re
from rapidfuzz import process, fuzz

class FuzzMatcher:
    """
    A model for fuzzy matching component names based on input test names.
    It supports direct matching, bifurcation based on blood/urine tests,
    and fuzzy matching with weighted scoring.
    """

    def __init__(self, avg_score_thresh, inclusion_dict, exclusion_dict):
        self.avg_score_thresh = avg_score_thresh
        self.inclusion_dict = inclusion_dict
        self.exclusion_dict = exclusion_dict

    def fuzzy_matcher(self,test_name, terms_dict):
        test_name = str(test_name).strip().lower()
        predicted_scores = {}

 
        if not terms_dict:
            return predicted_scores  

        for attr, key_terms in terms_dict.items():
            # Collect scores using different matchers

            scores = []
            key_terms_lower = [term.lower() for term in key_terms]
            for scorer in [fuzz.token_sort_ratio, fuzz.QRatio,  fuzz.token_set_ratio]:
                match = process.extractOne(test_name, key_terms_lower, scorer=scorer)
                if match:  # Check if match is not None
                    scores.append(match[1])
                else:
                    scores.append(0)  

            # Calculate the average score
            predicted_scores[attr] = sum(scores) / len(scores) if scores else 0  

        return predicted_scores
    
    def inclusion_exclusion(self, test_name, comp_ls):
        """Match the test name against inclusion and exclusion lists."""
        inclusion_list = {key: self.inclusion_dict[key] for key in comp_ls if key in self.inclusion_dict} if comp_ls else self.inclusion_dict
        exclusion_list = {key: self.exclusion_dict[key] for key in comp_ls if key in self.exclusion_dict} if comp_ls else self.exclusion_dict
        
        in_scores = self.fuzzy_matcher(test_name, inclusion_list)
        
        # Find the attribute with the highest score from inclusion
        max_in_attr = max(in_scores, key=in_scores.get)
        max_in_score = in_scores[max_in_attr]

        # Check against exclusions
        if exclusion_list[max_in_attr]:
            ex_scores = self.fuzzy_matcher(test_name, exclusion_list)
            ex_score=ex_scores[max_in_attr]

            # If the highest inclusion and exclusion attributes match and exclusion score is higher 
            if ex_score>max_in_score:
                return (None,0)  

        return (max_in_attr,max_in_score) if max_in_score >= self.avg_score_thresh else (None,0)
    def clean_test_name(self,test_name: str) -> str:
        if not isinstance(test_name, str):
            return ""
        cleaned = re.sub(r"\([^()]*\d+[^()]*\)|(\[.*?\])|\([^()]*$|((?:\d{1,}(?:\.\d{1,})?\s?\-\s?\d{1,}(?:\.\d{1,})?\s?))|\d{3}\/\d{2}|\d{1,}\/\d{1,}\/\d{1,}|\b(\d{1,}(?:\.\d{1,})?)\b|(\b\w{2}\/\w{2}\b)|(\b\w{3,4}\/\w{1}\b)|[:,.;><]", "", test_name)
        return re.sub(r' +', ' ', cleaned).strip().lower()

    def get_match_component_v2(self, inp_test_name):
        """Main function to match input test name to components."""
        inp_test_name = self.clean_test_name(inp_test_name)

        # Return None for invalid input
        if 'nan' in inp_test_name:
            return None,0

        # Step 1: Direct match
        for k, v in self.inclusion_dict.items():
            if inp_test_name in map(str.lower, v):
                return self.inclusion_exclusion(inp_test_name, list(self.inclusion_dict))
               

        # Step 2: Bifurcate based on blood/urine
        ur_aliases = ['ur', 'urine', 'ua', 'u', 'random', 'rndm']
        blood_aliases = ['serum', 'blood', 'bld', 'plasma']
        
        ur_reg = r'\b(?:' + '|'.join(map(re.escape, ur_aliases)) + r')\b'
        bld_reg = r'\b(?:' + '|'.join(map(re.escape, blood_aliases)) + r')\b'
        
        ur_route_match = re.search(ur_reg, inp_test_name, re.I)
        bld_route_match = re.search(bld_reg, inp_test_name, re.I)

        route_map_dict = {
            'ur': ['Urine Sodium', 'Urine Volume', 'Urine Creatinine', 
                   'Urine Osmolality', 'Urine Specific Gravity', 'Urine Protein','Urine Bilirubin','Uric Acid, Urine',],
            'blood': ['WBC', 'Bands', 'Procalcitonin', 
                      'C-Reactive Protein (CRP)', 'Sodium', 
                      'Potassium', 'Bilirubin', 'Serum Creatinine',
                      'Blood Urea Nitrogen (BUN)', 'Serum Osmolality',
                      'BUN/Creatinine ratio', 'Creatine Kinase',
                      'Cardiac Troponin', 'PT', 'INR', 
                      'APTT', 'Platelets',
                      'Acetaminophen Test','Alcohol Level',
                      'Serum Cortisol','Thiamine Test','Vitamin B12','Ammonia Level Test', 'GFR Test','RBC',
                      'Hemoglobin Test','Hematocrit Test','Alanine Transaminase (ALT)','Aspartate Transaminase (AST)','Alkaline Phosphatase','Gamma-glutamyltransferase (GGT)','Albumin','Direct Bilirubin',
                      'Chloride','Total Protein','Thyroid Stimulating Hormone (TSH)','Free Thyroxine (FT4)', 'Thyroxine (T4)','Triiodothyronine (FT3)','Relative Lymphocytes','Absolute Lymphocytes',
                      'Lactate Dehydrogenase (LDH)','Uric Acid, Serum', 'PaO2, Arterial','PaCO2','HCO3','Glucose','Calcium','Magnesium','Phosphate','Carbon Dioxide','PTT']
        }

        if ur_route_match:
            return self.inclusion_exclusion(inp_test_name, route_map_dict['ur'])
        
        if bld_route_match:
            return_comp = self.inclusion_exclusion(inp_test_name, route_map_dict['blood'])
            if return_comp is None:
                remaining_components = set(self.inclusion_dict.keys()) - set(route_map_dict['blood'])
                return self.inclusion_exclusion(inp_test_name, list(remaining_components))
        
        # Step 3: Default to all components excluding urine components
        remaining_components = set(self.inclusion_dict.keys())
        return self.inclusion_exclusion(inp_test_name, list(remaining_components))