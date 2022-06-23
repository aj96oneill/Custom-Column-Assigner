import pytest 
import pandas as pd
import json

from custom_map_assignment import CustomDFAssigner

class Test_start_processing():
    """
    Test the method 'start_processing' for valid and invalid scenarios
    (dependent on custom_assignment_processor, run_map_config, and logic_parser)
    """
    def test_1(self):
        """
        test that it loads a single mapping correctly and returns a correct df
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        updated = CustomDFAssigner("t1",maps_loc="./unit_test_mappings/").start_processing(df)
        group_count = updated[updated.new_col.str.lower() != "unknown"]['new_col'].value_counts()
        id_count = updated[updated.new_col.str.lower() != "unknown"]['map_id'].value_counts()
        assert group_count['test'] == 1, "Test 1 failed - wrong group"
        assert id_count['t1_0'] == 1, "Test 1 failed - wrong map id"

    def test_2(self):
        """
        test that it loads a single mapping correctly, even if env has wrong casing, and returns a correct df
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        updated = CustomDFAssigner("T1",maps_loc="./unit_test_mappings/").start_processing(df)
        group_count = updated[updated.new_col.str.lower() != "unknown"]['new_col'].value_counts()
        id_count = updated[updated.new_col.str.lower() != "unknown"]['map_id'].value_counts()
        assert group_count['test'] == 1, "Test 2 failed - wrong group"
        assert id_count['T1_0'] == 1, "Test 2 failed - wrong map id"

    def test_3(self):
        """
        test that it won't load if it can't find the mapping correctly
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        updated = CustomDFAssigner("p1",maps_loc="./unit_test_mappings/").start_processing(df)
        assert updated is None, "Test 3 failed - loading wrong json"
    
    def test_4(self):
        """
        test that it loads a single mapping correctly, runs post_check, and returns a correct df
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        updated = CustomDFAssigner("t1",maps_loc="./unit_test_mappings/").start_processing(df,True)
        group_count = updated[updated.new_col.str.lower() != "unknown"]['new_col'].value_counts()
        id_count = updated[updated.new_col.str.lower() != "unknown"]['map_id'].value_counts()
        assert group_count['test'] == 2, "Test 2 failed - wrong group"
        assert id_count['t1_0'] == 1, "Test 2 failed - wrong map id"
        assert id_count['extra_0'] == 1, "Test 2 failed - wrong map id"
    
    def test_5(self):
        """
        test that it won't load extra.json if it can't find the mapping correctly, and returns a correct df
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        updated = CustomDFAssigner("t1",maps_loc="./unit_test_mappings/run_map_config/").start_processing(df,True)
        group_count = updated[updated.new_col.str.lower() != "unknown"]['new_col'].value_counts()
        id_count = updated[updated.new_col.str.lower() != "unknown"]['map_id'].value_counts()
        assert group_count['test'] == 1, "Test 2 failed - wrong group"
        assert id_count['t1_0'] == 1, "Test 2 failed - wrong map id"

class Test_logic_parser():
    """
    test the method 'logic_parser' for valid and invalid scenarios
    """
    def test_1(self):
        """
        test that logic parser handles correctly and returns a correct df
        logic: equals
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        mapping = json.loads(open("./unit_test_mappings/logic_parser/t1.json","r").read())
        updated = CustomDFAssigner("t1").logic_parser(df, mapping[0]["logic"], mapping[0]["key"], mapping[0]["value"])
        assert len(updated) == 1, "Test 1 failed"


    def test_2(self):
        """
        test that logic parser handles correctly and returns a correct df
        logic: not_equals (uses [] logic)
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        mapping = json.loads(open("./unit_test_mappings/logic_parser/t2.json","r").read())
        updated = CustomDFAssigner("t1").logic_parser(df, mapping[0]["logic"], mapping[0]["key"], mapping[0]["value"])
        assert len(updated) == 3, "Test 2 failed"

    def test_3(self):
        """
        test that logic parser handles correctly and returns a correct df
        logic: contains
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        mapping = json.loads(open("./unit_test_mappings/logic_parser/t3.json","r").read())
        updated = CustomDFAssigner("t1").logic_parser(df, mapping[0]["logic"], mapping[0]["key"], mapping[0]["value"])
        assert len(updated) == 1, "Test 3 failed"
    
    def test_4(self):
        """
        test that logic parser handles correctly and returns a correct df
        logic: not_contains (uses [] logic)
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        mapping = json.loads(open("./unit_test_mappings/logic_parser/t4.json","r").read())
        updated = CustomDFAssigner("t1").logic_parser(df, mapping[0]["logic"], mapping[0]["key"], mapping[0]["value"])
        assert len(updated) == 3, "Test 4 failed"

    def test_5(self):
        """
        test that logic parser handles correctly and returns a correct df
        logic: starts_with
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        mapping = json.loads(open("./unit_test_mappings/logic_parser/t5.json","r").read())
        updated = CustomDFAssigner("t1").logic_parser(df, mapping[0]["logic"], mapping[0]["key"], mapping[0]["value"])
        assert len(updated) == 1, "Test 5 failed"

    def test_6(self):
        """
        test that logic parser will handle invalid logic value
        """
        
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        mapping = json.loads(open("./unit_test_mappings/logic_parser/t6.json","r").read())
        updated = CustomDFAssigner("t1").logic_parser(df, mapping[0]["logic"], mapping[0]["key"], mapping[0]["value"])
        assert updated is None, "Test 6 failed - Logic Parser using incorrect logic"

class Test_run_map_config():
    """
    Test the method 'run_map_config' for valid and invalid scenarios
    (dependent on logic_parser)
    """
    def test_1(self):
        """
        test that it handles correctly and returns a correct df
        1 mapping, no associated queries
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z'], 'new_col':['unknown','unknown','unknown']})
        mapping = json.loads(open("./unit_test_mappings/run_map_config/t1.json","r").read())
        updated = CustomDFAssigner("t1").run_map_config(df,mapping[0], mapping[0]["associated_query"])
        assert len(updated) == 1, "Test 1 - Failed to run mapping"

    def test_2(self):
        """
        test that it handles correctly and returns a correct df
        1 mapping, 1 associated queries
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z'], 'new_col':['unknown','unknown','unknown']})
        mapping = json.loads(open("./unit_test_mappings/run_map_config/t2.json","r").read())
        updated = CustomDFAssigner("t1").run_map_config(df,mapping[0], mapping[0]["associated_query"])
        assert len(updated) == 1, "Test 2 - Failed to run mapping"
    
    def test_3(self):
        """
        test that run mapping will handle invalid mapping value
        value is missing
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z'], 'new_col':['unknown','unknown','unknown']})
        mapping = json.loads(open("./unit_test_mappings/run_map_config/t3.json","r").read())
        updated = CustomDFAssigner("t1").run_map_config(df,mapping[0], mapping[0]["associated_query"])
        assert updated is None, "Test 3 - Using invalid mapping"

    def test_4(self):
        """
        test that run mapping will handle invalid mapping value
        value is missing in associated query
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z'], 'new_col':['unknown','unknown','unknown']})
        mapping = json.loads(open("./unit_test_mappings/run_map_config/t4.json","r").read())
        updated = CustomDFAssigner("t1").run_map_config(df,mapping[0], mapping[0]["associated_query"])
        assert updated is None, "Test 4 - Using invalid mapping"

class Test_custom_assignment_processor():
    """
    Test the method 'custom_assignment_processor' for valid and invalid scenarios
    (dependent on run_map_config, and logic_parser)
    """
    def test_1(self):
        """
        test that custom assignment processor handles correctly and returns correct df
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        mapping = json.loads(open("./unit_test_mappings/t1.json","r").read())
        updated = CustomDFAssigner("t1").custom_assignment_processor(df,mapping)
        group_count = updated[updated.new_col.str.lower() != "unknown"]['new_col'].value_counts()
        id_count = updated[updated.new_col.str.lower() != "unknown"]['map_id'].value_counts()
        assert group_count['test'] == 1, "Test 1 failed - wrong group"
        assert id_count['t1_0'] == 1, "Test 1 failed - wrong map id"

    def test_2(self):
        """
        test that an empty df will raise an error
        """
        df = pd.DataFrame()
        updated = CustomDFAssigner("t1").custom_assignment_processor(df,[])
        assert updated is None, "Test 2 failed - working with empty df"

    def test_3(self):
        """
        test that new_col will be added to the df
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        updated = CustomDFAssigner("t1").custom_assignment_processor(df,[])
        assert "new_col" in list(updated.columns), "Test 3 failed - esc group not added"

    def test_4(self):
        """
        test that map_id will be added to the df
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        updated = CustomDFAssigner("t1").custom_assignment_processor(df,[])
        assert "map_id" in list(updated.columns), "Test 4 failed - esc group not added"

    def test_5(self):
        """
        test that found rows will be updated correctly for many mappings
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        mapping = json.loads(open("./unit_test_mappings/t4.json","r").read())
        updated = CustomDFAssigner("t4").custom_assignment_processor(df,mapping)
        group_count = updated[updated.new_col.str.lower() != "unknown"]['new_col'].value_counts()
        id_count = updated[updated.new_col.str.lower() != "unknown"]['map_id'].value_counts()
        assert group_count['test'] == 3, "Test 5 failed - wrong group"
        assert id_count['t4_0'] == 1, "Test 5 failed - wrong map id"
        assert id_count['t4_1'] == 1, "Test 5 failed - wrong map id"
        assert id_count['t4_2'] == 1, "Test 5 failed - wrong map id"

    def test_6(self):
        """
        test that post run will run on the second pass
        """
        df = pd.DataFrame.from_dict({'A': ['a','a','x'], 'B': ['b','a','y'], 'C': ['c','q','z']})
        mapping = json.loads(open("./unit_test_mappings/t5.json","r").read())
        updated = CustomDFAssigner("t5").custom_assignment_processor(df,mapping)
        group_count = updated[updated.new_col.str.lower() != "unknown"]['new_col'].value_counts()
        id_count = updated[updated.new_col.str.lower() != "unknown"]['map_id'].value_counts()
        assert group_count['test'] == 2, "Test 6 failed - wrong group"
        assert id_count['t5_0'] == 1, "Test 6 failed - post_run value not right"
        assert id_count['t5_1'] == 1, "Test 6 failed - wrong map id"

    def test_7(self):
        """
        test that post run will run correctly for multiple pass
        """
        df = pd.DataFrame.from_dict({'A': ['a','a','a'], 'B': ['b','a','k'], 'C': ['c','q','k']})
        mapping = json.loads(open("./unit_test_mappings/t6.json","r").read())
        updated = CustomDFAssigner("t6").custom_assignment_processor(df,mapping)
        group_count = updated[updated.new_col.str.lower() != "unknown"]['new_col'].value_counts()
        id_count = updated[updated.new_col.str.lower() != "unknown"]['map_id'].value_counts()
        assert group_count['test'] == 3, "Test 7 failed - wrong group"
        assert id_count['t6_0'] == 1, "Test 7 failed - post_run value not right"
        assert id_count['t6_1'] == 1, "Test 7 failed - wrong map id"
        assert id_count['t6_2'] == 1, "Test 7 failed - wrong map id"

    def test_8(self):
        """
        test that it will not run an invalid mapping in a set of mappings
        """
        df = pd.DataFrame.from_dict({'A': ['a','a','a'], 'B': ['b','a','k'], 'C': ['c','q','z']})
        mapping = json.loads(open("./unit_test_mappings/t7.json","r").read())
        updated = CustomDFAssigner("t7").custom_assignment_processor(df,mapping)
        group_count = updated[updated.new_col.str.lower() != "unknown"]['new_col'].value_counts()
        id_count = updated[updated.new_col.str.lower() != "unknown"]['map_id'].value_counts()
        assert group_count['test'] == 1, "Test 8 failed - invalid mapping ran"
        assert id_count['t7_0'] == 1, "Test 8 failed"

    def test_9(self):
        """
        test that it will run when mapping doesn't have an associated query
        """
        df = pd.DataFrame.from_dict({'A': ['a','a','a'], 'B': ['b','a','k'], 'C': ['c','q','z']})
        mapping = json.loads(open("./unit_test_mappings/t8.json","r").read())
        updated = CustomDFAssigner("t8").custom_assignment_processor(df,mapping)
        group_count = updated[updated.new_col.str.lower() != "unknown"]['new_col'].value_counts()
        id_count = updated[updated.new_col.str.lower() != "unknown"]['map_id'].value_counts()
        assert group_count['test'] == 1, "Test 9 failed"
        assert id_count['t8_0'] == 1, "Test 9 failed"
        
class Test_map_validator():
    """
    Test the method 'map_validator' for valid and invalid scenarios
    """
    def test_1(self):
        """
        test if a valid mapping is valid
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        mapping = json.loads(open("./unit_test_mappings/t1.json","r").read())
        response = CustomDFAssigner("t1").map_validator(df,mapping)
        assert response == 'Maps given are valid', "Error validating a mapping"

    def test_2(self):
        """
        test if a valid set of mappings are valid
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        mapping = json.loads(open("./unit_test_mappings/t4.json","r").read())
        response = CustomDFAssigner("t1").map_validator(df,mapping)
        assert response == 'Maps given are valid', "Error validating a mapping"

    def test_3(self):
        """
        test if it handles when no mappings are given
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        response = CustomDFAssigner("t1", maps_loc="./unit_test_mappings/").map_validator(df)
        assert response == 'Maps given are valid', "Error validating a mapping"

    def test_4(self):
        """
        test if it handles when mapping is invalid
        Missing field: key
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        response = CustomDFAssigner("t1", maps_loc="./unit_test_mappings/map_validator/").map_validator(df)
        assert response is None, "Test 4 failed - invalid mapping not caught"

    def test_5(self):
        """
        test if it handles when mapping is invalid
        Missing field: logic
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        response = CustomDFAssigner("t2", maps_loc="./unit_test_mappings/map_validator/").map_validator(df)
        assert response is None, "Test 5 failed - invalid mapping not caught"

    def test_6(self):
        """
        test if it handles when mapping is invalid
        Incorrect logic
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        response = CustomDFAssigner("t3", maps_loc="./unit_test_mappings/map_validator/").map_validator(df)
        assert response is None, "Test 6 failed - invalid mapping not caught"
    
    def test_7(self):
        """
        test if it handles when mapping is invalid
        Missing field: value
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        response = CustomDFAssigner("t4", maps_loc="./unit_test_mappings/map_validator/").map_validator(df)
        assert response is None, "Test 7 failed - invalid mapping not caught"

    def test_8(self):
        """
        test if it handles when mapping is invalid
        Missing field: assign_to
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        response = CustomDFAssigner("t5", maps_loc="./unit_test_mappings/map_validator/").map_validator(df)
        assert response is None, "Test 8 failed - invalid mapping not caught"

    def test_9(self):
        """
        test if it handles when mapping is invalid
        Incorrect value for key
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        response = CustomDFAssigner("t6", maps_loc="./unit_test_mappings/map_validator/").map_validator(df)
        assert response is None, "Test 9 failed - invalid mapping not caught"

    def test_10(self):
        """
        test if it handles when mapping is invalid
        Missing field: associated query key
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        response = CustomDFAssigner("t7", maps_loc="./unit_test_mappings/map_validator/").map_validator(df)
        assert response is None, "Test 10 failed - invalid mapping not caught"

    def test_11(self):
        """
        test if it handles when mapping is invalid
        Missing field: associated query logic
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        response = CustomDFAssigner("t8", maps_loc="./unit_test_mappings/map_validator/").map_validator(df)
        assert response is None, "Test 11 failed - invalid mapping not caught"

    def test_12(self):
        """
        test if it handles when mapping is invalid
        Missing field: associated query value
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        response = CustomDFAssigner("t9", maps_loc="./unit_test_mappings/map_validator/").map_validator(df)
        assert response is None, "Test 12 failed - invalid mapping not caught"

    def test_13(self):
        """
        test if it handles when mapping is invalid
        Incorrect logic for associated query
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        response = CustomDFAssigner("t10", maps_loc="./unit_test_mappings/map_validator/").map_validator(df)
        assert response is None, "Test 13 failed - invalid mapping not caught"

    def test_14(self):
        """
        test if it handles when mapping is invalid
        Incorrect value for associated query key
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        response = CustomDFAssigner("t11", maps_loc="./unit_test_mappings/map_validator/").map_validator(df)
        assert response is None, "Test 14 failed - invalid mapping not caught"

    def test_15(self):
        """
        test if it handles when mapping is invalid
        Incorrect value for post run
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        response = CustomDFAssigner("t12", maps_loc="./unit_test_mappings/map_validator/").map_validator(df)
        assert response is None, "Test 15 failed - invalid mapping not caught"

    def test_16(self):
        """
        test if it handles when mapping is valid - post run included
        """
        df = pd.DataFrame.from_dict({'A': ['a','d','x'], 'B': ['b','e','y'], 'C': ['c','f','z']})
        response = CustomDFAssigner("t13", maps_loc="./unit_test_mappings/map_validator/").map_validator(df)
        assert response == 'Maps given are valid', "Test 16 failed - mapping is valid"

class Test_duplicate_mappings_check():
    """
    Test the method 'duplicate_mappings_check' for valid and invalid scenarios
    """
    def test_1(self):
        """
        Test that it will find no duplicates
        """
        response = CustomDFAssigner("t1").duplicate_mappings_check("./unit_test_mappings/no_duplicates/")
        assert response == 'No duplicates found', "Error checking for duplicates"

    def test_2(self):
        """
        Test that it will find duplicates
        """
        response = CustomDFAssigner("t1").duplicate_mappings_check("./unit_test_mappings/")
        assert response is None, "test failed - should be  duplicates"

class Test_print_mapping():
    """
    Test the method 'print_mapping' for valid and invalid scenarios
    """
    def test_1(self):
        """
        print a valid mapping
        """
        response = CustomDFAssigner("t1", maps_loc="./unit_test_mappings/").print_mapping("t1_0")
        assert response == {'key': 'A', 'logic': 'equals', 'value': 'a', 'assign_to': 'test', 'associated_query': []}, "Error printing mapping"

    def test_2(self):
        """
        test for an invalid map id that can't be found
        """
        response = CustomDFAssigner("t1", maps_loc="./unit_test_mappings/").print_mapping("X1_0")
        assert response == ["Error Finding This Map"], "Invalid mapping printed"
        
    def test_3(self):
        """
        test for a case sensative map id
        """
        response = CustomDFAssigner("t1", maps_loc="./unit_test_mappings/").print_mapping("T1_0")
        assert response == {'key': 'A', 'logic': 'equals', 'value': 'a', 'assign_to': 'test', 'associated_query': []}, "Error printing mapping"

    def test_4(self):
        """
        test for an invalid map id
        """
        response = CustomDFAssigner("t1", maps_loc="./unit_test_mappings/").print_mapping("t1_100")
        assert response == ["Error Finding This Map"], "Invalid mapping printed"

class Test_print_mappings():
    """
    Test the method 'print_mappings' for valid and invalid scenarios
    """
    def test_1(self):
        """
        test a set of valid mappings will handle correctly
        """
        response = CustomDFAssigner("t1", maps_loc="./unit_test_mappings/").print_mappings(["t1_0","t1_0"])
        assert response == [{'key': 'A', 'logic': 'equals', 'value': 'a', 'assign_to': 'test', 'associated_query': []}, {'key': 'A', 'logic': 'equals', 'value': 'a', 'assign_to': 'test', 'associated_query': []}], "Error printing mappings"

    def test_2(self):
        """
        test for an invalid map id
        """
        response = CustomDFAssigner("t1", maps_loc="./unit_test_mappings/").print_mappings(["t1_100","t1_0"])
        assert ["Error Finding This Map"] in response, "Error printing mappings"
