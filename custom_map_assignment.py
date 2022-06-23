import pandas as pd 
import json
import os
import traceback
from copy import copy

PATH_TO_MAPS = "./mappings/"

class CustomDFAssigner(object):
    def __init__(self,env,maps_loc=PATH_TO_MAPS):
        self.env = env #also the name of the json to use
        self.map_location = maps_loc if maps_loc[-1] == '/' else maps_loc+'/'
        self.levels = ["L1", "L2"] #order to run certain maps within env file

    def start_processing(self,df, additional_check=False):
        """
        This method loads the correct mapping and then calls the method to run the found maps on the df
        """
        try:
            # finds the correct map file of jsons to use
            path = self.map_location + self.env + ".json"
            if os.path.exists(path):
                json_map_set = path
            else:
                map_found = False
                for fn in os.listdir(self.map_location):
                    if os.path.isfile(os.path.join(self.map_location, fn)):
                        if self.env.lower() == fn.split(".")[0].lower():
                            json_map_set = self.map_location + fn
                            map_found = True 
                            break 
                if not map_found:
                    raise Exception(f"+--ERROR: Map set not found at this path:\n{path}--+")

            maps_found_list = json.loads(open(json_map_set,"r").read())
            # runs the maps over the given data frame
            df = self.custom_assignment_processor(
                df,
                maps_found_list
            )
            if additional_check == True:
                extra_file = "extra"
                path = self.map_location + extra_file + ".json"
                if os.path.exists(path):
                    json_map_set = path
                else:
                    raise Exception(f"+--ERROR: Could not run additional_check - {extra_file}.json not found:\n{path}--+")
                self.env=extra_file
                df = self.custom_assignment_processor(
                    df, 
                    json.loads(open(json_map_set,"r").read()))
            # returns the df with column corresponding escalation group (or unknown)
            return df
        except Exception as e:
            if "+--ERROR: Could not run additional_check" in str(e):
                return df
            else: 
                traceback.print_exc()

    def logic_parser(self, df, logic, key, value):
        """
        For a given df, run the given logic to return a filtered df
        """
        try:
            #If value is a list of values, then run those values with the assigned logic, returning all values combined using "or logic"
            if isinstance(value, list):
                running_df = pd.DataFrame()
                for item in value:
                    temp_df = self.logic_parser(df, logic, key, item)
                    running_df = pd.concat([running_df, temp_df])
                return running_df.drop_duplicates()
            
            if logic == "equals":
                return df[df[key].str.lower() == value]
            elif logic == "not_equals":
                return df[df[key].str.lower() != value]
            elif logic == "contains":
                return df[df[key].str.lower().str.contains(value, na=False, regex=False)]
            elif logic == "not_contains":
                return df[df[key].str.lower().str.contains(value, na=False, regex=False) == False]
            elif logic == "starts_with":
                return df[df[key].str.lower().str.startswith(value)]
            else:
                raise Exception(f"+--ERROR: {logic} is not a valid option.--+")
        except:
            traceback.print_exc()

    def run_map_config(self,df,map_config,associated_query):
        """
        Recursively filter the df and return df of rows that match all map queries
        """
        try:
            map_config["logic"] = map_config["logic"].lower().strip()
            if isinstance(map_config["value"], list):
                map_config["value"] = list(map(str.strip, map(str.lower, map_config["value"])))
            else:
                map_config["value"] = map_config["value"].lower().strip()

            logic = map_config["logic"]
            key = map_config["key"]
            value = map_config["value"]

            if len(associated_query) == 0:
                #return self.logic_parser(df, logic, key, value) #This row will overwrite previously assigned rows
                return self.logic_parser(df[df["new_col"] == 'unknown'], logic, key, value)
            
            #pop automatically removes the element from the list
            next_map_config = associated_query.pop(0)
            
            df_filtered = self.run_map_config(df, next_map_config, associated_query)
            return self.logic_parser(df_filtered, logic, key, value)
        except:
            traceback.print_exc()

    def custom_assignment_processor(self,df,maps_found_list):
        """
        Run each map over the df and update the new_col for the found rows
        """
        try:
            # if df is not empty
            if len(df):
                # add column to df with default value
                if "new_col" not in list(df.columns):
                    df["new_col"] = "unknown"
                
                if "map_id" not in list(df.columns):
                    df["map_id"] = "unknown"
                
                catches = []
                levels = copy(self.levels)
                while True:
                    map_list = []
                    for map_json_elem in maps_found_list:
                        if "associated_query" not in list(map_json_elem.keys()):
                            map_json_elem["associated_query"] = []
                        target = []
                        target.append(map_json_elem)
                        if self.map_validator(df, target) is None:
                            catches.append(map_json_elem)
                        elif "post_run" in list(map_json_elem.keys()):
                            if map_json_elem["post_run"] in levels:
                                map_list.append(map_json_elem)
                            else:
                                df_filtered = self.run_map_config(
                                    df,
                                    map_config = map_json_elem,
                                    associated_query = map_json_elem["associated_query"])
                                # use filtered df to get row indicies and then use those to update the correct rows
                                if len(df_filtered):
                                    indicies = df_filtered.index
                                    df.loc[indicies,["new_col", "map_id"]] = [map_json_elem["assign_to"], f"{self.env}_{maps_found_list.index(map_json_elem)}"]
                        else:
                            df_filtered = self.run_map_config(
                                df,
                                map_config = map_json_elem,
                                associated_query = map_json_elem["associated_query"])
                            # use filtered df to get row indicies and then use those to update the correct rows
                            if len(df_filtered):
                                indicies = df_filtered.index
                                df.loc[indicies,["new_col", "map_id"]] = [map_json_elem["assign_to"], f"{self.env}_{maps_found_list.index(map_json_elem)}"]

                    if map_list:
                        maps_found_list = map_list
                        levels.pop(0)
                    else:
                        break
                assert catches == [], f"The following maps were not used for being invalid: {catches}"                        
            else:
                raise Exception(f"+--ERROR: Custom_assignment_processor - Recieved Empty DF - {self.env}--+")
            return df
        except Exception as e:
            if "The following maps" in str(e):
                return df
            else:
                traceback.print_exc()
    
    def map_validator(self, df, mapping=[]):
        try:
            if len(mapping) == 0:
                for fn in os.listdir(self.map_location):
                    if self.env.lower() == fn.split(".")[0].lower():
                        json_map_set = self.map_location + fn 
                        break 

                mappings = json.loads(open(json_map_set,"r").read())
            else:
                mappings = mapping
            approved_logic_list = ["contains","equals","not_equals","not_contains", "starts_with"]
            cols = list(df.columns)
            for r in range(0,len(mappings)):
                key = mappings[r]
                location = f"\n{self.env}_{r}: {key}"
                # Validate mapping has appropraite fields 
                assert ("key" in list(key.keys())), "Key Missing" + location
                assert ("logic" in list(key.keys())), "Logic Missing" + location
                assert ("value" in list(key.keys())), "Value Missing" + location
                assert ("assign_to" in list(key.keys())), "assign_to Missing" + location

                # Is Key is a valid field in DataFrame
                assert (key["key"] in cols), "Key is not a valid field in df" + location

                # Is Logic in approved logic list 
                assert (key["logic"].lower() in approved_logic_list), "Logic is not valid" + location

                if ("post_run" in list(key.keys())):
                    assert key["post_run"] in self.levels, "Post run is not valid" + location
                
                if ("associated_query" in list(key.keys())) and (len(key["associated_query"])):
                    for i in range(0,len(key["associated_query"])):
                        child = key["associated_query"][i]
                        assert ("key" in list(child.keys())), "Associated query key is missing" + location + f"(query at index {i})"
                        assert ("logic" in list(child.keys())), "Associated query logic is missing" + location + f"(query at index {i})"
                        assert ("value" in list(child.keys())), "Associated query value is missing" + location + f"(query at index {i})"
                        # Is Key is a valid field in DataFrame
                        assert (child["key"] in cols), "Associated query key is not valid" + location + f"(query at index {i})"
                        # Is Logic in approved logic list 
                        assert (child["logic"].lower() in approved_logic_list), "Associated query logic is not valid" + location + f"(query at index {i})"
            if len(mappings) > 1 and r == len(mappings) - 1:
                catches = []
                for i in range(0,len(mappings)-1):
                    for j in range(i+1, len(mappings)):
                        if mappings[i]['value'] in mappings[j]['value'] and mappings[i]['key'] in mappings[j]['key']:
                            catches.append(f"{self.env}_{i} and {self.env}_{j}")
                assert (catches == []), f"The following mappings have conflicts: {catches}"
            return f"Maps given are valid"
        except:
            traceback.print_exc()
    
    def duplicate_mappings_check(self, path=PATH_TO_MAPS):
        try:
            map_list = []
            catches = []
            for fn in os.listdir(path):
                if os.path.isfile(os.path.join(path, fn)):
                    if 'json' == fn.split(".")[1].lower():
                        map_list.append(fn)
            for i in range(0, len(map_list)-1):
                for j in range(i, len(map_list)):
                    mappings_1 = json.loads(open(path+map_list[i],"r").read())
                    mappings_2 = json.loads(open(path+map_list[j],"r").read())
                    for ii in range(0,len(mappings_1)-1):
                        for jj in range(ii,len(mappings_2)):
                            if mappings_1[ii] == mappings_2[jj]:
                                catches.append(f"{map_list[i].split('.')[0]}_{ii} and {map_list[j].split('.')[0]}_{jj}")
            assert (catches == []), f"The following mappings have conflicts: {catches}"
            return "No duplicates found"
        except:
            traceback.print_exc()
    
    def print_mapping(self, map_id):
        try:
            path = self.map_location + map_id.split("_")[0] + ".json"
            if os.path.exists(path):
                json_map_set = path
            else:
                map_found = False
                for fn in os.listdir(self.map_location):
                    if map_id.split("_")[0] == fn.split(".")[0].lower():
                        json_map_set = self.map_location + fn
                        map_found = True 
                        break 
                if not map_found:
                    raise Exception(f"+--ERROR: Map set not found at this path:\n{path}--+")
            maps_found_list = json.loads(open(json_map_set,"r").read())
            id = int(map_id.split("_")[1])
            if id >= 0 and id < len(maps_found_list):
                return maps_found_list[id]
            else:
                raise Exception("+-- Error: Map ID not found --+")
        except:
            traceback.print_exc()
            return ["Error Finding This Map"]

    def print_mappings(self,id_list):
        try:
            mappings=[]
            for id in id_list:
                mapping = self.print_mapping(id)
                #print(mapping)
                mappings.append(mapping)
            return mappings
        except:
            traceback.print_exc()


if __name__ == "__main__":
    print(CustomDFAssigner(env="test").start_processing(
        df = pd.read_csv("./data.csv")
    )
    )