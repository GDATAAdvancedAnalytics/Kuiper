#!/usr/bin/python

from werkzeug.security import generate_password_hash
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import uuid
from datetime import datetime
import hashlib
from bson import ObjectId
import sys
import json
import ast
import shutil

from app import *


DB_NAME = app.config['DB_NAME']
DB_IP   = app.config['DB_IP']
DB_PORT = app.config['DB_PORT']




# =================================================
#               Database Groups
# =================================================

# ===================================== DB groups
# this class contain the groups
class DB_Groups():
    MongoClient = None
    def __init__(self):

        # if rules collection not exists, add it to mongoDB
        db_m = MClient[DB_NAME]
        
        if 'groups' not in db_m.list_collection_names():
            db_m.create_collection('groups')

        self.MongoClient = MClient[DB_NAME]["groups"]


    
    # ===================================== Get group
    # get a list of all groups
    def get_groups(self , case_id = None):
        try:
            query = {}
            if case_id is not None:
                query['case_name'] = case_id 

            data = self.MongoClient.find(query)
            datax = []
            for x in data:
                datax.append(x)
            return [True, datax]
        except Exception as e:
            return [False, str(e)]


    # ===================================== Add Group
    # add group to data base
    def add_group(self, case_name , group_name):
        try:
            
            self.MongoClient.insert_one({
                '_id'       : case_name + "_" + group_name,
                "case_name" : case_name,
                'group_name': group_name
                })
            return [True , "Group ["+group_name+"] on case ["+case_name+"] added to mongoDB"]

        except DuplicateKeyError:
            return [False , "Group ["+group_name+"] on case ["+case_name+"] already exists"]

        except Exception as e:
            return [False , str(e)]

    # =================================== Delete group 
    # deassien all machines from the group before deleting it
    def delete_group(self, case_id, group_name):
        try:
            res = db_cases.deassign_all_from_group(case_id , group_name )
            if res[0] == False:
                return res 

            data = self.MongoClient.delete_one({"_id":case_id + "_" + group_name})
            return [True, "Group ["+group_name+"] deleted"]

        except Exception as e:
            return [False, str(e)]
# generate the db rule object
def get_db_groups():
    return DB_Groups()



# =================================================
#               End Database Groups
# =================================================







# =================================================
#               Database Cases
# =================================================


class DB_Cases:

    mongo_db = None
    def __init__(self):

        # if cases collection not exists, add it to mongoDB
        db_m = MClient[DB_NAME]
        if 'cases' not in db_m.list_collection_names():
            db_m.create_collection('cases')

        self.mongo_db = MClient[DB_NAME]["cases"]

    # ======================= Machines ==========================

    # ================================ get all machines for case
    # take case id and return all machines for this case
    def get_machines(self, case_id , group=None):
        machines = []
        try:
            query = {'main_case': case_id}
            if group is not None:
                query['groups'] = group
            for d in self.mongo_db["machines"].find(query).sort([['creation_time', -1]]):
                machines.append(d)

            return [True, machines]
        except Exception as e:
            return [False, None]



    # ================================ get specific machines by id
    def get_machine_by_id(self , machine_id):
        try:
            for i in self.mongo_db["machines"].find({"_id" : machine_id}):
                return [True, i]
            return [True, None]
        except Exception as e:
            return [False, str(e)]


    
    # ================================ add machine
    def add_machine(self, machine_details):
        try:
            # Connect to the DB
            collection = self.mongo_db["machines"]

            machine_details['_id'] = machine_details['main_case'] + "_" + machine_details['machinename']
            machine_details['creation_time']= str( datetime.now() ).replace(' ' , 'T')
            # insert into collection
            collection.insert_one(machine_details)
            return [True, machine_details['_id'] ]

        except DuplicateKeyError:
            return [False , "Machine already exists"] 
        except Exception as e:
            return [False, str(e)]
    
    
    # ================================ delete machine
    def delete_machine(self, machine_id):
        try:
            collection = self.mongo_db['machines']
            data = collection.delete_one({"_id": machine_id})

            collection = MClient[DB_NAME]['files']
            data = collection.delete_many({"machine_id": machine_id})

            return [True, data]
        except Exception as e:
            return [False , str(e)]


    # ================================ Update machine
    # update case details information
    def update_machine(self , machine_id , machine_details):
        try:
            collection = self.mongo_db["machines"]
            up = collection.update_one({'_id':machine_id}, {'$set': machine_details },upsert=False)
            return [True, up]
        except Exception as e:
            return [False, "Error: " + str(e)]



    # ================================ Assign machines to group
    # assign machines to specified group
    def assign_to_group(self , machines_list , group_name):
        if len(machines_list) == 0:
            return [True, 'no machines has been specified']
        try:
            up = None 
            collection  = self.mongo_db["machines"]
            for m in machines_list:
                # check if the machine already assigned to this group 
                machine = self.get_machine_by_id(m)
                if machine[0] == False:
                    continue 
                if 'groups' in machine[1] and group_name in machine[1]['groups']:
                    continue

                up = collection.update_one({ "_id" : m }, {'$push': {'groups' : group_name} },upsert=False)
            return [True, up]
        except Exception as e:
            return [False, "Error: " + str(e)]



    # ================================ Deassign machines to group
    # deassign machines from specified group
    def deassign_from_group(self , machines_list , group_name):
        if len(machines_list) == 0:
            return [True, 'no machines has been specified']
        try:
            collection  = self.mongo_db["machines"]
            for m in machines_list:
                up = collection.update_one({ "_id" : m }, {'$pull': {'groups' : group_name} },upsert=False)
            return [True, up]
        except Exception as e:
            return [False, "Error: " + str(e)]


    

    # ================================ Deassign machines from group
    # deassign all machines from group
    def deassign_all_from_group(self , case_id , group_name):
        try:
            up = None
            collection  = self.mongo_db["machines"]
            machines = collection.find({ "main_case" : case_id })
            for m in machines:
                up = collection.update_one({ "_id" : m['_id'] }, {'$pull': {'groups' : group_name} },upsert=False)
            return [True, up]
        except Exception as e:
            return [False, "Error: " + str(e)]





    # ======================= Cases ==========================

    # ================================ get specific case by id
    def get_case_by_id(self, case_id):
        try:
            for d in self.mongo_db.find({'casename' : case_id}):
                return [True, d]
            return [True , None]
        except Exception as e:
            return [False , str(e)]
    
    # ================================ get all cases
    def get_cases(self):
        try:
            data = self.mongo_db.find({})
            d = []
            for x in data:
                d.append(x)
            return [True , d]

        except Exception as e:
            return [False , str(e)]


    # ================================ Delete case
    # delete case by its id
    def delete_case(self, case_id):
        try:
            data = self.mongo_db.delete_one({"casename":case_id})
            data = self.mongo_db['machines'].delete_many({"main_case":case_id})
            return [True, "Case ["+case_id+"] deleted"]
        except Exception as e:
            return [False, str(e)]

    # ================================ Update case
    # update case details information
    def update_case(self, case_id , case_details):
        try:
            up = self.mongo_db.update_one({'casename':case_id}, {'$set': case_details },upsert=False)
            return [True, up]
        except Exception as e:
            return [False, "Error: " + str(e)]

    # ================================ Create case
    # create new case
    def create_case(self, case_details):
        try:
            # get the UUID for casename as case id
            idtest = str(uuid.uuid4())
            idtestfilter = idtest.replace('-','')
            case_id = idtestfilter

            data = {
                '_id'                   :case_details['casename'],
                'casename'              :case_details['casename'],
                'status'                :case_details['status'],
                'date'                  :case_details['date']
            }


            # insert into collection
            self.mongo_db.insert_one(data)
            return [True, data]

        except DuplicateKeyError:
            return [False, "Case ["+case_details['casename']+"] already present in DB"]
        except Exception as e:
            return [False , "Error:" + str(e)]

# generate the db class object
def get_db_cases():
    return DB_Cases()


# =================================================
#               End Database Cases
# =================================================








# =================================================
#               Database Rules
# =================================================

# ===================================== DB rules
# this class contain the rules
class DB_Rules():
    MongoClient = None
    def __init__(self):

        # if rules collection not exists, add it to mongoDB
        db_m = MClient[DB_NAME]
        
        if 'rules' not in db_m.list_collection_names():
            db_m.create_collection('rules')

        self.MongoClient = MClient[DB_NAME]["rules"]


    # ===================================== Get rules
    # get a list of all rules
    def get_rules(self):
        try:
            data = self.MongoClient.find({})
            datax = []
            for x in data:
                datax.append(x)
            return [True, datax]
        except Exception as e:
            return [False, str(e)]


    # ===================================== Delete rule
    # delete rule
    def delete_rule(self, rule_id):
        try:
            self.MongoClient.delete_one({"_id":rule_id})
            return [True , "Rule ["+rule_id+"] deleted from mongoDB"]
        except Exception as e:
            return [False , e]

    # ===================================== Update rule
    # update rule
    def update_rule(self, rule_id , new_rule , new_sev , new_desc):
        try:
            up = self.MongoClient.update_one({'_id':rule_id}, {'$set': {'rule' : new_rule , 'rule_severity' : new_sev , 'rule_description' : new_desc} },upsert=False)
            if up.modified_count > 0:
                return [True , "Rule ["+rule_id+"] updated from mongoDB"]
            else:
                return [False , "Rule ["+rule_id+"] failed updated from mongoDB"]

        except Exception as e:
            return [False , str(e)]

    # ===================================== Add rule
    # add rule to data base
    def add_rule(self, rule_name , rule , rule_severity , rule_description):
        try:
            # insert the rule to mongoDB
            rule_id = hashlib.md5(rule_name).hexdigest() # get md5 hash of rule and use it as rule id to avoid duplication

            self.MongoClient.insert_one({
                '_id' : rule_id,
                "rule" : rule,
                'rule_name' : rule_name,
                'rule_severity' : rule_severity,
                'rule_description' : rule_description
                })
            return [True , "Rule ["+rule_name+"] added to mongoDB"]

        except DuplicateKeyError:
            return [False , "Rule ["+rule_name+"] already exists"]

        except Exception as e:
            return [False , str(e)]


# generate the db rule object
def get_db_rules():
    return DB_Rules()



# =================================================
#               End Database Rules 
# =================================================








# =================================================
#               Database Parsers
# =================================================

# ===================================== DB Parsers
class DB_Parsers:
    
    collection = None
    def __init__(self):
        # if parsers collection not exists, add it to mongoDB
        db_m = MClient[DB_NAME]
        
        if 'parsers' not in db_m.list_collection_names():
            db_m.create_collection('parsers')

        self.collection = MClient[DB_NAME]["parsers"]


        # == check the parsers on parsers folder and if parser not on the DB
        # == read the configuration from the parser folder and push it to the DB
        # get all parsers on the parsers folder
        filenames   = os.listdir( app.config['PARSER_PATH'] ) 
        parsers     = []
        for files in filenames:
            if os.path.isdir( app.config['PARSER_PATH'] + "/" + files ):
                parsers.append( [ files ,  app.config['PARSER_PATH'] + "/" + files ] )


        # check if the parser on the DB, if not add it
        for p in parsers:
            try:
                # get the configuration file from the parser folder
                conf_path = p[1] + "/configuration.json" 
                if os.path.exists( conf_path ):
                    
                    with open( conf_path ) as conf_json_file:
                        conf_json = ast.literal_eval(conf_json_file.read())
                        for conf in conf_json:
                            parser = self.get_parser_by_name( conf['name'] )
                            if parser[0] == True and parser[1] is None: # if parser not found
                                # push the parser information to the DB
                                add_parser = self.add_parser( conf )
                                if add_parser[0]:
                                    logger.logger(level=logger.INFO , type="mongo_parsers", message="Parser ["+conf['name']+"] added to DB")
                                else:
                                    logger.logger(level=logger.ERROR , type="mongo_parsers", message="Failed to add parser ["+conf['name']+"]" , reason=add_parser[1])
                            elif parser[0] == False:
                                # if failed to get the parser information
                                logger.logger(level=logger.ERROR , type="mongo_parsers", message="Failed checking if parser ["+conf['name']+"] exists", reason=parser[1])
                            else:
                                # if the parser exists
                                logger.logger(level=logger.DEBUG , type="mongo_parsers", message="Parser ["+conf['name']+"] exists")
            except Exception as e:
                logger.logger(level=logger.ERROR , type="mongo_parsers", message="Failed loading parsers configurations", reason=str(e))


    # ===================================== add parser
    def add_parser(self, parser_details):
        try:
            parser_details['_id'] = parser_details['name']
            parser_details['creation_time']= str( datetime.now() ).replace(' ' , 'T')
            # insert into collection
            self.collection.insert_one(parser_details)

            
                    
            # write the parser configuration to file
            config = self.collection.find({'parser_folder' : parser_details['parser_folder']})
            parsers_configuration = []
            for c in config:
                parsers_configuration.append(c)
            
            
            configuration_file = open(app.config['PARSER_PATH'] + "/" + parser_details['parser_folder'] + "/configuration.json" , 'w')
            configuration_file.write(json.dumps(parsers_configuration))
            configuration_file.close()


            return [True, parser_details['_id'] ]

        except DuplicateKeyError:
            return [False, "Parser ["+parser_details['name']+"] already in DB."]
    
    # ===================================== edit parser
    def edit_parser(self, parser_name ,  parser_details):
        try:
            # update into collection
            up = self.collection.update_one({'_id':parser_name}, {'$set': parser_details},upsert=False)

            
            if up.modified_count > 0:

                    
                # write the parser configuration to file
                config = self.collection.find({'parser_folder' : parser_details['parser_folder']})
                parsers_configuration = []
                for c in config:
                    parsers_configuration.append(c)
                
                
                configuration_file = open(app.config['PARSER_PATH'] + "/" + parser_details['parser_folder'] + "/configuration.json" , 'w')
                configuration_file.write(json.dumps(parsers_configuration))
                configuration_file.close()

                logger.logger(level=logger.INFO , type="mongo_parsers", message="Parser configuration file ["+parser_details['parser_folder']+"/configuration.json] written")

                return [True , "Parser ["+parser_details['name']+"] updated from mongoDB"]
            else:
                return [False , "Parser ["+parser_details['name']+"] failed updated from mongoDB"]

        except Exception as e:
            return [False, str(e) ]


    # ===================================== get parsers details
    def get_parser(self):
        try:
            parsers = []
            for p in self.collection.find({}).sort([['creation_time', -1]]):
                parsers.append(p)
            
            return [True, parsers]
        except Exception as e:
            return [False , str(e)]

    
    # ===================================== get parser by name
    # get parser details by its name
    def get_parser_by_name(self, parser_name):
        try:
            for p in self.collection.find({}):
                if p['name'] == parser_name:
                    return [True, p]

            return [True , None]
        except Exception as e:
            return [False, str(e)]
    
    # ===================================== Delete parser
    # delete parser by name
    def delete_parser_by_name(self, parser_name):
        try:
            parser = self.collection.find({'_id' : parser_name})
            parser_details = None
            for c in parser:
                parser_details = c
            if parser_details is None:
                return [False , "Parser not found in database"]

            data = self.collection.delete_one({"_id": parser_name})

            logger.logger(level=logger.INFO , type="mongo_parsers", message="Parser ["+parser_name+"] removed from database")
            

            

            
                    
            # write the parser configuration to file
            config = self.collection.find({'parser_folder' : parser_details['parser_folder']})
            parsers_configuration = []
            for c in config:
                parsers_configuration.append(c)
            
            
            configuration_file = open(app.config['PARSER_PATH'] + "/" + parser_details['parser_folder'] + "/configuration.json" , 'w')
            configuration_file.write(json.dumps(parsers_configuration))
            configuration_file.close()
            logger.logger(level=logger.INFO , type="mongo_parsers", message="Parser ["+parser_name+"] configuration file updated")


            # if there is no other parser uses the same folder, then move it to temp folder
            if len(parsers_configuration) == 0:
                parser_folder = os.path.join(app.config['PARSER_PATH'],  parser_details['parser_folder'])
                shutil.move(parser_folder , app.config['PARSER_PATH'] + "/temp/" + parser_details['parser_folder'] )
                logger.logger(level=logger.INFO , type="mongo_parsers", message="Parser ["+parser_name+"] folder moved to temp folder")

            return [True , "Parser ["+parser_name+"] deleted"]
        except Exception as e:
            return [False , "Error: " + str(e)]




# this class contain the parsers
def get_db_parsers():
    return DB_Parsers()

# =================================================
#               End Database Parsers 
# =================================================








# =================================================
#               Database Files
# =================================================

#======================================= Machine files

class DB_Files:
    
    collection = None
    def __init__(self):

        # if files collection not exists, add it to mongoDB
        db_m = MClient[DB_NAME]
        if 'files' not in db_m.list_collection_names():
            db_m.create_collection('files')

        self.collection = MClient[DB_NAME]["files"]
        self.collection.ensure_index("machine_id")
        # Compound index with high-cardinality field first (file_path)
        self.collection.ensure_index([("file_path", 1), ("machine_id", 1)])
        # Avoid COLSCAN in get_by_status()
        self.collection.ensure_index("parsers.status")

    # ===================================== Get machine files
    # get all files for specified machine
    def get_by_machine(self, machine_id):
        try:
            files = []
            for file in self.collection.find({'machine_id': machine_id}):
                files.append(file)
            return [True, files]
        except Exception as e:
            return [False, str(e)]

    # ===================================== Disable/Enable file processing
    # this function will disable a file from being parsed by specific parser
    def disable_enable_file(self, machine_id, file_path, parser, disable):
        try:
            up = self.collection.update_one(
                {
                    "machine_id": machine_id,
                    "file_path": file_path,
                    "parsers.parser_name": parser
                },
                {
                    "$set": {
                        "parsers.$.disable": disable
                    }
                }
            )

            if up.modified_count > 0:
                s = "disabled" if disable else "enabled"
                return [True , "File ["+file_path+"] "+s+" on parser ["+parser+"]" ]
            else:
                return [False , "Failed to update the record"]

        except Exception as e:
            return [False , "Failed to update the record: " + str(e)]

    # ===================================== Add file
    # add file to the database or merge details with existing file document
    def add_file(self, machine_id, file_details):
        try:
            # If the file does not exist yet, add it
            res = self.collection.update_one(
                {
                    "machine_id": machine_id,
                    "file_path": file_details["file_path"]
                },
                {
                    "$setOnInsert": {
                        "machine_id": machine_id,
                        "file_path": file_details["file_path"],
                        "file_size": file_details["file_size"],
                        "parsers": [file_details["parsers"]]
                    }
                },
                upsert=True
            )

            if res.upserted_id is not None:
                return [True, "File added to database"]

            # If file exists in DB, add the new parser if it does not exist
            res = self.collection.update_one(
                {
                    "machine_id": machine_id,
                    "file_path": file_details["file_path"],
                    "parsers.parser_name": { "$ne": file_details["parsers"]["parser_name"] }
                },
                {
                    "$push": { "parsers": file_details["parsers"] }
                }
            )

            if res.modified_count > 0:
                return [True, "Parser added to file in database"]

            # File and parser exist, update the parser details
            res = self.collection.update_one(
                {
                    "machine_id": machine_id,
                    "file_path": file_details["file_path"],
                    "parsers.parser_name": file_details["parsers"]["parser_name"]
                },
                {
                    "$set": { "parsers.$": file_details["parsers"] }
                }
            )

            if res.modified_count > 0:
                return [True, "Parser details updated in database"]
            else:
                return [False, "Failed to update the record"]

        except Exception as e:
            return [False , "Error: " + str(e)]

    # ===================================== Get file details
    # get parsers details for all parsers by the file path
    def get_by_file_path(self, machine_id , file_path):
        try:
            for f in self.collection.find({'machine_id': machine_id, 'file_path': file_path}):
                return [True, f]

            return [True, None]
        except Exception as e:
            return [False, str(e)]

    # ===================================== Get parsing progress
    # get the status of parsers for specific machine
    def get_parsing_progress(self, machine_id):
        try:
            parsers_progress = {}
            for f in self.collection.find({'machine_id': machine_id}):
                for p in f['parsers']:
                    if p['parser_name'] not in parsers_progress.keys():
                        parsers_progress[ p['parser_name'] ] = []
                    parsers_progress[ p['parser_name'] ].append( {'file' : f['file_path'] , 'status' : p['status']} )

            return [True, parsers_progress]
        except Exception as e:
            return [False,  str(e)]

    # ==================================== Get files based on status
    # get files globally based on their status 
    def get_by_status(self, status):
        try:
            files = []
            for f in self.collection.find({"parsers.status": status},
                                          {"machine_id": 1, "file_path": 1, "file_size": 1,
                                           "parsers": { "$elemMatch": { "status": status } }}):
                for parser in f['parsers']:
                    files.append({
                        'machine_id': f['machine_id'],
                        'file_path': f['file_path'],
                        'file_size': f['file_size'],
                        'parser': parser
                    })

            return [True, files]
        except Exception as e:
            return [False,  str(e)]

    # ==================================== update file based on task id
    # update file status/message based on the provided task id
    def update_files_by_task_id(self, machine_id, task_id, status, message):
        try:
            self.collection.update_many(
                {
                    "machine_id": machine_id,
                    "parsers": {
                        "$elemMatch": {
                            "task_id": task_id,
                            "status": {"$in": ["queued", "parsing"]}
                        }
                    }
                },
                {
                    "$set": {
                        "parsers.$[elem].status": status,
                        "parsers.$[elem].message": message
                    }
                },
                array_filters=[{"elem.task_id": task_id, "elem.status": {"$in": ["queued", "parsing"]}}]
            )

            return [True, "Updated"]
        except Exception as e:
            return [False,  str(e)]


# this class contain the files
def get_db_files():
    return DB_Files()

# =================================================
#               End Database Files 
# =================================================


try:
    MClient = MongoClient("mongodb://" + DB_IP + ":" + str(DB_PORT) )
except Exception as e:
    # NOTE: pymongo v3 does not throw a connection exception here anymore
    MClient = None
    logger.logger(level=logger.ERROR , type="database", message="Failed to access to MongoDB " + str(DB_IP) + ":" + str(DB_PORT) , reason=str(e))
    

db_cases    = get_db_cases()    # get case database
db_rules    = get_db_rules()    # get rule database
db_files    = get_db_files()    # get files databse
db_parsers  = get_db_parsers()  # get parser database
db_groups   = get_db_groups()
