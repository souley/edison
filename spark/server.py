#Http server that serves data from local mongodb
#Example usage:
#python3 server.py
from flask import Flask, request, jsonify
import subprocess
import json
from pymongo import MongoClient
from bson.json_util import loads
import re
from flask_cors import CORS, cross_origin

app = Flask(__name__)
CORS(app)
HOST = "127.0.0.1"
PORT = 5000
client = MongoClient('localhost', 27017)
db = client['edison-database']

def get_max_catid():
    return db['competences'].find().sort("cpid", -1).limit(1)[0]["cpid"]

def validate_method(method):
    if method not in ['tfidf', 'jacc', 'sgrank', 'lda', 'doc2vec', 'tfidf2']:
        raise

def validate_cats(cat_id_1, cat_id_2, cat_id_3, cat_id_4, cat_id_5):
    #make sure they are distinct
    if len(set([cat_id_1, cat_id_2, cat_id_3, cat_id_4, cat_id_5])) != 5:
        raise
    #make sure they are in accepted range
    if max(cat_id_1, cat_id_2, cat_id_3, cat_id_4, cat_id_5) > get_max_catid():
        raise

@app.route('/job/<id>', methods=['GET'])
def job_by_id(id):
    if request.method == 'GET':
        try:
            collection = db['raw_jobs']
            doc = collection.find_one({"jobid":int(id)})
            if not doc:
                return jsonify({"response": {}, "statusCode": 404, "message": "Not found"})
            obj = {"jobid": doc['jobid'], "description": doc['description']}
            return jsonify({"response": obj, "statusCode": 200})
        except ValueError:
            return jsonify({"response": {}, "statusCode": 404, "message": "Value error"})
        except:
            return jsonify({"response": {}, "statusCode": 404, "message": "Generic"})

@app.route('/cv/<cvid>', methods=['GET'])
def cv_by_id(cvid):
    if request.method == 'GET':
        try:
            collection = db['raw_cvs']
            doc = collection.find_one({"cvid":int(cvid)})
            if not doc:
                return jsonify({"response": {}, "statusCode": 404, "message": "Not found"})
            obj = {"cvid": doc['cvid'], "description": doc['description']}
            return jsonify({"response": obj, "statusCode": 200})
        except ValueError:
            return jsonify({"response": {}, "statusCode": 404, "message": "Value error"})
        except:
            return jsonify({"response": {}, "statusCode": 404, "message": "Generic"})


@app.route('/skill/<name>', methods=['GET'])
def cat_by_name(name):
    if request.method == 'GET':
        try:
            collection = db['competences']
            doc = collection.find_one({"category":name})
            if not doc:
                return jsonify({"response": {}, "statusCode": 404, "message": "Not found"})
            obj = {"skillName": doc['category'], "cpid": doc['catid'], "category": doc['category']}
            return jsonify({"response": obj, "statusCode": 200})
        except:
            return jsonify({"response": {}, "statusCode": 404, "message": "Generic"})

@app.route('/skillrec', methods=['GET'])
def skillrec():
    if request.method == 'GET':
        try:
            q = request.args['q']
            regex = re.compile('^' + re.escape(q) + '.*', re.IGNORECASE)
            items = db['competences'].find({'category': { '$regex': regex }}).limit(5)
            if items.count() == 0:
                return jsonify(response=[], statusCode=404, message="Not found")
            else:
                skills_arr = []
                for item in items:
                    item.pop('category', None)
                    item.pop('_id', None)
                    skills_arr.append(item)
                return jsonify(response=skills_arr, statusCode=200)
        except:
            return jsonify(response=[], statusCode=404, message="Generic")

#@app.route('/edisongraph', methods=['POST'])
#def edison_graph():
#    if request.method == 'POST':
#        try:
#            json_data = json.loads(request.get_data().decode('utf-8'))
#            job_id = int(json_data['jobid'])
#            method = json_data['method']
#            validate_method(method)
#            pagenum = int(json_data['pagenum'])
#            PAGESIZE = 6
#            #get job-category similarities
#            job_cat_diffs = db[method +'_jobcomp'].find({"jobid": job_id}, {"distance": 1, "_id": 0, "category": 1})\
#                .sort("distance", 1)
#            job_diff_obj = {}
#            for job_cat_diff in job_cat_diffs:
#                job_diff_obj[job_cat_diff['category']] = job_cat_diff['distance']
#
#            #Fetch  best CVs for this job
#            cv_differences = []
#            cvs = db[method + '_jobcv'].find({"jobid": job_id}).sort("distance", 1).skip(PAGESIZE*(pagenum-1)).limit(PAGESIZE)
#            print(cvs.count())
#            for cv in cvs:
#                print(cv["cvid"])
#                cv_obj = {"cvid": cv['cvid'], "job_distance": cv["distance"]}
#                cv_cat_diffs = db[method + '_cvcomp'].find({"cvid": cv['cvid']}, {"_id":0, "distance": 1, "category": 1})
#                skill_differences = {}
#                for cv_cat_diff in cv_cat_diffs:
#                    skill_differences[cv_cat_diff['category']] = cv_cat_diff['distance']
#                cv_obj["skill_differences"] = skill_differences
#                cv_differences.append(cv_obj)
#
#                final_obj = {"job_diff": job_diff_obj, "cv_differences": cv_differences}
#            return jsonify({"response": final_obj,"statusCode": 200})
#        except KeyError:
#            return jsonify({"response": {}, "statusCode": 404, "message": "Key error"})
#        except ValueError:
#            return jsonify({"response": {}, "statusCode": 404, "message": "Value error"})
#        except:
#            return jsonify({"response": {}, "statusCode": 404, "message": "Generic"})

@app.route('/edisongraph', methods=['POST'])
def edison_graph():
    if request.method == 'POST':
        try:
            json_data = json.loads(request.get_data().decode('utf-8'))
            job_id = int(json_data['jobid'])
            method = json_data['method']
            print ('===METHOD: ' + method)
            validate_method(method)
            pagenum = int(json_data['pagenum'])
            PAGESIZE = 6
            #get job-category similarities
            job_cat_diffs = db[method +'_jobcomp'].find({"jobid": job_id}, {"distance": 1, "_id": 0, "category": 1})\
                .sort("distance", 1)
            job_diff_obj = {}
            for job_cat_diff in job_cat_diffs:
                job_diff_obj[job_cat_diff['category']] = job_cat_diff['distance']

            #Fetch  best CVs for this job
            cv_differences = []
            cvs = db[method + '_jobcv'].find({"jobid": job_id}).sort("distance", 1).skip(PAGESIZE*(pagenum-1)).limit(PAGESIZE)
            print(cvs.count())
            for cv in cvs:
                print(cv["cvid"])
                cv_obj = {"cvid": cv['cvid'], "job_distance": cv["distance"]}
                cv_cat_diffs = db[method + '_cvcomp'].find({"cvid": cv['cvid']}, {"_id":0, "distance": 1, "category": 1})
                skill_differences = {}
                for cv_cat_diff in cv_cat_diffs:
                    skill_differences[cv_cat_diff['category']] = cv_cat_diff['distance']
                cv_obj["skill_differences"] = skill_differences
                cv_differences.append(cv_obj)

                final_obj = {"job_diff": job_diff_obj, "cv_differences": cv_differences}
            return jsonify({"response": final_obj,"statusCode": 200})
        except KeyError:
            return jsonify({"response": {}, "statusCode": 404, "message": "Key error"})
        except ValueError:
            return jsonify({"response": {}, "statusCode": 404, "message": "Value error"})
        except:
            return jsonify({"response": {}, "statusCode": 404, "message": "Generic"})

@app.route('/edisongraph4cat', methods=['POST'])
def edison_graph_cat():
    if request.method == 'POST':
        try:
            json_data = json.loads(request.get_data().decode('utf-8'))
            catid = int(json_data['catid'])
            method = json_data['method']
            validate_method(method)
            pagenum = int(json_data['pagenum'])
            #get cat-comp similarities
            cat_comp_sims = db[method +'_catcomp'].find({"catid": catid})\
                .sort("distance", 1)
            cat_comp_sims_obj = {}
            for cat_comp_sim in cat_comp_sims:
                cat_comp_sims_obj[cat_comp_sim['category']] = cat_comp_sim['distance']

            #get mrkt-comp similarities
            mkt_comp_sims = db[method +'_jmktcomp'].find({"jobid": 0})\
                .sort("distance", 1)
            mkt_comp_sims_obj = {}
            for mkt_comp_sim in mkt_comp_sims:
                mkt_comp_sims_obj[mkt_comp_sim['category']] = mkt_comp_sim['distance']


            final_obj = {"cat_sims": cat_comp_sims_obj, "mkt_sims": mkt_comp_sims_obj}
            return jsonify({"response": final_obj,"statusCode": 200})
        except KeyError:
            return jsonify({"response": {}, "statusCode": 404, "message": "Key error"})
        except ValueError:
            return jsonify({"response": {}, "statusCode": 404, "message": "Value error"})
        except:
            return jsonify({"response": {}, "statusCode": 404, "message": "Generic"})

#@app.route('/edisonskills', methods=['GET'])
#def edison_skills():
#    if request.method == 'GET':
#        try:
#            skills = db['allcategories'].find({"category": "Edison"}, {"skillName": 1, "_id": 0})
#            if skills.count() == 0:
#                return jsonify({"response": {}, "statusCode": 404, "message": "Not found"})
#            return jsonify(response=list(skills), statusCode=200)
#
#        except:
#            return jsonify({"response": {}, "statusCode": 404, "message": "Generic"})

@app.route('/edisongraphcv', methods=['POST'])
def edison_graph_by_cv():
    if request.method == 'POST':
        try:
            json_data = json.loads(request.get_data().decode('utf-8'))
            jobid = int(json_data['jobid'])
            cvid = int(json_data['cvid'])
            method = json_data['method']
            validate_method(method)
            job_cat_diffs = db[method +'_newjobcomp'].find({"jobid": jobid}, {"distance": 1, "_id": 0, "category": 1}).sort("distance", 1)
            job_diff_obj = {}
            for job_cat_diff in job_cat_diffs:
                job_diff_obj[job_cat_diff['category']] = job_cat_diff['distance']
            cv_cat_diffs = db[method + '_cvcomp'].find({"cvid": cvid}, {"_id":0, "distance": 1, "category": 1})
            skill_differences = {}
            for cv_cat_diff in cv_cat_diffs:
                skill_differences[cv_cat_diff['category']] = cv_cat_diff['distance']
            final_obj = {"job_diff": job_diff_obj, "skill_differences": skill_differences, "cvid": cvid}
            return jsonify({"response": final_obj,"statusCode": 200})
        except ValueError:
            return jsonify({"response": {}, "statusCode": 404, "message": "Value error"})
        except:
            return jsonify({"response": {}, "statusCode": 404, "message": "Generic"})



@app.route('/singleedisongraphcv', methods=['POST'])
def single_edison_graph_by_cv():
    if request.method == 'POST':
        try:
            json_data = json.loads(request.get_data().decode('utf-8'))
            jobid = int(json_data['jobid'])
            cvid = int(json_data['cvid'])
            method = json_data['method']
            validate_method(method)
            cat_id_1 = int(json_data['cat_id_1'])
            cat_id_2 = int(json_data['cat_id_2'])
            cat_id_3 = int(json_data['cat_id_3'])
            cat_id_4 = int(json_data['cat_id_4'])
            cat_id_5 = int(json_data['cat_id_5'])
            validate_cats(cat_id_1, cat_id_2, cat_id_3, cat_id_4, cat_id_5)
            print(1)
            job_cat_1_diff = db[method +'_jobcomp'].find_one({"jobid": jobid, "cpid": cat_id_1})
            job_cat_2_diff = db[method +'_jobcomp'].find_one({"jobid": jobid, "cpid": cat_id_2})
            job_cat_3_diff = db[method +'_jobcomp'].find_one({"jobid": jobid, "cpid": cat_id_3})
            job_cat_4_diff = db[method +'_jobcomp'].find_one({"jobid": jobid, "cpid": cat_id_4})
            job_cat_5_diff = db[method +'_jobcomp'].find_one({"jobid": jobid, "cpid": cat_id_5})

            job_diff_obj = {}

            job_diff_obj[job_cat_1_diff['category']] = job_cat_1_diff['distance']
            job_diff_obj[job_cat_2_diff['category']] = job_cat_2_diff['distance']
            job_diff_obj[job_cat_3_diff['category']] = job_cat_3_diff['distance']
            job_diff_obj[job_cat_4_diff['category']] = job_cat_4_diff['distance']
            job_diff_obj[job_cat_5_diff['category']] = job_cat_5_diff['distance']

            cv_cat_1_diff = db[method + '_cvcomp'].find_one({"cvid": cvid, "cpid": cat_id_1})
            cv_cat_2_diff = db[method + '_cvcomp'].find_one({"cvid": cvid, "cpid": cat_id_2})
            cv_cat_3_diff = db[method + '_cvcomp'].find_one({"cvid": cvid, "cpid": cat_id_3})
            cv_cat_4_diff = db[method + '_cvcomp'].find_one({"cvid": cvid, "cpid": cat_id_4})
            cv_cat_5_diff = db[method + '_cvcomp'].find_one({"cvid": cvid, "cpid": cat_id_5})

            skill_differences = {}
            skill_differences[cv_cat_1_diff['category']] = cv_cat_1_diff['distance']
            skill_differences[cv_cat_2_diff['category']] = cv_cat_2_diff['distance']
            skill_differences[cv_cat_3_diff['category']] = cv_cat_3_diff['distance']
            skill_differences[cv_cat_4_diff['category']] = cv_cat_4_diff['distance']
            skill_differences[cv_cat_5_diff['category']] = cv_cat_5_diff['distance']

            final_obj = {"job_diff": job_diff_obj, "skill_differences": skill_differences, "cvid": cvid}
            return jsonify({"response": final_obj,"statusCode": 200})
        except ValueError:
            return jsonify({"response": {}, "statusCode": 404, "message": "Value error"})
        except:
            return jsonify({"response": {}, "statusCode": 404, "message": "Generic"})








if __name__ == "__main__":
    app.run(host=HOST, port=PORT, threaded=True)
