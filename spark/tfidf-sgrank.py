#
# TF-IDF among job ads, CVs, categories (all together)
#
from pyspark.sql import SparkSession
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, RegexTokenizer
from pyspark.sql.functions import *
from pyspark.sql.types import *

def calculate_similarity(vec_job, vec_cv):
    from scipy import spatial
    cv = vec_cv.toArray()
    jobs = vec_job.toArray()
    result = spatial.distance.cosine(cv, jobs)
    return 1.0 - float(result)    


def cosine_similarity(vec1, vec2):
    from sklearn.metrics.pairwise import cosine_similarity
    n_vec1 = list(map(float, vec1))
    n_vec2 = list(map(float, vec2))
    result = cosine_similarity(n_vec1, n_vec2)
    return float(result)    

NUM_FEATURES = 2**8

def sim_job_comp(spark):
    df_jobs = spark.read.json("RDD/nljobs.jsonl").filter("description is not NULL").cache()
    df_jobs.registerTempTable("jobs")
    df_comps = spark.read.json("RDD/competences.jsonl").cache()
    df_comps.registerTempTable("comps")

    job_schema = StructType([StructField("jobid", IntegerType(), True), StructField("cpid", IntegerType(), True),
                         StructField("category", IntegerType(), False), StructField("distance", FloatType(), True)])
    df_jobcomp_sim = spark.createDataFrame([], job_schema)
    num_jobs = df_jobs.count()
    print('### job size:' + str(num_jobs))
#    num_jobs = 100 # testing
    for job in range(0, num_jobs):
        df_job = df_jobs.where(df_jobs['jobid'] == job)
        df_job.registerTempTable("job")
        joined = spark.sql("SELECT description AS text, jobid AS id, 'job' AS type FROM job UNION ALL \
               SELECT text AS text, cpid AS id, 'comp' AS type FROM comps")
        
        regexTokenizer = RegexTokenizer(inputCol="text", outputCol="terms", pattern="\\W,")
        tokenized = regexTokenizer.transform(joined)
        
#        print('### ' + str(job) + ': head = ' + str(tokenized.head()))
        
        hashingTF = HashingTF(inputCol="terms", outputCol="rawFeatures", numFeatures=NUM_FEATURES)
        featurizedData = hashingTF.transform(tokenized)
        
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        idfModel = idf.fit(featurizedData)
        rescaledData = idfModel.transform(featurizedData)
  
        rescaledData.registerTempTable("resultTable")
        rs_job = spark.sql("SELECT features AS featuresJOB, id AS jobid FROM resultTable WHERE type = 'job'")
        rs_comps = spark.sql("SELECT features AS featuresCOMP, comp.cpid, category FROM resultTable AS rt\
        LEFT JOIN comps AS comp ON rt.id = comp.cpid WHERE type = 'comp'")
        
        #Calculate cv-category similarity START
        crossJoined_comp_job = rs_job.select("jobid", "featuresJOB").crossJoin(rs_comps.select("cpid", "featuresCOMP", "category"))
        calculatedDF_comp_job = crossJoined_comp_job.rdd\
        .map(lambda x: (x.jobid, x.cpid, x.category, cosine_similarity(x.featuresJOB, x.featuresCOMP)))\
        .toDF(["jobid", "cpid", "category", "distance"])
        df_jobcomp_sim = df_jobcomp_sim.union(calculatedDF_comp_job)
        #Calculate cv-category similarity END
    ordered_comp_job = df_jobcomp_sim.orderBy(asc("jobid"), asc("distance")).coalesce(2)
    ordered_comp_job.write.csv('results/sgrank-tfidf/nljob-comp')

def sim_cv_comp(spark):
    df_cvs = spark.read.json("RDD/cvs.jsonl").cache()
    df_cvs.registerTempTable("cvs")
    df_comps = spark.read.json("RDD/competences.jsonl").cache()
    df_comps.registerTempTable("comps")

    ### CV vs COMP    
    cv_schema = StructType([StructField("cvid", IntegerType(), True), StructField("cpid", IntegerType(), True),
                         StructField("category", IntegerType(), False), StructField("distance", FloatType(), True)])
    df_cvcomp_sim = spark.createDataFrame([], cv_schema)
    num_cvs = df_cvs.count()
    print('### CV size:' + str(num_cvs))
    for cv in range(0, num_cvs):
        df_cv = df_cvs.where(df_cvs['cvid'] == cv)
#        print('### CV DF size:' + str(df_cv.count()))
        df_cv.registerTempTable("cv")
        joined = spark.sql("SELECT description AS text, cvid AS id, 'cv' AS type FROM cv UNION ALL \
               SELECT text AS text, cpid AS id, 'comp' AS type FROM comps")
        
        regexTokenizer = RegexTokenizer(inputCol="text", outputCol="terms", pattern="\\W,")
        tokenized = regexTokenizer.transform(joined)
        
        hashingTF = HashingTF(inputCol="terms", outputCol="rawFeatures", numFeatures=NUM_FEATURES)
        featurizedData = hashingTF.transform(tokenized)
        
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        idfModel = idf.fit(featurizedData)
        rescaledData = idfModel.transform(featurizedData)
  
        rescaledData.registerTempTable("resultTable")
        cv = spark.sql("SELECT features AS featuresCV, id AS cvid FROM resultTable WHERE type = 'cv'")
        comps = spark.sql("SELECT features AS featuresCOMP, comp.cpid, category FROM resultTable AS rt\
        LEFT JOIN comps AS comp ON rt.id = comp.cpid WHERE type = 'comp'")
        
        #Calculate cv-comp similarity START
        crossJoined_comp_cv = cv.select("cvid", "featuresCV").crossJoin(comps.select("cpid", "featuresCOMP", "category"))
        calculatedDF_comp_cv = crossJoined_comp_cv.rdd\
        .map(lambda x: (x.cvid, x.cpid, x.category, cosine_similarity(x.featuresCV, x.featuresCOMP)))\
        .toDF(["cvid", "cpid", "category", "distance"])
        df_cvcomp_sim = df_cvcomp_sim.union(calculatedDF_comp_cv)
        #Calculate cv-comp similarity END
    ordered_comp_cv = df_cvcomp_sim.orderBy(asc("cvid"), asc("distance")).coalesce(2)
    ordered_comp_cv.write.csv('results/sgrank-tfidf/cv-comp')    
    
def sim_job_cv(spark):
    df_jobs = spark.read.json("RDD/nljobs.jsonl").filter("description is not NULL").cache()
    df_jobs.registerTempTable("jobs")
    df_cvs = spark.read.json("RDD/cvs.jsonl").cache()
    df_cvs.registerTempTable("cvs")

    jobcv_schema = StructType([StructField("jobid", IntegerType(), True), StructField("cvid", IntegerType(), True),
                         StructField("distance", FloatType(), True)])
    df_jobcv_sim = spark.createDataFrame([], jobcv_schema)
    num_jobs = df_jobs.count()
    print('### job size:' + str(num_jobs))
    for job in range(0, num_jobs):
        df_job = df_jobs.where(df_jobs['jobid'] == job)
        df_job.registerTempTable("job")
        joined = spark.sql("SELECT description AS text, jobid AS id, 'job' AS type FROM job UNION ALL \
               SELECT description AS text, cvid AS id, 'cv' AS type FROM cvs")
        
        regexTokenizer = RegexTokenizer(inputCol="text", outputCol="terms", pattern="\\W,")
        tokenized = regexTokenizer.transform(joined)
        
        hashingTF = HashingTF(inputCol="terms", outputCol="rawFeatures", numFeatures=NUM_FEATURES)
        featurizedData = hashingTF.transform(tokenized)
        
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        idfModel = idf.fit(featurizedData)
        rescaledData = idfModel.transform(featurizedData)
  
        rescaledData.registerTempTable("resultTable")
        rs_job = spark.sql("SELECT features AS featuresJOB, id AS jobid FROM resultTable WHERE type = 'job'")
        rs_cvs = spark.sql("SELECT features AS featuresCV, cv.cvid FROM resultTable AS rt\
        LEFT JOIN cvs AS cv ON rt.id = cv.cvid WHERE type = 'cv'")
        
        #Calculate cv-category similarity START
        crossJoined_job_cv = rs_job.select("jobid", "featuresJOB").crossJoin(rs_cvs.select("cvid", "featuresCV"))
        calculatedDF_job_cv = crossJoined_job_cv.rdd\
        .map(lambda x: (x.jobid, x.cvid, cosine_similarity(x.featuresJOB, x.featuresCV)))\
        .toDF(["jobid", "cvid", "distance"])
        df_jobcv_sim = df_jobcv_sim.union(calculatedDF_job_cv)
        #Calculate cv-category similarity END
    ordered_job_cv = df_jobcv_sim.orderBy(asc("jobid"), asc("distance")).coalesce(2)
    ordered_job_cv.write.csv('results/sgrank-tfidf/nljob-cv')


def main():
    spark = SparkSession.builder \
        .appName("Spark CV-job ad matching") \
        .config("spark.some.config.option", "some-value") \
        .master("local[*]") \
        .getOrCreate()

#    NUM_FEATURES = 2**6

#    df_jobs = spark.read.json("RDD/jobs.jsonl").filter("description is not NULL").cache()
#    df_jobs.registerTempTable("jobs")
#    df_cvs = spark.read.json("RDD/cvs.jsonl").cache()
#    df_cvs.registerTempTable("cvs")
#    df_kags = spark.read.json("RDD/kags.jsonl").cache()
#    df_kags.registerTempTable("kags")
#    df_comps = spark.read.json("RDD/competences.jsonl").cache()
#    df_comps.registerTempTable("comps")

#    df_jobmarket = spark.read.json("RDD/job_market.jsonl").filter("description is not NULL").cache()
#    df_jobmarket.registerTempTable("job_market")
#    df_categs = spark.read.json("RDD/categories.jsonl").cache()
#    df_categs.registerTempTable("categories")
#    ### Competences2 following changes in Yuri's documents
#    df_comps2 = spark.read.json("RDD/competences2.jsonl").cache()
#    df_comps2.registerTempTable("comps2")
#    ### Testing new jobs
#    df_newjobs = spark.read.json("RDD/newjobs.jsonl").filter("description is not NULL").cache()
#    df_newjobs.registerTempTable("newjobs")
    
 
#    ### CV vs KAG    
#    cv_schema = StructType([StructField("cvid", IntegerType(), True), StructField("kagid", IntegerType(), True),
#                         StructField("category", IntegerType(), False), StructField("distance", FloatType(), True)])
#    df_cvkag_sim = spark.createDataFrame([], cv_schema)
#    num_cvs = df_cvs.count()
#    print('### CV size:' + str(num_cvs))
#    for cv in range(0, num_cvs):
#        df_cv = df_cvs.where(df_cvs['cvid'] == cv)
##        print('### CV DF size:' + str(df_cv.count()))
#        df_cv.registerTempTable("cv")
#        joined = spark.sql("SELECT description AS text, cvid AS id, 'cv' AS type FROM cv UNION ALL \
#               SELECT text AS text, id AS id, 'kags' AS type FROM kags")
#        
#        regexTokenizer = RegexTokenizer(inputCol="text", outputCol="terms", pattern="\\W,")
#        tokenized = regexTokenizer.transform(joined)
##        tokenizer = Tokenizer(inputCol="text", outputCol="terms")
##        tokenized = tokenizer.transform(joined)
##        print('### ' + str(cv) + ': head = ' + str(tokenized.head()))
##        tokenized.head()
#        
#        hashingTF = HashingTF(inputCol="terms", outputCol="rawFeatures", numFeatures=NUM_FEATURES)
#        featurizedData = hashingTF.transform(tokenized)
##        print('### ' + str(cv) + ': features = ' + str(hashingTF.getNumFeatures()))
#        
#        idf = IDF(inputCol="rawFeatures", outputCol="features")
#        idfModel = idf.fit(featurizedData)
#        rescaledData = idfModel.transform(featurizedData)
#  
#        rescaledData.registerTempTable("resultTable")
#        cv = spark.sql("SELECT features AS featuresCV, id AS cvid FROM resultTable WHERE type = 'cv'")
#        kags = spark.sql("SELECT features AS featuresKAG, kag.id, category FROM resultTable AS rt\
#        LEFT JOIN kags AS kag ON rt.id = kag.id WHERE type = 'kags'")
#        
#        #Calculate cv-category similarity START
#        crossJoined_cat_cv = cv.select("cvid", "featuresCV").crossJoin(kags.select("id", "featuresKAG", "category"))
#        calculatedDF_cat_cv = crossJoined_cat_cv.rdd\
#        .map(lambda x: (x.cvid, x.id, x.category, calculate_distance(x.featuresCV, x.featuresKAG)))\
#        .toDF(["cvid", "kagid", "category", "distance"])
##        print('### DataFrame size:' + str(calculatedDF_cat_cv.count()))
#        df_cvkag_sim = df_cvkag_sim.union(calculatedDF_cat_cv)
#        #Calculate cv-category similarity END
#    ordered_cat_cv = df_cvkag_sim.orderBy(asc("cvid"), asc("distance")).coalesce(2)
#    ordered_cat_cv.write.csv('results/sgrank-tfidf/cv-kag')
   
#    ### JOB vs KAG    
#    job_schema = StructType([StructField("jobid", IntegerType(), True), StructField("kagid", IntegerType(), True),
#                         StructField("category", IntegerType(), False), StructField("distance", FloatType(), True)])
#    df_jobkag_sim = spark.createDataFrame([], job_schema)
#    num_jobs = df_jobs.count()
#    print('### job size:' + str(num_jobs))
#    num_jobs = 100 # testing
#    for job in range(0, num_jobs):
#        df_job = df_jobs.where(df_jobs['jobid'] == job)
#        df_job.registerTempTable("job")
#        joined = spark.sql("SELECT description AS text, jobid AS id, 'job' AS type FROM job UNION ALL \
#               SELECT text AS text, id AS id, 'kags' AS type FROM kags")
#        
#        regexTokenizer = RegexTokenizer(inputCol="text", outputCol="terms", pattern="\\W,")
#        tokenized = regexTokenizer.transform(joined)
#        
#        hashingTF = HashingTF(inputCol="terms", outputCol="rawFeatures", numFeatures=NUM_FEATURES)
#        featurizedData = hashingTF.transform(tokenized)
#        
#        idf = IDF(inputCol="rawFeatures", outputCol="features")
#        idfModel = idf.fit(featurizedData)
#        rescaledData = idfModel.transform(featurizedData)
#  
#        rescaledData.registerTempTable("resultTable")
#        job = spark.sql("SELECT features AS featuresJOB, id AS jobid FROM resultTable WHERE type = 'job'")
#        kags = spark.sql("SELECT features AS featuresKAG, kag.id, category FROM resultTable AS rt\
#        LEFT JOIN kags AS kag ON rt.id = kag.id WHERE type = 'kags'")
#        
#        #Calculate cv-category similarity START
#        crossJoined_cat_job = job.select("jobid", "featuresJOB").crossJoin(kags.select("id", "featuresKAG", "category"))
#        calculatedDF_cat_job = crossJoined_cat_job.rdd\
#        .map(lambda x: (x.jobid, x.id, x.category, calculate_distance(x.featuresJOB, x.featuresKAG)))\
#        .toDF(["jobid", "kagid", "category", "distance"])
#        df_jobkag_sim = df_jobkag_sim.union(calculatedDF_cat_job)
#        #Calculate cv-category similarity END
#    ordered_cat_job = df_jobkag_sim.orderBy(asc("jobid"), asc("distance")).coalesce(2)
#    ordered_cat_job.write.csv('results/sgrank-tfidf/job-kag')


#   ### JOB MARKET vs COMP    
#    job_schema = StructType([StructField("jobid", IntegerType(), True), StructField("cpid", IntegerType(), True),
#                         StructField("category", IntegerType(), False), StructField("distance", FloatType(), True)])
#    df_mktcomp_sim = spark.createDataFrame([], job_schema)
#    num_jobmarket = df_jobmarket.count()
#    print('### job market size:' + str(num_jobmarket))
#    for job in range(0, num_jobmarket):
#        df_jmarket = df_jobmarket.where(df_jobmarket['jobid'] == job)
#        df_jmarket.registerTempTable("jmarket")
#        joined = spark.sql("SELECT description AS text, jobid AS id, 'market' AS type FROM jmarket UNION ALL \
#               SELECT text AS text, cpid AS id, 'comp' AS type FROM comps")
#        
#        regexTokenizer = RegexTokenizer(inputCol="text", outputCol="terms", pattern="\\W,")
#        tokenized = regexTokenizer.transform(joined)
#        
#        hashingTF = HashingTF(inputCol="terms", outputCol="rawFeatures", numFeatures=NUM_FEATURES)
#        featurizedData = hashingTF.transform(tokenized)
#        
#        idf = IDF(inputCol="rawFeatures", outputCol="features")
#        idfModel = idf.fit(featurizedData)
#        rescaledData = idfModel.transform(featurizedData)
#  
#        rescaledData.registerTempTable("resultTable")
#        jmarket = spark.sql("SELECT features AS featuresJOB, id AS jobid FROM resultTable WHERE type = 'market'")
#        comps = spark.sql("SELECT features AS featuresCOMP, comp.cpid, category FROM resultTable AS rt\
#        LEFT JOIN comps AS comp ON rt.id = comp.cpid WHERE type = 'comp'")
#        
#        #Calculate cv-category similarity START
#        crossJoined_comp_jmarket = jmarket.select("jobid", "featuresJOB").crossJoin(comps.select("cpid", "featuresCOMP", "category"))
#        calculatedDF_comp_jmarket = crossJoined_comp_jmarket.rdd\
#        .map(lambda x: (x.jobid, x.cpid, x.category, calculate_distance(x.featuresJOB, x.featuresCOMP)))\
#        .toDF(["jobid", "cpid", "category", "distance"])
#        df_mktcomp_sim = df_mktcomp_sim.union(calculatedDF_comp_jmarket)
#        #Calculate cv-category similarity END
#    ordered_comp_jmkt = df_mktcomp_sim.orderBy(asc("jobid"), asc("distance")).coalesce(2)
#    ordered_comp_jmkt.write.csv('results/sgrank-tfidf/jmkt-comp')

#   ### CATEGORY vs COMP    
#    cat_schema = StructType([StructField("catid", IntegerType(), True), StructField("cpid", IntegerType(), True),
#                         StructField("category", IntegerType(), False), StructField("distance", FloatType(), True)])
#    df_catcomp_sim = spark.createDataFrame([], cat_schema)
#    num_cats = df_categs.count()
#    print('### job market size:' + str(num_cats))
#    for cat in range(0, num_cats):
#        df_cat = df_categs.where(df_categs['catid'] == cat)
#        df_cat.registerTempTable("tt_cat")
#        joined = spark.sql("SELECT text AS text, catid AS id, 'category' AS type FROM tt_cat UNION ALL \
#               SELECT text AS text, cpid AS id, 'comp' AS type FROM comps")
#        
#        regexTokenizer = RegexTokenizer(inputCol="text", outputCol="terms", pattern="\\W,")
#        tokenized = regexTokenizer.transform(joined)
#        
#        hashingTF = HashingTF(inputCol="terms", outputCol="rawFeatures", numFeatures=NUM_FEATURES)
#        featurizedData = hashingTF.transform(tokenized)
#        
#        idf = IDF(inputCol="rawFeatures", outputCol="features")
#        idfModel = idf.fit(featurizedData)
#        rescaledData = idfModel.transform(featurizedData)
#  
#        rescaledData.registerTempTable("resultTable")
#        rs_cat = spark.sql("SELECT features AS featuresCAT, id AS catid FROM resultTable WHERE type = 'category'")
#        comps = spark.sql("SELECT features AS featuresCOMP, comp.cpid, category FROM resultTable AS rt\
#        LEFT JOIN comps AS comp ON rt.id = comp.cpid WHERE type = 'comp'")
#        
#        #Calculate cv-category similarity START
#        crossJoined_comp_cat = rs_cat.select("catid", "featuresCAT").crossJoin(comps.select("cpid", "featuresCOMP", "category"))
#        calculatedDF_comp_cat = crossJoined_comp_cat.rdd\
#        .map(lambda x: (x.catid, x.cpid, x.category, calculate_distance(x.featuresCAT, x.featuresCOMP)))\
#        .toDF(["jobid", "cpid", "category", "distance"])
#        df_catcomp_sim = df_catcomp_sim.union(calculatedDF_comp_cat)
#        #Calculate cv-category similarity END
#    ordered_comp_cat = df_catcomp_sim.orderBy(asc("catid"), asc("distance")).coalesce(2)
#    ordered_comp_cat.write.csv('results/sgrank-tfidf/cat-comp')

#   ### JOB vs COMP2    
#    job_schema = StructType([StructField("jobid", IntegerType(), True), StructField("cpid", IntegerType(), True),
#                         StructField("category", IntegerType(), False), StructField("distance", FloatType(), True)])
#    df_jobcomp_sim = spark.createDataFrame([], job_schema)
#    num_jobs = df_jobs.count()
#    num_comps = df_comps2.count()
#    print('### job size:' + str(num_jobs) + '\tcomp size: ' + str(num_comps))
#    num_jobs = 100 # testing
#    for job in range(0, num_jobs):
#        df_job = df_jobs.where(df_jobs['jobid'] == job)
#        df_job.registerTempTable("job")
#        joined = spark.sql("SELECT description AS text, jobid AS id, 'job' AS type FROM job UNION ALL \
#               SELECT text AS text, cpid AS id, 'comp' AS type FROM comps2")
#        
#        regexTokenizer = RegexTokenizer(inputCol="text", outputCol="terms", pattern="\\W,")
#        tokenized = regexTokenizer.transform(joined)
#        
#        hashingTF = HashingTF(inputCol="terms", outputCol="rawFeatures", numFeatures=NUM_FEATURES)
#        featurizedData = hashingTF.transform(tokenized)
#        
#        idf = IDF(inputCol="rawFeatures", outputCol="features")
#        idfModel = idf.fit(featurizedData)
#        rescaledData = idfModel.transform(featurizedData)
#  
#        rescaledData.registerTempTable("resultTable")
#        rs_job = spark.sql("SELECT features AS featuresJOB, id AS jobid FROM resultTable WHERE type = 'job'")
#        rs_comp = spark.sql("SELECT features AS featuresCOMP, comp.cpid, category FROM resultTable AS rt\
#        LEFT JOIN comps2 AS comp ON rt.id = comp.cpid WHERE type = 'comp'")
#        
#        #Calculate job-competence similarity START
#        crossJoined_comp_job = rs_job.select("jobid", "featuresJOB").crossJoin(rs_comp.select("cpid", "featuresCOMP", "category"))
#        calculatedDF_comp_job = crossJoined_comp_job.rdd\
#        .map(lambda x: (x.jobid, x.cpid, x.category, calculate_distance(x.featuresJOB, x.featuresCOMP)))\
#        .toDF(["jobid", "cpid", "category", "distance"])
#        df_jobcomp_sim = df_jobcomp_sim.union(calculatedDF_comp_job)
#        #Calculate job-competence similarity END
##    ordered_comp_job = df_jobcomp_sim.orderBy(asc("jobid"), asc("distance")).coalesce(2)
#    ordered_comp_job = df_jobcomp_sim.orderBy(asc("jobid")).coalesce(2)
#    ordered_comp_job.write.csv('results/sgrank-tfidf/job-comp2')



    sim_cv_comp(spark)
    sim_job_comp(spark)
    sim_job_cv(spark)
    

if __name__ == '__main__':
    main()
