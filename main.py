import pyspark
from pyspark.sql import SparkSession

#creation of spark session
spark = SparkSession.builder.master("local[1]") \
                    .appName('practice') \
                    .config("spark.sql.warehouse.dir", "home/spark-warehouse") \
                    .enableHiveSupport() \
                    .getOrCreate()

#creation of simple dataframe
df = spark.createDataFrame(
    [("Scala", 25000), ("Spark", 35000), ("PHP", 21000)])
df.show()

#creation of dataframe with column names
df = spark.createDataFrame(
    [
        (1, "foo"),  # create your data here, be consistent in the types.
        (2, "bar"),
    ],
    ["id", "label"]  # add your column names here
)
df.show()

#creation of dataframe with column datatypes
df = spark.createDataFrame(
    [
        (1, "foo"),  # Add your data here
        (2, "bar"),
    ],
    T.StructType(  # Define the whole schema within a StructType
        [
            T.StructField("id", T.IntegerType(), True),
            T.StructField("label", T.StringType(), True),
        ]
    ),
)

#Structtype of creating fields
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data2 = [("James", "", "Smith", "36636", "M", 3000),
         ("Michael", "Rose", "", "40288", "M", 4000),
         ("Robert", "", "Williams", "42114", "M", 4000),
         ("Maria", "Anne", "Jones", "39192", "F", 4000),
         ("Jen", "Mary", "Brown", "", "F", -1)
         ]

schema = StructType([ \
    StructField("firstname", StringType(), True), \
    StructField("middlename", StringType(), True), \
    StructField("lastname", StringType(), True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
    ])

df = spark.createDataFrame(data=data2, schema=schema)

#create dataframe from csv
df2 = spark.read.csv("/src/resources/file.csv")

#create dataframe from text file
df2 = spark.read.text("/src/resources/file.txt")

#create dataframe from JSON
df2 = spark.read.json("/src/resources/file.json")

#Create RDD from parallelize
data = [1,2,3,4,5,6,7,8,9,10,11,12]
rdd=spark.sparkContext.parallelize(data)

#Create RDD from external Data source
rdd2 = spark.sparkContext.textFile("/path/textFile.txt")

#Reads entire file into a RDD as single record.
rdd3 = spark.sparkContext.wholeTextFiles("/path/textFile.txt")

#get the num of partitions
print("initial partition count:"+str(rdd.getNumPartitions()))

#Repartition of RDD
reparRdd = rdd.repartition(4)
print("re-partition count:"+str(reparRdd.getNumPartitions()))

#rdd transformations
#flatmap:

rdd = spark.sparkContext.textFile("/tmp/test.txt")

rdd2 = rdd.flatMap(lambda x: x.split(" "))

#Map:
rdd3 = rdd2.map(lambda x: (x,1))

#reduce by key: Merges the value of each key
rdd5 = rdd4.reduceByKey(lambda a,b: a+b)

#sortbyKey
rdd6 = rdd5.map(lambda x: (x[1],x[0])).sortByKey()

#filter
rdd4 = rdd3.filter(lambda x : 'an' in x[1])

#rdd actions

# Action - count
print("Count : "+str(rdd6.count()))

# Action - first
firstRec = rdd6.first()
print("First Record : "+str(firstRec[0]) + ","+ firstRec[1])


# Action - max
datMax = rdd6.max()
print("Max Record : "+str(datMax[0]) + ","+ datMax[1])


# Action - reduce
totalWordCount = rdd6.reduce(lambda a,b: (a[0]+b[0],a[1]))
print("dataReduce Record : "+str(totalWordCount[0]))


# Action - take
data3 = rdd6.take(3)
for f in data3:
    print("data3 Key:"+ str(f[0]) +", Value:"+f[1])


# Action - collect
data = rdd6.collect()
for f in data:
    print("Key:"+ str(f[0]) +", Value:"+f[1])

#Rdd Cache
 cachedRdd = rdd.cache()

#Rdd Persist

dfPersist = rdd.persist(pyspark.StorageLevel.MEMORY_ONLY)


