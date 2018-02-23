# Gestion du Docker
sudo docker ps
mdp : infotel
sudo docker ps -a

# Lancement du Docker
sudo docker start spark-msos
sudo docker attach spark-msos

# Chargement de la dépendance csv (attention à bien placer les fichiers dans le répertoire de pyspark)
wget http://search.maven.org/remotecontent?filepath=org/apache/commons/commons-csv/1.1/commons-csv-1.1.jar -O commons-csv-1.1.jar
wget http://search.maven.org/remotecontent?filepath=com/databricks/spark-csv_2.10/1.0.0/spark-csv_2.10-1.0.0.jar -O spark-csv_2.10-1.0.0.jar

# Lancement de PySpark sans et avec la dépendance csv
sudo -E /spark/bin/pyspark --master mesos://zk://sep467ubuntu:2181/mesos --verbose --total-executor-cores 16 --executor-memory 20G
sudo -E /spark/bin/pyspark --master mesos://zk://sep467ubuntu:2181/mesos --verbose --total-executor-cores 16 --executor-memory 20G --jars "spark-csv_2.10-1.0.0.jar,commons-csv-1.1.jar"

# Exemple de chargement d'un fichier .csv avec la dépendance
fulltext = sqlContext.load(source="com.databricks.spark.csv", path = "hdfs://sep467ubuntu:9000/keven/fulltext3.csv")

################################################################## CONCATENATION DES FICHIERS FULLTEXT #############################################################################

# Importation des différentes parties
header = sc.parallelize(["SectionId,Type,Text"])
fulltext1 = sc.textFile("hdfs://sep467ubuntu:9000/keven/Export/fulltext-sections/New/Parsed/fulltext_part1_parsed.csv").filter(lambda line: line != "SectionId,Type,Text")
fulltext2 = sc.textFile("hdfs://sep467ubuntu:9000/keven/Export/fulltext-sections/New/Parsed/fulltext_part2_parsed.csv").filter(lambda line: line != "SectionId,Type,Text")
fulltext3 = sc.textFile("hdfs://sep467ubuntu:9000/keven/Export/fulltext-sections/New/Parsed/fulltext_part3_parsed.csv").filter(lambda line: line != "SectionId,Type,Text")
fulltext4 = sc.textFile("hdfs://sep467ubuntu:9000/keven/Export/fulltext-sections/New/Parsed/fulltext_part4_parsed.csv").filter(lambda line: line != "SectionId,Type,Text")
fulltext5 = sc.textFile("hdfs://sep467ubuntu:9000/keven/Export/fulltext-sections/New/Parsed/fulltext_part5_parsed.csv").filter(lambda line: line != "SectionId,Type,Text")
fulltext6 = sc.textFile("hdfs://sep467ubuntu:9000/keven/Export/fulltext-sections/New/Parsed/fulltext_part6_parsed.csv").filter(lambda line: line != "SectionId,Type,Text")
fulltext7 = sc.textFile("hdfs://sep467ubuntu:9000/keven/Export/fulltext-sections/New/Parsed/fulltext_part7_parsed.csv").filter(lambda line: line != "SectionId,Type,Text")
fulltext8 = sc.textFile("hdfs://sep467ubuntu:9000/keven/Export/fulltext-sections/New/Parsed/fulltext_part8_parsed.csv").filter(lambda line: line != "SectionId,Type,Text")
fulltext9 = sc.textFile("hdfs://sep467ubuntu:9000/keven/Export/fulltext-sections/New/Parsed/fulltext_part9_parsed.csv").filter(lambda line: line != "SectionId,Type,Text")

# Concaténation
fulltext_final = header + fulltext1 + fulltext2 + fulltext3 + fulltext4 + fulltext5 + fulltext6 + fulltext7 + fulltext8 + fulltext9

# Ecriture
fulltext_final.repartition(1).saveAsTextFile("hdfs://sep467ubuntu:9000/keven/Export/fulltext-sections/New/Parsed/total3")


################################################################## CREATION DATAFRAME ###############################################################################################

from pyspark.sql.types import *

# Metadata
metadata_Header = "DocId,Type,SectionID,VersionID"
metadata_Fields = [StructField(field_name, StringType(), True) for field_name in metadata_Header.split(",")]
metadata_Schema = StructType(metadata_Fields)
metadata_Rdd = sc.textFile("hdfs://sep467ubuntu:9000/keven/metadata.csv").filter(lambda line: line != "DocId,Type,SectionID,VersionID ").map(lambda line: line.split(","))
metadata_Df = sqlContext.createDataFrame(metadata_Rdd, metadata_Schema)


# Titre_CPC
titreCPC_Header = "DocId,Title,Main_CPC"
titreCPC_Fields = [StructField(field_name, StringType(), True) for field_name in titreCPC_Header.split(",")]
titreCPC_Schema = StructType(titreCPC_Fields)
titreCPC_Rdd = sc.textFile("hdfs://sep467ubuntu:9000/keven/titreCpc_parsed_OK.csv").filter(lambda line: line != "DocId,Title,Main_CPC").map(lambda line: line.split(","))
titreCPC_Df = sqlContext.createDataFrame(titreCPC_Rdd, titreCPC_Schema)

# Fulltext
fulltext_Header = "SectionId,Type,Text"
fulltext_Fields = [StructField(field_name, StringType(), True) for field_name in fulltext_Header.split(",")]
fulltext_Schema = StructType(fulltext_Fields)
fulltext_Rdd = sc.textFile("hdfs://sep467ubuntu:9000/keven/fulltext.csv").filter(lambda line: line != "SectionId,Type,Text").map(lambda line: line.split(","))
fulltext_Df = sqlContext.createDataFrame(fulltext_Rdd, fulltext_Schema)


################################################################## JOINTURE ######################################################################################################

# "Transformation" en table SQL
fulltext_Df.registerTempTable("fulltext")
titreCPC_Df.registerTempTable("titre_cpc")
metadata_Df.registerTempTable("metadata")

# Lancement de la jointure
final_data = sqlContext.sql("SELECT t.DocId,m.SectionID,m.Type,t.Main_CPC,t.Title,f.Text FROM titre_cpc as t inner join metadata as m on t.DocId=m.DocId inner join fulltext as f on m.SectionID=f.SectionId")

################################################################## ECRITURE ######################################################################################################

# Dataframe to RDD
data = final_data.map(lambda p: p[0] + "," + p[1] + "," + p[2] + "," + p[3] + "," + p[4] + "," + p[5])

# Ecriture
data.repartition(1).saveAsTextFile("hdfs://sep467ubuntu:9000/keven/FINAL_RESULT")


################################################################## PRE - PROCESSING ##############################################################################################

# Chargement de la dataframe
data_Header = "DocId,SectionID,Type,Main_CPC,Title,Text"
data_Fields = [StructField(field_name, StringType(), True) for field_name in data_Header.split(",")]
data_Schema = StructType(data_Fields)
data_Rdd = sc.textFile("hdfs://sep467ubuntu:9000/keven/FINAL/data_final.csv").filter(lambda line: line != "DocId,SectionID,Type,Main_CPC,Title,Text").map(lambda line: line.split(","))
data_Df = sqlContext.createDataFrame(data_Rdd, data_Schema)
data_Df.registerTempTable("data_Df")

# Etude préalable sur la répartition des classes
cpc = sqlContext.sql("SELECT COUNT(DocId) as repartition, Main_CPC as Classes_CPC FROM data_Df GROUP BY Main_CPC ORDER BY COUNT(DocId) DESC")
nb_cpc = sqlContext.sql("COUNT(DISTINCT Main_CPC) as nombre_classesCPC FROM data_SQL")
# Ecriture
cpc_final = cpc.rdd.map(lambda p: (p[0], p[1].encode('utf8')))
cpc_final = cpc.map(lambda p: str(p[0]) + "," + p[1])
cpc_final.repartition(1).saveAsTextFile("hdfs://sep467ubuntu:9000/keven/CPC_RESULT")


# Construction du data_set pour le machine learing
dataset = sqlContext.sql("SELECT DocId, Main_CPC, Title, Text FROM data_Df WHERE Main_CPC IN (B01D,B65D,C07C,E04B,F16B,F16D,F16K,G03B,H01H,H01R)")
dataset = dataset.withColumn('FullText', concat(dataset.Text, dataset.Title))
dataset.drop('Title')
dataset.drop('Text')


# Ecriture
dataset_final = dataset.map(lambda p : (p[0].encode('utf8'), p[1].encode('utf8'), p[2].encode('utf8'))
dataset_final.repartition(1).saveAsTextFile("hdfs://sep467ubuntu:9000/keven/FINAL/dataset.csv")



################################################################## MACHINE LEARNING ##############################################################################################

from pyspark.sql.types import *

# Chargement de la dataframe
df_Header = "DocId,CPC,Text"
df_Fields = [StructField(field_name, StringType(), True) for field_name in df_Header.split(",")]
df_Schema = StructType(df_Fields)
df_Rdd = sc.textFile("hdfs://sep467ubuntu:9000/keven/FINAL/dataset.csv").filter(lambda line: line != "DocId,CPC,Text").map(lambda line: line.split(","))
df_Df = sqlContext.createDataFrame(df_Rdd, df_Schema)

df_Df.registerTempTable("df_SQL")
cpc = sqlContext.sql("SELECT DISTINCT CPC FROM df_SQL GROUP BY CPC")



from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import HashingTF, IDF, Tokenizer

tokenizer = Tokenizer(inputCol="Text", outputCol="Text_list")
wordsData = tokenizer.transform(df_Df)
remover = StopWordsRemover(inputCol="Text_list", outputCol="Text_filtered")
df_Df = remover.transform(wordsData)
hashingTF = HashingTF(inputCol="Text_filtered", outputCol="rawFeatures", numFeatures=15)
featurizedData = hashingTF.transform(df_Df)
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)


def mycategory(score):
	if score == ' F16K': return 0.0
	elif score == ' B65D': return 1.0
	elif score == ' H01H': return 2.0
	elif score == ' H01R': return 3.0
	elif score == ' C07C': return 4.0
	elif score == ' B01D': return 5.0
	elif score == ' G03B': return 6.0
	elif score == ' E04B': return 7.0
	elif score == ' F16B': return 8.0
	elif score == ' F16D': return 9.0

myhash = udf(mycategory, StringType())
new_data = rescaledData.withColumn('CPC_New', myhash(rescaledData.CPC))
data_label = new_data.map(lambda l: LabeledPoint(l.CPC_New, l.features))


from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
(trainingData, testData) = data_label.randomSplit([0.7, 0.3])
model = RandomForest.trainClassifier(trainingData, numClasses=10, categoricalFeaturesInfo={},numTrees=150, featureSubsetStrategy="auto",impurity='gini', maxDepth=4, maxBins=32)

predictions = model.predict(testData.map(lambda x: x.features))
labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
print('Test Error = ' + str(testErr))
print('Learned classification forest model:')
print(model.toDebugString())

# Save and load model
model.save(sc, "target/tmp/myRandomForestClassificationModel")
sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")



########################################################################################################################
########################################################################################################################

# Dans Pyspark
# 2ième étape : Récupération des champs utiles via une jointure des deux collections et écrire le résultat en CSV

from pyspark.sql import SQLContext
import pandas as pd
sqlContext = SQLContext(sc)

#DataFrame Bibli_ft
bibli_ft = sqlContext.read.json("/home/kevenlemoing/Documents/Data_EPO/bibli_ft6.json")
bibli_ft.registerTempTable("bibli_ft")
bibli_ft.printSchema()

# Filtre SQL pour les résultats
dataset = sqlContext.sql("SELECT * FROM bibli_ft")

#Drop a non-pandas column
bibli_ft=bibli_ft.drop('column-name')


##Transformation du DataFrame --> utilisation de pandas
dataset = bibli_ft.toPandas()
#Concatener deux colonnes pour un dataframe pandas
dataset['full_text']=dataset['full_abstract']+dataset['full_claims']+dataset['full_description']+dataset['title']
# Drop a pandas column
dataset=dataset.drop('col_name',axis=1)
#Ecritude en CSV
dataset.to_csv('dataset1.csv', sep=',',encoding='utf-8', index=False) # Plus rapide : dataset.toPandas().to_csv('bibli_ftSPARK.csv', sep=',', encoding='utf-8')

# Par défault, au format PARQUET: dataset.save('nom_dossier')
# Au format JSON : df.toJSON()


T-NSE
http://scikit-learn.org/stable/auto_examples/manifold/plot_lle_digits.html


########################################################################################################################
########################################################################################################################

example2
dt = data
dt['full_claims'][0] = example2
dt.to_csv('\Users\k_lemo\Documents\Stage\Data_EPO\example2.csv', sep=',',encoding='utf_8', index=False)



>>> df = pd.concat([df1, df2])
>>> df = df.reset_index(drop=True)
group by

>>> df_gpby = df.groupby(list(df.columns))
get index of unique records

>>> idx = [x[0] for x in df_gpby.groups.values() if len(x) == 1]
filter

>>> df.reindex(idx)
         Date   Fruit   Num   Color
9  2013-11-25  Orange   8.6  Orange
8  2013-11-25   Apple  22.1     Red






http://scikit-learn.org/stable/tutorial/text_analytics/working_with_text_data.html
>>> from sklearn.feature_extraction.text import TfidfTransformer
>>> tf_transformer = TfidfTransformer(use_idf=False).fit(X_train_counts)
>>> X_train_tf = tf_transformer.transform(X_train_counts)
>>> X_train_tf.shape



########################################################################################################################
########################################################################################################################


# Au préalable : télécharger et installer Anaconda (https://www.continuum.io/downloads)
# Dans Pyspark
# 1ère étape : Connection au serveur hébergeant la base mongoDB pour faire un 'mongoexport' des collections bibli et bibli_ft au format JSON

#Connection à la base MongoDB
#Pour importer le package paramiko non-compris dans anaconda, faire la commande "sudo pip install paramiko"

import sys, paramiko

client = paramiko.SSHClient()
client.load_system_host_keys()
client.set_missing_host_key_policy(paramiko.WarningPolicy())
client.connect('http://172.17.42.1', username='mongo', password='mongo')
stdin, stdout, stderr = client.exec_command('COMMAND')
print stdout.read()

#SCRIPTS SHELL d'EXPORTATION VIA LE SERVEUR HEBERGEANT LA BASE MONGODB (ubuntucloud1)

#JSON
mongoexport --port 31017  -d baobab -c bibli_ft -f classification_cpc,classification_ipcr,doc_id,full_abstract,full_claims,full_description,title -q "{classification_cpc:{ \$exists: true, \$not: {\$size: 0} }})" -o ./bibli_ft.json  -limit 10000

#CSV
mongoexport --port 31017  -d baobab -c bibli_ft -f classification_cpc,classification_ipcr,doc_id,full_abstract,full_claims,full_description,title -q "{classification_cpc:{ \$exists: true, \$not: {\$size: 0} }})" --csv -o ./bibli_ft.csv  -limit 10000

mongoexport --port 31017  -d kimestats -c Pubstats --fieldFile ./Fields_pubStats.txt -q "{pubCC:"FR"}" --csv -o ./Publi_PubStats.csv  -limit 1000


